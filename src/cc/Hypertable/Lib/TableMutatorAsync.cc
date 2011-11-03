/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"

extern "C" {
#include <poll.h>
}

#include "Common/Config.h"
#include "Common/StringExt.h"

#include "Key.h"
#include "TableMutatorAsync.h"
#include "ResultCallback.h"
#include "TableMutatorSyncDispatchHandler.h"
#include "Table.h"
#include "TableMutator.h"
#include "LoadDataEscape.h"

using namespace Hypertable;
using namespace Hypertable::Config;

void TableMutatorAsync::handle_send_exceptions() {
  try {
    throw;
  }
  catch (std::bad_alloc &e) {
    m_last_error = Error::BAD_MEMORY_ALLOCATION;
    HT_ERROR("caught bad_alloc here");
  }
  catch (std::exception &e) {
    m_last_error = Error::EXTERNAL;
    HT_ERRORF("caught std::exception: %s", e.what());
  }
  catch (...) {
    m_last_error = Error::EXTERNAL;
    HT_ERROR("caught unknown exception here");
  }
}


TableMutatorAsync::TableMutatorAsync(PropertiesPtr &props, Comm *comm,
    ApplicationQueuePtr &app_queue, Table *table, RangeLocatorPtr &range_locator,
    uint32_t timeout_ms, ResultCallback *cb,  uint32_t flags,
    bool explicit_block_only)
  : m_comm(comm), m_app_queue(app_queue), m_table(table), m_range_locator(range_locator),
    m_memory_used(0), m_resends(0), m_timeout_ms(timeout_ms), m_cb(cb), m_flags(flags),
    m_mutex(m_buffer_mutex), m_cond(m_buffer_cond), m_explicit_block_only(explicit_block_only),
    m_next_buffer_id(0), m_cancelled(false), m_mutated(false) {
  initialize(props);
}


TableMutatorAsync::TableMutatorAsync(Mutex &mutex, boost::condition &cond, PropertiesPtr &props,
				     Comm *comm, ApplicationQueuePtr &app_queue, Table *table,
				     RangeLocatorPtr &range_locator, uint32_t timeout_ms,
				     ResultCallback *cb, uint32_t flags,
				     bool explicit_block_only)
  : m_comm(comm), m_app_queue(app_queue), m_table(table), m_range_locator(range_locator),
    m_memory_used(0), m_resends(0), m_timeout_ms(timeout_ms), m_cb(cb), m_flags(flags),
    m_mutex(mutex), m_cond(cond), m_explicit_block_only(explicit_block_only), m_next_buffer_id(0),
    m_cancelled(false), m_mutated(false) {
  initialize(props);
}

void TableMutatorAsync::initialize(PropertiesPtr &props) {
  HT_ASSERT(m_timeout_ms);
  m_table->get(m_table_identifier, m_schema);

  m_max_memory = props->get_i64("Hypertable.Mutator.ScatterBuffer.FlushLimit.Aggregate");

  uint32_t buffer_id = ++m_next_buffer_id;
  m_current_buffer = new TableMutatorAsyncScatterBuffer(m_comm, m_app_queue, this,
      &m_table_identifier, m_schema, m_range_locator, m_table->auto_refresh(), m_timeout_ms,
      buffer_id);

  // create new index mutator
  if (m_table->has_index_table())
    m_index_mutator=new TableMutator(props, m_comm, 
                                    &(*m_table->get_index_table()), 
                                    m_range_locator, m_timeout_ms, m_flags);
  // create new qualifier index mutator
  if (m_table->has_qualifier_index_table())
    m_qualifier_index_mutator=new TableMutator(props, m_comm, 
                                    &(*m_table->get_qualifier_index_table()), 
                                    m_range_locator, m_timeout_ms, m_flags);

  if (m_cb)
    m_cb->register_mutator(this);
}


TableMutatorAsync::~TableMutatorAsync() {
  try {
    // call sync on any unsynced rangeservers and flush current buffer if needed
    flush();
    if (!m_explicit_block_only)
      wait_for_completion();
    if (m_cb)
      m_cb->deregister_mutator(this);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e.what() << HT_END;
  }
}

void TableMutatorAsync::wait_for_completion() {
  ScopedLock lock(m_mutex);
  while(m_outstanding_buffers.size()>0)
    m_cond.wait(lock);
}

void
TableMutatorAsync::update_index(Key &key, const void *value, uint32_t value_len) {
  HT_ASSERT(key.flag == FLAG_INSERT);

  // only continue if the column family is indexed
  Schema::ColumnFamily *cf=m_schema->get_column_family(key.column_family_code);
  if (!cf || (!cf->has_index && !cf->has_qualifier_index))
    return;

  // indexed keys get an auto-assigned timestamp to make sure that the 
  // index key and the original key have identical timestamps
  if (key.timestamp == AUTO_ASSIGN)
    key.timestamp = get_ts64();

  // now create the key for the index
  KeySpec k;
  k.timestamp = key.timestamp;
  k.flag = key.flag;
  k.column_family = "v1";

  // every \t in the original row key gets escaped
  const char *row;
  size_t rowlen;
  LoadDataEscape lde, ldev;
  lde.escape(key.row, key.row_len, &row, &rowlen);

  // in a normal (non-qualifier) index the format of the new row
  // key is "value\trow"
  //
  // if value has a 0 byte then we also have to escape it
  if (cf->has_index) {
    const char *val_ptr = (const char *)value;
    for (const char *v = val_ptr; v < val_ptr + value_len; v++) {
      if (*v == '\0') {
        const char *outp;
        ldev.escape(val_ptr, (size_t)value_len, 
                    &outp, (size_t *)&value_len);
        value = outp;
        break;
      }
    }
    StaticBuffer sb(4 + value_len + rowlen + 1 + 1);
    char *p = (char *)sb.base;
    sprintf(p, "%d,", (int)cf->id);
    p     += strlen(p);
    memcpy(p, value, value_len);
    p     += value_len;
    *p++  = '\t';
    memcpy(p, row, rowlen);
    p     += rowlen;
    *p++  = '\0';
    k.row = sb.base;
    k.row_len = p - 1 - (const char *)sb.base; /* w/o the terminating zero */

    // and insert it
    m_index_mutator->set(k, 0, 0);
  }

  // in a qualifier index the format of the new row key is "qualifier\trow"
  if (cf->has_qualifier_index) {
    size_t qlen = key.column_qualifier ? strlen(key.column_qualifier) : 0;
    StaticBuffer sb(4 + qlen + rowlen + 1 + 1);
    char *p = (char *)sb.base;
    sprintf(p, "%d,", (int)cf->id);
    p     += strlen(p);
    if (qlen) {
      memcpy(p, key.column_qualifier, qlen);
      p   += qlen;
    }
    *p++  = '\t';
    memcpy(p, row, rowlen);
    p     += rowlen;
    *p++  = '\0';
    k.row = sb.base;
    k.row_len = p - 1 - (const char *)sb.base; /* w/o the terminating zero */

    // and insert it
    m_qualifier_index_mutator->set(k, 0, 0);
  }
}

void
TableMutatorAsync::set(const KeySpec &key, const void *value, uint32_t value_len) {
  bool unknown_cf;
  bool ignore_unknown_cfs = (m_flags & Table::MUTATOR_FLAG_IGNORE_UNKNOWN_CFS) ;
  size_t incr_mem;

  try {
    key.sanity_check();

    Key full_key;
    to_full_key(key, full_key, unknown_cf);
    if (ignore_unknown_cfs && unknown_cf)
      return;

    // first update secondary index, then set the key
    full_key.row_len = key.row_len;
    if (key.flag == FLAG_INSERT && (m_index_mutator || m_qualifier_index_mutator))
      update_index(full_key, value, value_len);

    incr_mem = 20 + key.row_len + key.column_qualifier_len + value_len;
    m_current_buffer->set(full_key, value, value_len, incr_mem);
    m_memory_used += incr_mem;
  }
  catch (...) {
    handle_send_exceptions();
    throw;
  }
}

void
TableMutatorAsync::set_cells(Cells::const_iterator it, Cells::const_iterator end) {
  bool unknown_cf;
  bool ignore_unknown_cfs = (m_flags & Table::MUTATOR_FLAG_IGNORE_UNKNOWN_CFS) ;
  size_t incr_mem;

  try {
    for (; it != end; ++it) {
      Key full_key;
      const Cell &cell = *it;
      cell.sanity_check();

      if (!cell.column_family) {
        if (cell.flag != FLAG_DELETE_ROW)
          HT_THROW(Error::BAD_KEY,
              (String)"Column family not specified in non-delete row set on row="
              + (String)cell.row_key);
        full_key.row = cell.row_key;
        full_key.timestamp = cell.timestamp;
        full_key.revision = cell.revision;
        full_key.flag = cell.flag;
      }
      else {
        to_full_key(cell, full_key, unknown_cf);
        if (ignore_unknown_cfs && unknown_cf)
          continue;
      }

      if (cell.row_key)
        full_key.row_len = strlen(cell.row_key);

      // first update secondary index, then set the key
      if (cell.flag == FLAG_INSERT && (m_index_mutator || m_qualifier_index_mutator))
        update_index(full_key, cell.value, cell.value_len);

      // assuming all inserts for now
      incr_mem = 20 + full_key.row_len
                + (cell.column_qualifier ? strlen(cell.column_qualifier) : 0);
      m_current_buffer->set(full_key, cell.value, cell.value_len, incr_mem);
      m_memory_used += incr_mem;
    }
  }
  catch (...) {
    handle_send_exceptions();
    throw;
  }
}

void TableMutatorAsync::set_delete(const KeySpec &key) {
  Key full_key;
  bool unknown_cf;
  bool ignore_unknown_cfs = (m_flags & Table::MUTATOR_FLAG_IGNORE_UNKNOWN_CFS) ;
  size_t incr_mem;
  try {
    key.sanity_check();

    if (!key.column_family) {
      full_key.row = (const char *)key.row;
      full_key.timestamp = key.timestamp;
      full_key.revision = key.revision;
      full_key.flag = key.flag;
    }
    else {
      to_full_key(key, full_key, unknown_cf);
      if (ignore_unknown_cfs && unknown_cf)
        return;
    }

    incr_mem = 20 + key.row_len + key.column_qualifier_len;
    m_current_buffer->set_delete(full_key, incr_mem);
    m_memory_used += incr_mem;
  }
  catch (...) {
    handle_send_exceptions();
    m_last_key = key;
    throw;
  }
}

void
TableMutatorAsync::to_full_key(const void *row, const char *column_family,
    const void *column_qualifier, int64_t timestamp, int64_t revision,
    uint8_t flag, Key &full_key, bool &unknown_cf) {
  bool ignore_unknown_cfs = (m_flags & Table::MUTATOR_FLAG_IGNORE_UNKNOWN_CFS);

  unknown_cf = false;

  if (flag > FLAG_DELETE_ROW) {
    if (!column_family)
      HT_THROW(Error::BAD_KEY, "Column family not specified");

    Schema::ColumnFamily *cf = m_schema->get_column_family(column_family);

    if (!cf) {
      if (m_table->auto_refresh()) {
        m_table->refresh(m_table_identifier, m_schema);
        m_current_buffer->refresh_schema(m_table_identifier, m_schema);
        cf = m_schema->get_column_family(column_family);
        if (!cf) {
          unknown_cf = true;
          if (ignore_unknown_cfs)
            return;
          HT_THROWF(Error::BAD_KEY, "Bad column family '%s'", column_family);
        }
      }
      else {
        unknown_cf = true;
        if (ignore_unknown_cfs)
          return;
        HT_THROWF(Error::BAD_KEY, "Bad column family '%s'", column_family);
      }
    }
    full_key.column_family_code = (uint8_t)cf->id;
  }
  else
    full_key.column_family_code = 0;

  full_key.row = (const char *)row;
  full_key.column_qualifier = (const char *)column_qualifier;
  full_key.timestamp = timestamp;
  full_key.revision = revision;
  full_key.flag = flag;
}

void TableMutatorAsync::cancel() {
  ScopedLock lock(m_cancel_mutex);
  m_cancelled=true;
}

bool TableMutatorAsync::is_cancelled() {
  ScopedLock lock(m_cancel_mutex);
  return m_cancelled;
}

bool TableMutatorAsync::needs_flush() {
  if (m_current_buffer->full() || m_memory_used > m_max_memory)
    return true;
  return false;
}

void TableMutatorAsync::flush(bool sync) {

  if (is_cancelled())
    return;

  uint32_t flags = sync ? 0:Table::MUTATOR_FLAG_NO_LOG_SYNC;

  try {
    if (m_current_buffer->memory_used() > 0) {
      m_current_buffer->send(flags);
      {
	ScopedLock lock(m_mutex);
	uint32_t buffer_id = ++m_next_buffer_id;
	if (m_outstanding_buffers.size() == 0 && m_cb)
	  m_cb->increment_outstanding();
	m_outstanding_buffers[m_current_buffer->get_id()] = m_current_buffer;
	m_current_buffer = new TableMutatorAsyncScatterBuffer(m_comm, m_app_queue, this,
                &m_table_identifier, m_schema, m_range_locator, m_table->auto_refresh(),
	         m_timeout_ms, buffer_id);
	m_memory_used = 0;
      }
    }
    // sync any unsynced RS
    if (sync)
      do_sync();
  }
  HT_RETHROW("flushing")
}

void TableMutatorAsync::get_unsynced_rangeservers(std::vector<CommAddress> &unsynced) {
  unsynced.clear();
  foreach (const CommAddress &comm_addr, m_unsynced_rangeservers)
    unsynced.push_back(comm_addr);
}

void TableMutatorAsync::do_sync() {
  // sync unsynced rangeservers
  try {
    if (!m_unsynced_rangeservers.empty()) {
      TableMutatorSyncDispatchHandler sync_handler(m_comm, m_table_identifier, m_timeout_ms);
      foreach (CommAddress addr, m_unsynced_rangeservers)
        sync_handler.add(addr);

      if (!sync_handler.wait_for_completion()) {
        std::vector<TableMutatorSyncDispatchHandler::ErrorResult> errors;
        uint32_t retry_count = 0;
        bool retry_failed;
        do {
          bool do_refresh = false;
          retry_count++;
          sync_handler.get_errors(errors);
          for (size_t i=0; i<errors.size(); i++) {
            if (m_table->auto_refresh() &&
                (errors[i].error == Error::RANGESERVER_GENERATION_MISMATCH ||
                 (!m_mutated && errors[i].error == Error::RANGESERVER_TABLE_NOT_FOUND)))
              do_refresh = true;
            else
              HT_ERRORF("commit log sync error - %s - %s", errors[i].msg.c_str(),
                  Error::get_text(errors[i].error));
          }
          if (do_refresh)
            m_table->refresh(m_table_identifier, m_schema);
          sync_handler.retry();
        }
        while ((retry_failed = (!sync_handler.wait_for_completion())) &&
            retry_count < ms_max_sync_retries);
        /**
         * Commit log sync failed
         */
        if (retry_failed) {
          sync_handler.get_errors(errors);
          String error_str;
          error_str =  (String) "commit log sync error '" + errors[0].msg.c_str() + "' '" +
            Error::get_text(errors[0].error) + "' max retry limit=" +
            ms_max_sync_retries + " hit";
          HT_THROW(errors[0].error, error_str);
        }
      }
    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    throw;
  }
  catch (...) {
    handle_send_exceptions();
    throw;
  }
}

/**
 *  NOTE:  mutex must be locked when making this call
 */
TableMutatorAsyncScatterBufferPtr TableMutatorAsync::get_outstanding_buffer(size_t id) {
  ScopedLock lock(m_mutex);
  TableMutatorAsyncScatterBufferPtr buffer;
  ScatterBufferAsyncMap::iterator it = m_outstanding_buffers.find(id);
  if (it != m_outstanding_buffers.end())
    buffer = it->second;
  return buffer;
}

void TableMutatorAsync::update_unsynced_rangeservers(const CommAddressSet &unsynced) {
  foreach (const CommAddress &comm_addr, unsynced)
    m_unsynced_rangeservers.insert(comm_addr);
}

void TableMutatorAsync::update_outstanding(TableMutatorAsyncScatterBufferPtr &buffer) {
  m_outstanding_buffers.erase(buffer->get_id());
  if (m_outstanding_buffers.size()==0) {
    m_cond.notify_one();
    if (m_cb)
      m_cb->decrement_outstanding();
  }
  m_cond.notify_one();
}

void TableMutatorAsync::buffer_finish(uint32_t id, int error, bool retry) {
  ScopedLock lock(m_mutex);
  bool cancelled = is_cancelled();
  TableMutatorAsyncScatterBufferPtr buffer;
  ScatterBufferAsyncMap::iterator it;

  it = m_outstanding_buffers.find(id);
  HT_ASSERT(it != m_outstanding_buffers.end());

  buffer = it->second;
  m_failed_mutations.clear();
  update_unsynced_rangeservers(buffer->get_unsynced_rangeservers());

  if (cancelled) {
    update_outstanding(buffer);
    return;
  }

  if (error != Error::OK) {
    if (error == Error::RANGESERVER_GENERATION_MISMATCH ||
        (!m_mutated && error == Error::RANGESERVER_TABLE_NOT_FOUND)) {
      // retry possible
      m_table->refresh(m_table_identifier, m_schema);
      buffer->refresh_schema(m_table_identifier, m_schema);
      retry = true;
    }
    else {
      if (retry)
        buffer->set_retries_to_fail(error);
      // send error to callback
      buffer->get_failed_mutations(m_failed_mutations);
      if (m_cb != 0)
        m_cb->update_error(this, error, m_failed_mutations);
      update_outstanding(buffer);
      return;
    }
  }

  if (retry) {
    // create & send redo buffer
    uint32_t next_id = ++m_next_buffer_id;
    TableMutatorAsyncScatterBufferPtr redo;
    try {
      redo = buffer->create_redo_buffer(next_id);
    }
    catch (Exception &e) {
      error = e.code();
      redo=0;
    }
    if (!redo) {
      buffer->get_failed_mutations(m_failed_mutations);
      // send error to callback
      if (m_cb != 0)
        m_cb->update_error(this, error, m_failed_mutations);
      update_outstanding(buffer);
    }
    else {
      HT_ASSERT(redo);
      m_resends += buffer->get_resend_count();
      m_outstanding_buffers.erase(it);
      redo->send(buffer->get_send_flags());
      m_outstanding_buffers[next_id] = redo;
    }
  }
  else {
    // everything went well
    m_mutated = true;
    if (m_cb != 0)
      m_cb->update_ok(this);
    update_outstanding(buffer);
  }
}
