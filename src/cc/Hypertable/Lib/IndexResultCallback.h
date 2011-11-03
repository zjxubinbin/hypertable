/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
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

#ifndef HYPERTABLE_INDEXRESULTCALLBACK_H
#define HYPERTABLE_INDEXRESULTCALLBACK_H

#include <vector>
#include <deque>
#include <map>
#include <boost/thread/condition.hpp>

#include "ResultCallback.h"
#include "TableScannerAsync.h"
#include "ScanSpec.h"
#include "HyperAppHelper/Unique.h"
#include "Namespace.h"
#include "Client.h"
#include "Common/Filesystem.h"

namespace Hypertable {

  static const char *tmp_schema_outer=
        "<Schema>"
          "<AccessGroup name=\"default\">"
            "%s"
          "</AccessGroup>"
        "</Schema>";

  static const char *tmp_schema_inner=
            "<ColumnFamily>"
              "<Name>%s</Name>"
              "<Counter>false</Counter>"
              "<MaxVersions>1</MaxVersions> "
              "<deleted>false</deleted>"
            "</ColumnFamily>";

  /** ResultCallback for secondary indices; used by TableScannerAsync
   */
  class IndexResultCallback: public ResultCallback {

    enum {
      STATE_COLLECTING_INDICES = 0,
      STATE_VERIFYING_RESULTS
    };

  public:

    IndexResultCallback(TablePtr primary_table, const ScanSpec &primary_spec, 
            ResultCallback *original_cb, uint32_t timeout_ms, 
            bool qualifier_scan)
      : ResultCallback(), m_primary_table(primary_table), 
        m_primary_spec(primary_spec), m_original_cb(original_cb), 
        m_timeout_ms(timeout_ms), m_mutator(0), m_state(0), m_inserted_keys(0),
        m_row_limit(0), m_cell_limit(0), m_cell_count(0), m_row_offset(0), 
        m_cell_offset(0), m_row_count(0), m_cell_limit_per_family(0), 
        m_eos(false), m_limits_reached(false), m_readahead_count(0),
        m_qualifier_scan(qualifier_scan) {
      m_original_cb->increment_outstanding();

      if (m_primary_spec.row_limit != 0 ||
          m_primary_spec.cell_limit != 0 ||
          m_primary_spec.row_offset != 0 ||
          m_primary_spec.cell_offset != 0 ||
          m_primary_spec.cell_limit_per_family != 0) {
        // keep track of offset and limit
        m_track_limits = true;
        m_row_limit = m_primary_spec.row_limit;
        m_cell_limit = m_primary_spec.cell_limit;
        m_row_offset = m_primary_spec.row_offset;
        m_cell_offset = m_primary_spec.cell_offset;
        m_cell_limit_per_family = m_primary_spec.cell_limit_per_family;
      }
      else
        m_track_limits = false;

      Schema::ColumnFamilies &families =
                primary_table->schema()->get_column_families();
      foreach (Schema::ColumnFamily *cf, families) {
        if (!cf->has_index && !cf->has_qualifier_index)
          continue;
        m_column_map[cf->id] = cf->name;
      }
    }

    virtual ~IndexResultCallback() {
        ScopedLock lock(m_mutex);
        if (m_mutator)
          delete m_mutator;
        foreach (TableScannerAsync *s, m_scanners)
          delete s;
        foreach (ScanSpecBuilder *ssb, m_sspecs)
          delete ssb;
        m_scanners.clear();
        if (m_tmp_table) {
          Client *client=m_primary_table->get_namespace()->get_client();
          NamespacePtr nstmp=client->open_namespace("/tmp");
          nstmp->drop_table(Filesystem::basename(m_tmp_table->get_name()), 
                        true);
        }
    }

    /**
     * Callback method for successful scan
     *
     * @param scanner
     * @param cells returned cells
     */
    virtual void scan_ok(TableScannerAsync *scanner, ScanCellsPtr &scancells) {
      ScopedLock lock(m_mutex);
      bool is_eos = false;

      // ignore empty packets
      if (scancells->get_eos() == false && !scancells->size())
        return;

      HT_ASSERT(m_eos == false);

      // if cells are from the primary table: check LIMIT/OFFSET and send
      // them to the original callback
      if (scanner->get_table_name() == m_primary_table->get_name()) {
        is_eos = scancells->get_eos();
        scancells->set_eos(false);
        if (m_track_limits)
          track_predicates(scanner, scancells);
        else
          m_original_cb->scan_ok(scanner, scancells);
        // immediately read ahead after receiving the first result from the
        // first scanner, and whenever the current scanner sends "eos"; 
        // that way there's always one outstanding scan
        if (is_eos || m_readahead_count<=1)
          readahead();
      }
      // otherwise the cells are either from the index table - in that case
      // they're collected and stored in a temporary table...
      else if (m_state == STATE_COLLECTING_INDICES)
        collect_indices(scanner, scancells);
      // ... or from the temporary table and need to be verified against the
      // primary table.
      else if (m_state == STATE_VERIFYING_RESULTS)
        verify_results(scanner, scancells);

      if (m_eos)
        scanner->cancel();

      // the last outstanding scanner just finished; send an "eos" packet
      // to the original callback
      if (is_eos == true || m_eos == true) {
        m_eos = true;
        send_empty_eos_packet(scanner);
        m_original_cb->decrement_outstanding();
      }
    }

    /**
     * Callback method for scan errors
     *
     * @param scanner
     * @param error
     * @param error_msg
     * @param eos end of scan
     */
    virtual void scan_error(TableScannerAsync *scanner, int error, 
                            const String &error_msg, bool eos) {
      m_original_cb->scan_error(scanner, error, error_msg, eos);
      if (eos)
        m_original_cb->decrement_outstanding();
    }

    virtual void update_ok(TableMutatorAsync *mutator) {
    }

    virtual void update_error(TableMutatorAsync *mutator, int error, 
            FailedMutations &failedMutations) {
      m_original_cb->update_error(mutator, error, failedMutations);
    }

   private:
    void send_empty_eos_packet(TableScannerAsync *scanner) {
      ScanCellsPtr empty = new ScanCells;
      empty->set_eos();
      m_original_cb->scan_ok(scanner, empty);
    }

    void collect_indices(TableScannerAsync *scanner, ScanCellsPtr &scancells) {
      // if the search in the index table did not yield any results then
      // return immediately
      if (!m_tmp_table && !scancells->size() && scancells->get_eos()) {
        m_eos = true;
        return;
      }

      // otherwise split the index row into column id, cell value and
      // cell row key
      CkeyMap keys;
      Cells cells;
      scancells->get(cells);
      foreach (Cell &cell, cells) {
        char *r = (char *)cell.row_key;
        char *p = r + strlen(r);
        while (*p != '\t' && p > (char *)cell.row_key)
          p--;
        if (*p != '\t') {
          HT_WARNF("Invalid index entry '%s' in index table '^%s'",
                  r, m_primary_table->get_name().c_str());
          continue;
        }
        // cut off the "%d," part at the beginning to get the column id
        char *id = r;
        while (*r != ',')
          r++;
        *r++ = 0;
        uint32_t cfid = (uint32_t)atoi(id);
        if (!cfid || m_column_map.find(cfid) == m_column_map.end()) {
          HT_WARNF("Invalid index entry '%s' in index table '^%s'",
                  r, m_primary_table->get_name().c_str());
          continue;
        }

        // split strings; after the next line p will point to the row key
        // and r will point to the value. id points to the column id
        *p++ = 0;

        // if the original query specified row intervals then these have
        // to be filtered in the client
        if (m_primary_spec.row_intervals.size()) {
          if (!row_intervals_match(m_primary_spec.row_intervals, p))
            continue;
        }

        // same about cell intervals
        if (m_primary_spec.cell_intervals.size()) {
          if (!cell_intervals_match(m_primary_spec.cell_intervals, p, 
                                m_column_map[cfid].c_str()))
            continue;
        }

        // if a temporary table was already created then store it in the 
        // temporary table. otherwise buffer it in memory but make sure
        // that no duplicate rows are inserted
        KeySpec key;
        key.row = p;
        key.row_len = strlen(p);
        key.column_family = m_column_map[cfid].c_str();
        key.timestamp = cell.timestamp;
        if (keys.find(key) == keys.end()) {
          keys.insert(CkeyMap::value_type(key, true));
          if (m_mutator)
            m_mutator->set(key, 0);
          m_inserted_keys++;
        }
      }

      // reached EOS? then flush the mutator
      if (scancells->get_eos()) {
        if (m_mutator) {
          delete m_mutator;
          m_mutator = 0;
        }
        if (!m_inserted_keys) {
          m_eos = true;
          return;
        }
      }
      // not EOS? if we haven't yet created a temporary table then create it
      // and store all buffered keys in this table
      else {
        if (!m_tmp_table) {
          create_temp_table();
          for (CkeyMap::iterator it = keys.begin(); it != keys.end(); ++it) 
            m_mutator->set(it->first, 0);
        }
        return;
      }

      // we've reached EOS. If there's a temporary table then create a 
      // scanner for this table. Otherwise immediately send the temporary
      // results to the primary table for verification
      ScanSpecBuilder ssb;
      ssb.set_max_versions(m_primary_spec.max_versions);
      ssb.set_return_deletes(m_primary_spec.return_deletes);
      ssb.set_keys_only(m_primary_spec.keys_only);
      ssb.set_row_regexp(m_primary_spec.row_regexp);
      foreach (const String &s, m_primary_spec.columns)
        ssb.add_column(s.c_str());
      ssb.set_time_interval(m_primary_spec.time_interval.first, 
                            m_primary_spec.time_interval.second);

      TableScannerAsync *s;
      if (m_tmp_table) {
        s = m_tmp_table->create_scanner_async(this, ssb.get(), 
                m_timeout_ms, Table::SCANNER_FLAG_IGNORE_INDEX);
      }
      else {
        for (CkeyMap::iterator it=keys.begin(); it!=keys.end(); ++it) 
          ssb.add_row((const char *)it->first.row);
        foreach (const ColumnPredicate &cp, m_primary_spec.column_predicates)
          ssb.add_column_predicate(cp.column_family, cp.operation,
                  cp.value, cp.value_len);

        s = m_primary_table->create_scanner_async(this, ssb.get(), 
                m_timeout_ms, Table::SCANNER_FLAG_IGNORE_INDEX);
      }

      m_scanners.push_back(s);
      m_state = STATE_VERIFYING_RESULTS;
    }

    /*
     * the temporary table mimicks the primary table: all column families
     * with an index are also created for the temporary table
     */
    void create_temp_table() {
      HT_ASSERT(m_tmp_table == NULL);
      HT_ASSERT(m_mutator == NULL);

      String inner;
      foreach (Schema::ColumnFamily *cf, 
              m_primary_table->schema()->get_column_families()) {
        if (m_qualifier_scan && !cf->has_qualifier_index)
          continue;
        if (!m_qualifier_scan && !cf->has_index)
          continue;
        inner += format(tmp_schema_inner, cf->name.c_str());
      }

      Client *client = m_primary_table->get_namespace()->get_client();
      NamespacePtr nstmp = client->open_namespace("/tmp");
      String guid = HyperAppHelper::generate_guid();
      nstmp->create_table(guid, format(tmp_schema_outer, inner.c_str()));
      m_tmp_table = nstmp->open_table(guid);

      m_mutator = m_tmp_table->create_mutator_async(this);
    }

    void verify_results(TableScannerAsync *scanner, ScanCellsPtr &scancells) {
      // no results? then return immediately
      if (scancells->get_eos() && !scancells->size()) {
        m_eos = true;
        return;
      }
      // the LIMIT/CELL_LIMIT was already exceeded, therefore this scanner 
      // can be cancelled
      if (m_limits_reached) {
        m_eos = true;
        scanner->cancel();
        return;
      }

      ScanSpecBuilder *ssb = new ScanSpecBuilder;
      foreach (const String &s, m_primary_spec.columns)
        ssb->add_column(s.c_str());
      ssb->set_max_versions(m_primary_spec.max_versions);
      ssb->set_return_deletes(m_primary_spec.return_deletes);
      foreach (const ColumnPredicate &cp, m_primary_spec.column_predicates)
        ssb->add_column_predicate(cp.column_family, cp.operation,
                cp.value, cp.value_len);
      if (m_primary_spec.value_regexp)
        ssb->set_value_regexp(m_primary_spec.value_regexp);

      // foreach cell from the secondary index: verify that it exists in
      // the primary table, but make sure that each rowkey is only inserted
      // ONCE
      Cells cells;
      scancells->get(cells);
      const char *last = m_last_rowkey_verify.size() 
                       ? m_last_rowkey_verify.c_str() 
                       : "";
      foreach (Cell &cell, cells) {
        if (strcmp(last, (const char *)cell.row_key)) {
          ssb->add_row(cell.row_key);
          last = (const char *)cell.row_key;
        }
      }

      // store the "last" pointer before it goes out of scope
      m_last_rowkey_verify = last;

      // add the ScanSpec to the queue and immediately read-ahead from the 
      // primary table 
      m_sspecs.push_back(ssb);
      readahead();
    }

    void readahead() {
      if (m_sspecs.empty())
        return;

      if (!m_limits_reached) {
        ScanSpecBuilder *ssb = m_sspecs[0];
        m_sspecs.pop_front();
        TableScannerAsync *s = 
            m_primary_table->create_scanner_async(this, ssb->get(), 
                        m_timeout_ms, Table::SCANNER_FLAG_IGNORE_INDEX);
        m_scanners.push_back(s);
        m_readahead_count++;
        delete ssb;
      }
    }

    void track_predicates(TableScannerAsync *scanner, ScanCellsPtr &scancells) {
      if (m_limits_reached)
        return;

      // count cells and rows; skip CELL_OFFSET/OFFSET cells/rows and reduce
      // the results to CELL_LIMIT/LIMIT cells/rows
      ScanCellsPtr scp = new ScanCells();
      Cells cells;
      scancells->get(cells);
      const char *last = m_last_rowkey_tracking.size() 
                       ? m_last_rowkey_tracking.c_str() 
                       : "";
      bool skip_row = false;
      foreach (Cell &cell, cells) {
        bool new_row = false;
        if (strcmp(last, cell.row_key)) {
          new_row = true;
          skip_row = false;
          last = cell.row_key;
          if (m_cell_limit_per_family)
            m_cell_count = 0;
          // adjust row offset
          if (m_row_offset) {
            m_row_offset--;
            skip_row = true;
            continue;
          }
        }
        else if (skip_row)
          continue;

        // check cell offset
        if (m_cell_offset) {
          m_cell_offset--;
          continue;
        }
        // check row offset
        if (m_row_offset)
          continue;
        // check cell limit
        if (m_cell_limit && m_cell_count >= m_cell_limit) {
          m_limits_reached = true;
          break;
        }
        // check row limit
        if (m_row_limit && new_row && m_row_count >= m_row_limit) {
          m_limits_reached = true;
          break;
        }
        // check cell limit per family
        if (!m_cell_limit_per_family || m_cell_count < m_cell_limit_per_family){
          // cell pointers will go out of scope, therefore "own" is true
          scp->add(cell, true);
        }

        m_cell_count++;
        if (new_row)
          m_row_count++;
      }

      // store the contents of "last" before it goes out of scope
      m_last_rowkey_tracking = last;

      // send the results to the original callback
      if (scp->size())
        m_original_cb->scan_ok(scanner, scp);
    }

    bool row_intervals_match(const RowIntervals &rivec, const char *row) {
      foreach (const RowInterval &ri, rivec) {
        if (ri.start && ri.start[0]) {
          if (ri.start_inclusive) {
            if (strcmp(row, ri.start)<0)
              continue;
          }
          else {
            if (strcmp(row, ri.start)<=0)
              continue;
          }
        }
        if (ri.end && ri.end[0]) {
          if (ri.end_inclusive) {
            if (strcmp(row, ri.end)>0)
              continue;
          }
          else {
            if (strcmp(row, ri.end)>=0)
              continue;
          }
        }
        return true;
      } 
      return false;
    }

    bool cell_intervals_match(const CellIntervals &civec, const char *row,
            const char *column) {
      foreach (const CellInterval &ci, civec) {
        if (ci.start_row && ci.start_row[0]) {
          int s=strcmp(row, ci.start_row);
          if (s>0)
            return true;
          if (s<0)
            continue;
        }
        if (ci.start_column && ci.start_column[0]) {
          if (ci.start_inclusive) {
            if (strcmp(column, ci.start_column)<0)
              continue;
          }
          else {
            if (strcmp(column, ci.start_column)<=0)
              continue;
          }
        }
        if (ci.end_row && ci.end_row[0]) {
          int s=strcmp(row, ci.end_row);
          if (s<0)
            return true;
          if (s>0)
            continue;
        }
        if (ci.end_column && ci.end_column[0]) {
          if (ci.end_inclusive) {
            if (strcmp(column, ci.end_column)>0)
              continue;
          }
          else {
            if (strcmp(column, ci.end_column)>=0)
              continue;
          }
        }
        return true;
      } 
      return false;
    }

    typedef std::map<KeySpec, bool> CkeyMap;
    typedef std::map<String, String> CstrMap;

    // a pointer to the primary table
    TablePtr m_primary_table;

    // the original scan spec for the primary table
    ScanSpec m_primary_spec;

    // the original callback object specified by the user
    ResultCallback *m_original_cb;

    // the original timeout value specified by the user
    uint32_t m_timeout_ms;

    // a list of all scanners that are created in this object
    std::vector<TableScannerAsync *> m_scanners;

    // a deque of ScanSpecs, needed for readahead in the primary table
    std::deque<ScanSpecBuilder *> m_sspecs;

    // a mapping from column id to column name
    std::map<uint32_t, String> m_column_map;

    // the temporary table; can be NULL
    TablePtr m_tmp_table;

    // a mutator for the temporary table
    TableMutatorAsync *m_mutator;

    // the current state 
    int m_state;

    // number of inserted keys in the temp. table 
    int m_inserted_keys;

    // limit and offset values from the original ScanSpec
    int m_row_limit;
    int m_cell_limit;
    int m_cell_count;
    int m_row_offset;
    int m_cell_offset;
    int m_row_count;
    int m_cell_limit_per_family;

    // we reached eos - no need to continue scanning
    bool m_eos;

    // track limits and offsets
    bool m_track_limits;

    // limits were reached, all following keys are discarded
    bool m_limits_reached;

    // a mutex 
    Mutex m_mutex;

    // counting the read-ahead scans
    int m_readahead_count;

    // temporary storage to persist pointer data before it goes out of scope
    String m_last_rowkey_verify;

    // temporary storage to persist pointer data before it goes out of scope
    String m_last_rowkey_tracking;

    // true if this index is a qualifier index
    bool m_qualifier_scan;
  };

  typedef intrusive_ptr<IndexResultCallback> IndexResultCallbackPtr;

  inline bool operator<(const KeySpec &lhs, const KeySpec &rhs) {
    size_t len1 = strlen((const char *)lhs.row); 
    size_t len2 = strlen((const char *)rhs.row); 
    int cmp = memcmp(lhs.row, rhs.row, std::min(len1, len2));
    if (cmp > 0) 
      return false;
    if (cmp < 0)
      return true;
    if (len1 < len2)
      return true;
    return false;
  }
} // namespace Hypertable

#endif // HYPERTABLE_INDEXRESULTCALLBACK_H
