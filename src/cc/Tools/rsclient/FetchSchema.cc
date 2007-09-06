/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Logger.h"

#include "Hyperspace/HyperspaceClient.h"

#include "FetchSchema.h"

namespace hypertable {

  int FetchSchema(std::string &tableName, HyperspaceClient *hyperspace, SchemaPtr &schemaPtr) {
    std::string tableFile = (std::string)"/hypertable/tables/" + tableName;
    DynamicBuffer valueBuf(0);
    int error;

    if ((error = hyperspace->AttrGet(tableFile.c_str(), "schema", valueBuf)) != Error::OK) {
      LOG_VA_ERROR("Problem getting 'schema' attribute for '%s'", tableName.c_str());
      return error;
    }

    Schema *schema = Schema::NewInstance((const char *)valueBuf.buf, valueBuf.fill(), true);
    if (!schema->IsValid()) {
      LOG_VA_ERROR("Schema Parse Error: %s", schema->GetErrorString());
      delete schema;
      return Error::RANGESERVER_SCHEMA_PARSE_ERROR;
    }
    schemaPtr.reset(schema);

    return Error::OK;
  }

}
