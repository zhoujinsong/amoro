/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.amoro.server.table;

public class ExpiredTable {
  private final long tableId;
  private final String catalog;
  private final String schema;
  private final String table;
  private final int status; // 0: waiting, 1: processing, 2: finished

  public ExpiredTable(long tableId, String catalog, String schema, String table, int status) {
    this.tableId = tableId;
    this.catalog = catalog;
    this.schema = schema;
    this.table = table;
    this.status = status;
  }

  public String getCatalog() {
    return catalog;
  }

  public String getSchema() {
    return schema;
  }

  public String getTable() {
    return table;
  }

  public long getTableId() {
    return tableId;
  }

  public int getStatus() {
    return status;
  }

  @Override
  public String toString() {
    return String.format(
        "ExpiredTable: tableId=%s#name=%s.%s.%s#status=%s",
        tableId, catalog, schema, table, status);
  }
}
