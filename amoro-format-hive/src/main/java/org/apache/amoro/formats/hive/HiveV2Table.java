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

package org.apache.amoro.formats.hive;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableSnapshot;
import org.apache.amoro.table.TableIdentifier;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.Map;

public class HiveV2Table implements AmoroTable<Table> {

  public static HiveV2Table newHiveTable(
      TableIdentifier identifier, Table hiveTable, Map<String, String> tableProperties) {
    return new HiveV2Table(identifier, hiveTable, tableProperties);
  }

  private final TableIdentifier identifier;
  private final Table table;
  private final Map<String, String> tableProperties;

  protected HiveV2Table(
      TableIdentifier identifier, Table table, Map<String, String> tableProperties) {
    this.identifier = identifier;
    this.table = table;
    this.tableProperties = tableProperties;
  }

  @Override
  public TableIdentifier id() {
    return this.identifier;
  }

  @Override
  public TableFormat format() {
    return TableFormat.HIVE_V2;
  }

  @Override
  public Map<String, String> properties() {
    return tableProperties;
  }

  @Override
  public Table originalTable() {
    return table;
  }

  @Override
  public TableSnapshot currentSnapshot() {
    throw new UnsupportedOperationException("Not supported yet for HiveTable.");
  }
}
