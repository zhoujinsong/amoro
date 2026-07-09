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

package org.apache.amoro.server.catalog.gravitino;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.FormatCatalog;
import org.apache.amoro.NoSuchDatabaseException;
import org.apache.amoro.NoSuchTableException;
import org.apache.amoro.TableFormat;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.table.TableIdentifier;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fake data-plane {@link FormatCatalog} used by {@link TestGravitinoUnifiedCatalog}. Records {@code
 * loadTable}/{@code dropTable} invocations so tests can assert routing behavior without a real
 * Iceberg/Lance backend.
 */
class FakeFormatCatalog implements FormatCatalog {

  private final TableFormat format;
  private final String catalogName;
  final List<String> loadedTables = new java.util.ArrayList<>();
  final Set<String> droppedTables = ConcurrentHashMap.newKeySet();

  FakeFormatCatalog(TableFormat format, String catalogName) {
    this.format = format;
    this.catalogName = catalogName;
  }

  @Override
  public List<String> listTables(String database) {
    return Collections.emptyList();
  }

  @Override
  public List<String> listDatabases() {
    return Collections.emptyList();
  }

  @Override
  public boolean databaseExists(String database) {
    return true;
  }

  @Override
  public boolean tableExists(String database, String table) {
    return true;
  }

  @Override
  public void createDatabase(String database) {}

  @Override
  public void createDatabase(String database, Map<String, String> metadata) {}

  @Override
  public Map<String, String> loadDatabaseMetadata(String database) {
    return Maps.newHashMap();
  }

  @Override
  public void dropDatabase(String database) {}

  @Override
  public void setDatabaseProperties(String database, Map<String, String> properties)
      throws NoSuchDatabaseException {}

  @Override
  public void removeDatabaseProperties(String database, Set<String> properties)
      throws NoSuchDatabaseException {}

  @Override
  public AmoroTable<?> loadTable(String database, String table) {
    if ("__missing__".equals(table)) {
      throw new NoSuchTableException("Table does not exist: " + database + "." + table);
    }
    loadedTables.add(database + "." + table);
    return new FakeAmoroTable(TableIdentifier.of(catalogName, database, table), format);
  }

  @Override
  public boolean dropTable(String database, String table, boolean purge) {
    droppedTables.add(database + "." + table);
    return true;
  }
}
