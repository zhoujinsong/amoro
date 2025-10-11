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
import org.apache.amoro.FormatCatalog;
import org.apache.amoro.NoSuchDatabaseException;
import org.apache.amoro.NoSuchTableException;
import org.apache.amoro.hive.CachedHiveClientPool;
import org.apache.amoro.hive.HMSClient;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.TableMetaStore;
import org.apache.amoro.utils.CatalogUtil;
import org.apache.amoro.utils.HiveUtil;
import org.apache.amoro.utils.IsolatedClassLoader;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class HiveV2Catalog implements FormatCatalog {

  private final TableMetaStore metaStore;
  private final String catalog;
  private final Map<String, String> properties;
  private final CachedHiveClientPool hiveClientPool;

  public HiveV2Catalog(
      String catalog, Map<String, String> catalogProperties, TableMetaStore metaStore) {
    this(catalog, catalogProperties, metaStore, null);
  }

  public HiveV2Catalog(
      String catalog,
      Map<String, String> catalogProperties,
      TableMetaStore metaStore,
      IsolatedClassLoader isolatedClassLoader) {
    this.catalog = catalog;
    this.metaStore = metaStore;
    this.properties =
        catalogProperties == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(catalogProperties);
    if (isolatedClassLoader == null) {
      this.hiveClientPool =
          new CachedHiveClientPool(metaStore, catalogProperties, Optional.empty());
    } else {
      this.hiveClientPool =
          new CachedHiveClientPool(metaStore, catalogProperties, Optional.of(isolatedClassLoader));
    }
  }

  @Override
  public List<String> listDatabases() {
    try {
      return hiveClientPool.run(HMSClient::getAllDatabases);
    } catch (TException | InterruptedException e) {
      throw new RuntimeException("Failed to fetch database from HMS ..", e);
    }
  }

  @Override
  public boolean databaseExists(String database) {
    try {
      hiveClientPool.run(client -> client.getDatabase(database));
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    } catch (TException | InterruptedException e) {
      throw new RuntimeException("Failed to fetch database from HMS", e);
    }
  }

  @Override
  public boolean tableExists(String database, String table) {
    return loadHiveV2Table(database, table).isPresent();
  }

  @Override
  public void createDatabase(String database) {
    createDatabase(database, Collections.emptyMap());
  }

  @Override
  public void createDatabase(String database, Map<String, String> metadata) {
    try {
      hiveClientPool.run(
          client -> {
            client.createDatabase(
                HiveUtil.constructHiveDatabase(database, metadata, metaStore.getConfiguration()));
            return null;
          });
    } catch (AlreadyExistsException e) {
      throw new org.apache.amoro.AlreadyExistsException("Database:" + database + " already exists");
    } catch (TException | InterruptedException e) {
      throw new RuntimeException("Failed to create database from HMS", e);
    }
  }

  @Override
  public Map<String, String> loadDatabaseMetadata(String database) {
    try {
      Database hiveDatabase = hiveClientPool.run(client -> client.getDatabase(database));
      return HiveUtil.collectDatabaseMetadata(hiveDatabase);
    } catch (UnknownDBException | NoSuchObjectException e) {
      throw new NoSuchDatabaseException("Database: " + database + " dose not exists.", e);
    } catch (TException | InterruptedException e) {
      throw new RuntimeException("Failed to drop database from HMS", e);
    }
  }

  @Override
  public void dropDatabase(String database) {
    try {
      hiveClientPool.run(
          client -> {
            client.dropDatabase(database, false, false, false);
            return null;
          });
    } catch (NoSuchObjectException e) {
      // pass
    } catch (TException | InterruptedException e) {
      throw new RuntimeException("Failed to drop database from HMS", e);
    }
  }

  @Override
  public boolean dropTable(String database, String table, boolean purge) {
    Optional<Table> optHiveTable = loadHiveV2Table(database, table);
    if (!optHiveTable.isPresent()) {
      return false;
    }
    try {
      hiveClientPool.run(
          client -> {
            client.dropTable(database, table, purge, true);
            return null;
          });
    } catch (TException | InterruptedException e) {
      throw new RuntimeException("Failed to drop database from HMS", e);
    }
    if (purge) {
      // TODO need support purge option
    }
    return true;
  }

  @Override
  public List<String> listTables(String database) {
    List<String> hiveTableLists;
    try {
      hiveTableLists = hiveClientPool.run(client -> client.getAllTables(database));
    } catch (NoSuchObjectException e) {
      throw new NoSuchDatabaseException("Database: " + database + " dose not exists.");
    } catch (TException | InterruptedException e) {
      throw new RuntimeException("Failed to drop database from HMS", e);
    }
    // TODO add filter ?
    return new ArrayList<>(hiveTableLists);
  }

  @Override
  public AmoroTable<?> loadTable(String database, String table) {
    Optional<org.apache.hadoop.hive.metastore.api.Table> hiveV2Table =
        loadHiveV2Table(database, table);
    if (!hiveV2Table.isPresent()) {
      throw new NoSuchTableException(
          "HiveV2 table: " + database + "." + table + " dose not exists");
    }
    org.apache.hadoop.hive.metastore.api.Table hiveTable = hiveV2Table.get();
    return metaStore.doAs(
        () -> {
          TableIdentifier identifier = TableIdentifier.of(catalog, database, table);
          Map<String, String> tableProperties = hiveTable.getParameters();
          Map<String, String> properties =
              CatalogUtil.mergeCatalogPropertiesToTable(tableProperties, this.properties);
          return HiveV2Table.newHiveTable(identifier, hiveTable, properties);
        });
  }

  private Optional<Table> loadHiveV2Table(String database, String table) {
    try {
      Table hiveTable = hiveClientPool.run(client -> client.getTable(database, table));
      return Optional.of(hiveTable);
    } catch (NoSuchObjectException e) {
      return Optional.empty();
    } catch (TException | InterruptedException e) {
      throw new RuntimeException("Failed to fetch database from HMS", e);
    }
  }
}
