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

package org.apache.amoro.formats.iceberg;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.FormatCatalog;
import org.apache.amoro.properties.CatalogMetaProperties;
import org.apache.amoro.table.MixedTable;
import org.apache.amoro.table.TableMetaStore;
import org.apache.amoro.utils.MixedFormatCatalogUtil;
import org.apache.amoro.utils.PropertyUtil;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class IcebergCatalog implements FormatCatalog {

  private SupportsNamespaces asNamespaceCatalog;
  private final Catalog icebergCatalog;
  private final TableMetaStore metaStore;
  private final Map<String, String> properties;
  private final boolean filterMixedChangeStoreTable;

  public IcebergCatalog(Catalog catalog, Map<String, String> properties, TableMetaStore metaStore) {
    this.icebergCatalog = MixedFormatCatalogUtil.buildCacheCatalog(catalog, properties);
    if (catalog instanceof SupportsNamespaces) {
      this.asNamespaceCatalog = (SupportsNamespaces) catalog;
    }
    this.metaStore = metaStore;
    this.properties = properties;
    this.filterMixedChangeStoreTable =
        PropertyUtil.propertyAsBoolean(
            properties, CatalogMetaProperties.FILTER_MIXED_CHANGE_STORE_TABLE, false);
  }

  @Override
  public List<String> listDatabases() {
    return metaStore.doAs(
        () ->
            asNamespaceCatalog().listNamespaces().stream()
                .map(ns -> ns.level(0))
                .collect(Collectors.toList()));
  }

  @Override
  public boolean databaseExists(String database) {
    return listDatabases().contains(database);
  }

  @Override
  public boolean tableExists(String database, String table) {
    TableIdentifier identifier = TableIdentifier.of(database, table);
    return metaStore.doAs(() -> icebergCatalog.tableExists(identifier));
  }

  @Override
  public void createDatabase(String database) {
    metaStore.doAs(
        () -> {
          asNamespaceCatalog().createNamespace(Namespace.of(database));
          return null;
        });
  }

  @Override
  public void createDatabase(String database, Map<String, String> metadata) {
    metaStore.doAs(
        () -> {
          asNamespaceCatalog().createNamespace(Namespace.of(database), metadata);
          return null;
        });
  }

  @Override
  public Map<String, String> loadDatabaseMetadata(String database) {
    return metaStore.doAs(() -> asNamespaceCatalog().loadNamespaceMetadata(Namespace.of(database)));
  }

  @Override
  public void dropDatabase(String database) {
    metaStore.doAs(
        () -> {
          asNamespaceCatalog().dropNamespace(Namespace.of(database));
          return null;
        });
  }

  @Override
  public List<String> listTables(String database) {
    return metaStore.doAs(
        () -> {
          List<TableIdentifier> identifiers = icebergCatalog.listTables(Namespace.of(database));
          if (filterMixedChangeStoreTable) {
            Set<String> names =
                identifiers.stream().map(TableIdentifier::name).collect(Collectors.toSet());
            List<String> tables = Lists.newArrayList();
            String separator =
                properties.getOrDefault(
                    CatalogMetaProperties.MIXED_FORMAT_TABLE_STORE_SEPARATOR,
                    CatalogMetaProperties.MIXED_FORMAT_TABLE_STORE_SEPARATOR_DEFAULT);
            String changeStoreSuffix = separator + MixedTable.CHANGE_STORE_IDENTIFIER + separator;
            names.forEach(
                name -> {
                  if (!name.endsWith(changeStoreSuffix)
                      || !names.contains(
                          name.substring(0, name.length() - changeStoreSuffix.length()))) {
                    tables.add(name);
                  }
                });
            return tables;
          } else {
            return identifiers.stream().map(TableIdentifier::name).collect(Collectors.toList());
          }
        });
  }

  @Override
  public AmoroTable<?> loadTable(String database, String table) {
    return metaStore.doAs(
        () -> {
          try {
            Table icebergTable = icebergCatalog.loadTable(TableIdentifier.of(database, table));
            return IcebergTable.newIcebergTable(
                org.apache.amoro.table.TableIdentifier.of(icebergCatalog.name(), database, table),
                icebergTable,
                metaStore,
                properties);
          } catch (NoSuchTableException e) {
            throw new org.apache.amoro.NoSuchTableException(e);
          }
        });
  }

  private SupportsNamespaces asNamespaceCatalog() {
    if (asNamespaceCatalog == null) {
      throw new UnsupportedOperationException(
          String.format(
              "Iceberg catalog: %s doesn't implement SupportsNamespaces",
              icebergCatalog.getClass().getName()));
    }
    return asNamespaceCatalog;
  }

  @Override
  public boolean dropTable(String database, String table, boolean purge) {
    return metaStore.doAs(
        () -> icebergCatalog.dropTable(TableIdentifier.of(database, table), purge));
  }
}
