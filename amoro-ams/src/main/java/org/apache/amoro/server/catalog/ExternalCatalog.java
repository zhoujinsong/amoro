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

package org.apache.amoro.server.catalog;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.CommonUnifiedCatalog;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableIDWithFormat;
import org.apache.amoro.UnifiedCatalog;
import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.properties.CatalogMetaProperties;
import org.apache.amoro.server.catalog.gravitino.GravitinoUnifiedCatalog;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.table.TableIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ExternalCatalog extends ServerCatalog {

  UnifiedCatalog unifiedCatalog;
  private Pattern tableFilterPattern;
  private Pattern databaseFilterPattern;

  protected ExternalCatalog(CatalogMeta metadata) {
    super(metadata);
    this.unifiedCatalog = metaStore.doAs(() -> buildUnifiedCatalog(metadata));
    catalogMetadataChanged();
  }

  private UnifiedCatalog buildUnifiedCatalog(CatalogMeta metadata) {
    if (CatalogMetaProperties.CATALOG_TYPE_GRAVITINO.equalsIgnoreCase(metadata.getCatalogType())) {
      return new GravitinoUnifiedCatalog(this::getMetadata, Maps.newHashMap());
    }
    return new CommonUnifiedCatalog(this::getMetadata, Maps.newHashMap());
  }

  @Override
  protected void catalogMetadataChanged() {
    this.unifiedCatalog.refresh();
    updateDatabaseFilter(getMetadata());
    updateTableFilter(getMetadata());
  }

  public void syncTable(String database, String tableName, TableFormat format) {
    ServerTableIdentifier tableIdentifier =
        ServerTableIdentifier.of(getMetadata().getCatalogName(), database, tableName, format);
    doAs(TableMetaMapper.class, mapper -> mapper.insertTable(tableIdentifier));
  }

  public ServerTableIdentifier getServerTableIdentifier(String database, String tableName) {
    return getAs(
        TableMetaMapper.class,
        mapper ->
            mapper.selectTableIdentifier(getMetadata().getCatalogName(), database, tableName));
  }

  public void disposeTable(String database, String tableName) {
    doAs(
        TableMetaMapper.class,
        mapper -> mapper.deleteTableIdByName(getMetadata().getCatalogName(), database, tableName));
  }

  @Override
  public boolean databaseExists(String database) {
    return doAs(() -> unifiedCatalog.databaseExists(database));
  }

  @Override
  public boolean tableExists(String database, String tableName) {
    return doAs(() -> unifiedCatalog.tableExists(database, tableName));
  }

  @Override
  public List<String> listDatabases() {
    return doAs(
        () ->
            unifiedCatalog.listDatabases().stream()
                .filter(
                    database ->
                        databaseFilterPattern == null
                            || databaseFilterPattern.matcher(database).matches())
                .collect(Collectors.toList()));
  }

  @Override
  public List<TableIDWithFormat> listTables() {
    return doAs(
        () ->
            unifiedCatalog.listDatabases().stream()
                .map(this::listTables)
                .flatMap(List::stream)
                .collect(Collectors.toList()));
  }

  @Override
  public List<TableIDWithFormat> listTables(String database) {
    return doAs(
        () ->
            new ArrayList<>(
                unifiedCatalog.listTables(database).stream()
                    .filter(
                        tableIDWithFormat ->
                            tableMatchesFilter(
                                database, tableIDWithFormat.getIdentifier().getTableName()))
                    .collect(Collectors.toList())));
  }

  @Override
  public List<TableIdentifier> listTableIdentifiers(String database) {
    return doAs(
        () ->
            new ArrayList<>(
                unifiedCatalog.listTableIdentifiers(database).stream()
                    .filter(identifier -> tableMatchesFilter(database, identifier.getTableName()))
                    .collect(Collectors.toList())));
  }

  private boolean tableMatchesFilter(String database, String tableName) {
    return tableFilterPattern == null
        || tableFilterPattern.matcher(database + "." + tableName).matches();
  }

  @Override
  public AmoroTable<?> loadTable(String database, String tableName) {
    return doAs(() -> unifiedCatalog.loadTable(database, tableName));
  }

  public TableFormat loadTableFormat(String database, String tableName) {
    return doAs(() -> unifiedCatalog.loadTableFormat(database, tableName));
  }

  private void updateDatabaseFilter(CatalogMeta metadata) {
    String databaseFilter =
        metadata.getCatalogProperties().get(CatalogMetaProperties.KEY_DATABASE_FILTER);
    if (databaseFilter != null) {
      databaseFilterPattern = Pattern.compile(databaseFilter);
    } else {
      databaseFilterPattern = null;
    }
  }

  private void updateTableFilter(CatalogMeta metadata) {
    String tableFilter =
        metadata.getCatalogProperties().get(CatalogMetaProperties.KEY_TABLE_FILTER);
    if (tableFilter != null) {
      tableFilterPattern = Pattern.compile(tableFilter);
    } else {
      tableFilterPattern = null;
    }
  }

  private <T> T doAs(Callable<T> callable) {
    return metaStore.doAs(callable);
  }
}
