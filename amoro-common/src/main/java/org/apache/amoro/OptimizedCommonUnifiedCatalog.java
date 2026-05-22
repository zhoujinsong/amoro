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

package org.apache.amoro;

import org.apache.amoro.properties.CatalogMetaProperties;
import org.apache.amoro.shade.guava32.com.google.common.collect.Sets;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.TableMetaStore;
import org.apache.amoro.utils.CatalogUtil;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OptimizedCommonUnifiedCatalog extends CommonUnifiedCatalog {

  public OptimizedCommonUnifiedCatalog(
      String catalogName,
      String metaStoreType,
      Map<String, String> properties,
      TableMetaStore tableMetaStore) {
    super(catalogName, metaStoreType, properties, tableMetaStore);
  }

  @Override
  public List<String> listDatabases() {
    if (!CatalogUtil.supportOptimizedMode(formatCatalogs.keySet())) {
      return super.listDatabases();
    }
    return getHiveV2Catalog().listDatabases();
  }

  @Override
  public boolean tableExists(String database, String table) {
    if (!CatalogUtil.supportOptimizedMode(formatCatalogs.keySet())) {
      return super.tableExists(database, table);
    }
    return getHiveV2Catalog().tableExists(database, table);
  }

  @Override
  public List<TableIdentifier> listTableIdentifiers(String database) {
    if (!CatalogUtil.supportOptimizedMode(formatCatalogs.keySet())) {
      return super.listTableIdentifiers(database);
    }
    Set<String> tableNames = Sets.newHashSet(getHiveV2Catalog().listTables(database));
    // Filter change store for mixed formats
    if ((formatCatalogs.containsKey(TableFormat.MIXED_ICEBERG)
        || formatCatalogs.containsKey(TableFormat.MIXED_HIVE))) {
      String separator =
          catalogProperties.getOrDefault(
              CatalogMetaProperties.MIXED_FORMAT_TABLE_STORE_SEPARATOR,
              CatalogMetaProperties.MIXED_FORMAT_TABLE_STORE_SEPARATOR_DEFAULT);
      String changeStoreSuffix = separator + "change" + separator;
      Set<String> returnTables = Sets.newHashSet();
      final Set<String> allTables = tableNames;
      allTables.forEach(
          name -> {
            if (!name.endsWith(changeStoreSuffix)
                || !allTables.contains(
                    name.substring(0, name.length() - changeStoreSuffix.length()))) {
              returnTables.add(name);
            }
          });
      tableNames = returnTables;
    }
    return tableNames.stream()
        .map(
            tableName -> {
              return TableIdentifier.of(catalogName, database, tableName);
            })
        .collect(Collectors.toList());
  }

  @Override
  public TableFormat loadTableFormat(String database, String table) {
    if (!CatalogUtil.supportOptimizedMode(formatCatalogs.keySet())) {
      return super.loadTableFormat(database, table);
    }
    FormatCatalog formatCatalog = formatCatalogs.get(TableFormat.HIVE_V2);
    AmoroTable<?> amoroTable = formatCatalog.loadTable(database, table);
    // TODO: support more table formats
    if (CatalogUtil.isMixedTableFormat(amoroTable.properties())) {
      return TableFormat.MIXED_ICEBERG;
    } else if (CatalogUtil.isIcebergTableFormat(amoroTable.properties())) {
      return TableFormat.ICEBERG;
    } else if (CatalogUtil.isHoodieTableFormat(amoroTable.properties())) {
      return TableFormat.HUDI;
    } else if (CatalogUtil.isLanceTableFormat(amoroTable.properties())) {
      return TableFormat.LANCE;
    } else {
      return TableFormat.HIVE_V2;
    }
  }

  private FormatCatalog getHiveV2Catalog() {
    return formatCatalogs.get(TableFormat.HIVE_V2);
  }
}
