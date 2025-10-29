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
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.TableMetaStore;
import org.apache.amoro.utils.CatalogUtil;
import org.apache.iceberg.BaseMetastoreTableOperations;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CommonFastUnifiedCatalog extends CommonUnifiedCatalog {

  public static final String SPARK_SOURCE_PROVIDER = "spark.sql.sources.provider";
  public static String FLINK_CONNECTOR = "connector";

  public CommonFastUnifiedCatalog(
      String catalogName,
      String metaStoreType,
      Map<String, String> properties,
      TableMetaStore tableMetaStore) {
    super(catalogName, metaStoreType, properties, tableMetaStore);
  }

  @Override
  public List<String> listDatabases() {
    if (!CatalogUtil.supportHiveFastMode(formatCatalogs.keySet())) {
      return super.listDatabases();
    }
    return getHiveV2Catalog().listDatabases();
  }

  @Override
  public boolean tableExists(String database, String table) {
    if (!CatalogUtil.supportHiveFastMode(formatCatalogs.keySet())) {
      return super.tableExists(database, table);
    }
    return getHiveV2Catalog().tableExists(database, table);
  }

  @Override
  public List<TableIdentifier> listTableIdentifiers(String database) {
    if (!CatalogUtil.supportHiveFastMode(formatCatalogs.keySet())) {
      return super.listTableIdentifiers(database);
    }
    return getHiveV2Catalog().listTables(database).stream()
        .map(
            tableName -> {
              return TableIdentifier.of(catalogName, database, tableName);
            })
        .collect(Collectors.toList());
  }

  @Override
  public TableFormat loadTableFormat(String database, String table) {
    if (!CatalogUtil.supportHiveFastMode(formatCatalogs.keySet())) {
      return super.loadTableFormat(database, table);
    }
    FormatCatalog formatCatalog = formatCatalogs.get(TableFormat.HIVE_V2);
    AmoroTable<?> amoroTable = formatCatalog.loadTable(database, table);
    // TODO: support more table formats
    if (isMixedTableFormat(amoroTable.properties())) {
      return TableFormat.MIXED_ICEBERG;
    } else if (isHoodieTableFormat(amoroTable.properties())) {
      return TableFormat.HUDI;
    } else if (isIcebergTableFormat(amoroTable.properties())) {
      return TableFormat.ICEBERG;
    } else {
      return TableFormat.HIVE_V2;
    }
  }

  private FormatCatalog getHiveV2Catalog() {
    return formatCatalogs.get(TableFormat.HIVE_V2);
  }

  /**
   * Check if the given table properties is a iceberg-format table
   *
   * @param properties properties of the table.
   * @return true if this is a mixed-format table.
   */
  private boolean isIcebergTableFormat(Map<String, String> properties) {
    String tableType = properties.getOrDefault(BaseMetastoreTableOperations.TABLE_TYPE_PROP, "");
    return BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(tableType);
  }

  /**
   * Check if the given table properties is a mixed-format table
   *
   * @param properties properties of the table.
   * @return true if this is a mixed-format table.
   */
  private boolean isMixedTableFormat(Map<String, String> properties) {
    String tableFormat = properties.get(CatalogMetaProperties.MIXED_FORMAT_TABLE_KEY);
    String tableStore = properties.get(CatalogMetaProperties.MIXED_FORMAT_TABLE_STORE);
    return isIcebergTableFormat(properties)
        && TableFormat.MIXED_ICEBERG.name().equalsIgnoreCase(tableFormat)
        && CatalogMetaProperties.MIXED_FORMAT_TABLE_STORE_BASE.equalsIgnoreCase(tableStore);
  }

  /**
   * Check if the given table properties is a hudi-format table
   *
   * @param properties properties of the table.
   * @return true if this is a mixed-format table.
   */
  private boolean isHoodieTableFormat(Map<String, String> properties) {
    return "hudi".equalsIgnoreCase(properties.getOrDefault(SPARK_SOURCE_PROVIDER, ""))
        || "hudi".equalsIgnoreCase(properties.getOrDefault(FLINK_CONNECTOR, ""));
  }
}
