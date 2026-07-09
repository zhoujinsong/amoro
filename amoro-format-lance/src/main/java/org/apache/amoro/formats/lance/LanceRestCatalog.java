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

package org.apache.amoro.formats.lance;

import org.apache.amoro.AlreadyExistsException;
import org.apache.amoro.AmoroTable;
import org.apache.amoro.DatabaseNotEmptyException;
import org.apache.amoro.FormatCatalog;
import org.apache.amoro.NoSuchDatabaseException;
import org.apache.amoro.NoSuchTableException;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.table.TableIdentifier;
import org.apache.commons.lang3.StringUtils;
import org.lance.Dataset;
import org.lance.ReadOptions;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.TableExistsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lance format catalog implementation backed by a REST-based namespace.
 *
 * <p>This catalog uses a REST {@link LanceNamespace} (created via {@code
 * LanceNamespace.connect("rest", ...)}) to manage table metadata, while the actual data files are
 * managed by the Lance native SDK.
 *
 * <p>Supports a three-level namespace: {@code catalog / database / table}. The top-level catalog
 * name corresponds to the catalog name on the remote REST service and is supplied through the
 * {@value #REST_CATALOG_NAME} property (set it via {@code lance.catalog} at the unified-catalog
 * level, which is stripped to {@code catalog} before reaching this class). Object ids are
 * therefore:
 *
 * <ul>
 *   <li>databases (namespaces): {@code id = [catalog]} for listing, {@code [catalog, database]} for
 *       a concrete database;
 *   <li>tables: {@code id = [catalog, database, table]}.
 * </ul>
 */
public class LanceRestCatalog implements FormatCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(LanceRestCatalog.class);

  /**
   * Property key (after the {@code lance.} prefix is stripped) carrying the catalog name on the
   * remote REST service. Configure it at the unified-catalog level as {@code lance.catalog}.
   */
  public static final String REST_CATALOG_NAME = "catalog";

  private final LanceNamespace restNamespace;
  private final Map<String, String> properties;
  private final String catalogName;

  /**
   * Create a new LanceRestCatalog instance.
   *
   * @param restNamespace the REST-based namespace client
   * @param properties catalog-level properties; must contain {@value #REST_CATALOG_NAME} (the
   *     remote REST catalog name) and may carry storage configurations
   */
  public LanceRestCatalog(LanceNamespace restNamespace, Map<String, String> properties) {
    this.restNamespace = restNamespace;
    this.properties = properties;
    this.catalogName = properties == null ? null : properties.get(REST_CATALOG_NAME);
    if (StringUtils.isBlank(catalogName)) {
      throw new IllegalArgumentException(
          "Lance REST catalog requires a non-empty '"
              + REST_CATALOG_NAME
              + "' property (configure it as 'lance."
              + REST_CATALOG_NAME
              + "' at the catalog level) to address the remote REST catalog.");
    }
    LOG.info("Initialized LanceRestCatalog, remote catalog name={}", catalogName);
  }

  /** Namespace id for listing databases under the catalog: {@code [catalog]}. */
  private List<String> catalogId() {
    return Collections.singletonList(catalogName);
  }

  /** Namespace id for a concrete database: {@code [catalog, database]}. */
  private List<String> databaseId(String database) {
    return Arrays.asList(catalogName, database);
  }

  /** Table id: {@code [catalog, database, table]}. */
  private List<String> tableId(String database, String table) {
    return Arrays.asList(catalogName, database, table);
  }

  @Override
  public List<String> listDatabases() {
    ListNamespacesRequest request = new ListNamespacesRequest();
    request.setId(catalogId());
    ListNamespacesResponse response = restNamespace.listNamespaces(request);
    return new ArrayList<>(response.getNamespaces());
  }

  @Override
  public boolean databaseExists(String database) {
    return listDatabases().contains(database);
  }

  @Override
  public List<String> listTables(String database) {
    ListTablesRequest request = new ListTablesRequest();
    request.setId(databaseId(database));
    try {
      ListTablesResponse response = restNamespace.listTables(request);
      return new ArrayList<>(response.getTables());
    } catch (NamespaceNotFoundException e) {
      throw new NoSuchDatabaseException("Database does not exist: " + database);
    }
  }

  @Override
  public boolean tableExists(String database, String table) {
    try {
      TableExistsRequest request = new TableExistsRequest();
      request.setId(tableId(database, table));
      restNamespace.tableExists(request);
      return true;
    } catch (TableNotFoundException e) {
      return false;
    }
  }

  @Override
  public void createDatabase(String database) {
    if (databaseExists(database)) {
      throw new AlreadyExistsException("Database already exists: " + database);
    }
    CreateNamespaceRequest request = new CreateNamespaceRequest();
    request.setId(databaseId(database));
    restNamespace.createNamespace(request);
  }

  @Override
  public void createDatabase(String database, Map<String, String> metadata) {
    if (databaseExists(database)) {
      throw new AlreadyExistsException("Database already exists: " + database);
    }
    CreateNamespaceRequest request = new CreateNamespaceRequest();
    request.setId(databaseId(database));
    request.setProperties(metadata);
    restNamespace.createNamespace(request);
  }

  @Override
  public Map<String, String> loadDatabaseMetadata(String database) {
    DescribeNamespaceRequest request = new DescribeNamespaceRequest();
    request.setId(databaseId(database));
    try {
      DescribeNamespaceResponse response = restNamespace.describeNamespace(request);
      return response.getProperties();
    } catch (NamespaceNotFoundException e) {
      throw new NoSuchDatabaseException("Database does not exist: " + database);
    }
  }

  @Override
  public void dropDatabase(String database) {
    if (!databaseExists(database)) {
      throw new NoSuchDatabaseException("Database does not exist: " + database);
    }
    if (!listTables(database).isEmpty()) {
      throw new DatabaseNotEmptyException("Cannot drop non-empty database: " + database);
    }
    DropNamespaceRequest request = new DropNamespaceRequest();
    request.setId(databaseId(database));
    restNamespace.dropNamespace(request);
  }

  @Override
  public void setDatabaseProperties(String database, Map<String, String> properties)
      throws NoSuchDatabaseException {
    LOG.warn("setDatabaseProperties not yet implemented: {}", properties);
  }

  @Override
  public void removeDatabaseProperties(String database, Set<String> properties)
      throws NoSuchDatabaseException {
    LOG.warn("removeDatabaseProperties not yet implemented: {}", properties);
  }

  @Override
  public AmoroTable<?> loadTable(String database, String table) {
    DescribeTableRequest request = new DescribeTableRequest();
    request.setId(tableId(database, table));
    DescribeTableResponse response;
    try {
      response = restNamespace.describeTable(request);
    } catch (TableNotFoundException e) {
      throw new NoSuchTableException("Table does not exist: " + database + "." + table);
    }

    String location = response.getLocation();
    if (location == null || location.isEmpty()) {
      throw new IllegalArgumentException(
          "Failed to get table location for " + database + "." + table);
    }

    // 将 namespace 返回的 location 统一转换为 Lance SDK 使用的 cos:// 协议
    String lanceLocation = LanceProperties.toCosScheme(location);

    // 提取 catalog 级存储配置并打开 Lance 数据集
    Map<String, String> storageOptions = LanceProperties.extractCatalogStorageOptions(properties);
    ReadOptions.Builder optionsBuilder = new ReadOptions.Builder();
    optionsBuilder.setStorageOptions(storageOptions);
    Dataset dataset;
    try {
      dataset = Dataset.open(lanceLocation, optionsBuilder.build());
    } catch (Exception e) {
      String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
      if (msg.contains("was not found")) {
        LOG.info(
            "Table {}.{} is registered but dataset not yet created at {}, returning empty table",
            database,
            table,
            lanceLocation);
        Map<String, String> emptyProperties = Maps.newHashMap();
        emptyProperties.put(LanceProperties.TABLE_LOCATION, lanceLocation);
        emptyProperties.put(LanceProperties.TABLE_EMPTY, "true");
        TableIdentifier tableIdentifier = TableIdentifier.of(catalogName, database, table);
        return new LanceTable(tableIdentifier, null, emptyProperties);
      }
      throw e;
    }

    // 合并 dataset 的三个 metadata 源到统一的 tableProperties
    Map<String, String> mergedProperties = LanceProperties.mergeDatasetMetadata(dataset);

    TableIdentifier tableIdentifier = TableIdentifier.of(catalogName, database, table);
    return new LanceTable(tableIdentifier, dataset, mergedProperties);
  }

  @Override
  public boolean dropTable(String database, String table, boolean purge) {
    DropTableRequest request = new DropTableRequest();
    request.setId(tableId(database, table));
    try {
      restNamespace.dropTable(request);
    } catch (TableNotFoundException e) {
      throw new NoSuchTableException("Table does not exist: " + database + "." + table);
    }
    return true;
  }
}
