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

import static org.apache.amoro.formats.lance.LanceProperties.CREATE_MODE_EXIST_OK;
import static org.apache.amoro.formats.lance.LanceProperties.DATA_STORAGE_VERSION;
import static org.apache.amoro.formats.lance.LanceProperties.DATA_STORAGE_VERSION_STABLE;
import static org.apache.amoro.formats.lance.LanceProperties.toCosScheme;

import org.apache.amoro.AlreadyExistsException;
import org.apache.amoro.AmoroTable;
import org.apache.amoro.DatabaseNotEmptyException;
import org.apache.amoro.FormatCatalog;
import org.apache.amoro.NoSuchDatabaseException;
import org.apache.amoro.NoSuchTableException;
import org.apache.amoro.properties.CatalogMetaProperties;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.TableMetaStore;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.lance.Dataset;
import org.lance.ReadOptions;
import org.lance.WriteParams;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.hive3.Hive3Namespace;
import org.lance.namespace.hive3.Hive3NamespaceConfig;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.DeclareTableRequest;
import org.lance.namespace.model.DeclareTableResponse;
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.namespace.model.DeregisterTableResponse;
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
import org.lance.operation.UpdateConfig;
import org.lance.operation.UpdateMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lance format catalog implementation backed by Hive Metastore.
 *
 * <p>This catalog uses {@link LanceNamespace} (Hive3Namespace) to manage table metadata in Hive
 * Metastore, and uses Lance native SDK to manage the actual data files.
 *
 * <p>Supports three-level namespace: catalog / database / table.
 */
public class LanceHiveCatalog implements FormatCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(LanceHiveCatalog.class);

  private static final String HIVE3_CATALOG_LOCATION_URI_KEY = "catalog.location.uri";

  /** Hadoop Configuration 中 COS region 的兜底配置 key。 */
  private static final String HADOOP_COS_REGION_KEY = "fs.cosn.bucket.region";

  /** TCLake 获取 COS 临时密钥的服务端地址 key，底层 TCLakeCOSCredentialsProvider 从 hadoopConf 读取。 */
  private static final String TCLAKE_CRED_SERVER_URL_KEY = "tclake.cos.credentials.server.url";

  /** TCLake appid key，底层 TCLakeCOSCredentialsProvider 从 hadoopConf 读取。 */
  private static final String TCLAKE_CRED_USER_APPID_KEY = "tclake.cos.credentials.user.appid";

  private final String catalogName;
  private final LanceNamespace hive3Namespace;
  private final Map<String, String> properties;
  private final String root;

  private final BufferAllocator allocator;

  /**
   * Lance COS 临时密钥提供者，仅在启用 TCLake 场景下非 null。 当 {@link #loadTable(String, String)} 在 catalog
   * 级别无法直接拿到 cos 密钥时，通过该 provider 基于表 location 的 bucket 向 TCLake 动态申请临时密钥。
   */
  private final LanceCOSCredentialsProvider cosCredentialsProvider;

  /**
   * Create a new LanceHiveCatalog instance.
   *
   * @param catalogName catalog name, falls back to "hive" if null or empty
   * @param hive3Namespace the Hive3 namespace client for HMS operations
   * @param properties catalog-level properties including storage and HMS configurations
   * @param allocator Arrow memory allocator for Lance dataset operations
   */
  public LanceHiveCatalog(
      String catalogName,
      LanceNamespace hive3Namespace,
      Map<String, String> properties,
      BufferAllocator allocator) {
    this(catalogName, hive3Namespace, properties, allocator, null);
  }

  /**
   * Create a new LanceHiveCatalog instance with an optional Hadoop Configuration.
   *
   * <p>当传入 {@code hadoopConf} 时，会尝试基于其中的 TCLake 配置构造 {@link LanceCOSCredentialsProvider}， 用于 {@link
   * #loadTable(String, String)} 场景下按表 bucket 动态申请 COS 临时密钥。
   *
   * @param catalogName catalog name, falls back to "hive" if null or empty
   * @param hive3Namespace the Hive3 namespace client for HMS operations
   * @param properties catalog-level properties including storage and HMS configurations
   * @param allocator Arrow memory allocator for Lance dataset operations
   * @param hadoopConf Hadoop Configuration，允许为 null（如传入，会用于申请 COS 临时密钥）
   */
  public LanceHiveCatalog(
      String catalogName,
      LanceNamespace hive3Namespace,
      Map<String, String> properties,
      BufferAllocator allocator,
      Configuration hadoopConf) {
    this.catalogName = catalogName;
    this.hive3Namespace = hive3Namespace;
    this.properties = properties;
    this.allocator = allocator;
    this.root = properties.get(Hive3NamespaceConfig.ROOT);
    this.cosCredentialsProvider = buildCosCredentialsProvider(properties, hadoopConf);
    if (!"hive".equals(name())) {
      // 启用hive3特性，使用三层namespace catalog-db-table
      CreateNamespaceRequest request = new CreateNamespaceRequest();
      request.setId(Arrays.asList(name()));
      Map<String, String> namespaceProperties = new HashMap<>(properties);
      if (!namespaceProperties.containsKey(HIVE3_CATALOG_LOCATION_URI_KEY)
          && root != null
          && !root.isEmpty()) {
        namespaceProperties.put(HIVE3_CATALOG_LOCATION_URI_KEY, root + "/" + name());
        LOG.info(
            "Set hive3 catalog location uri to: "
                + namespaceProperties.get(HIVE3_CATALOG_LOCATION_URI_KEY));
      }
      request.setProperties(namespaceProperties);
      request.setMode(CREATE_MODE_EXIST_OK);
      // 初始化HMS中的catalog
      this.hive3Namespace.createNamespace(request);
      LOG.info("Created HMS catalog: " + name());
    }
    LOG.info(
        "Initialized LanceHiveCatalog: name={}, root={}, hadoopConf={}, cosCredentialsProvider={}",
        name(),
        root,
        hadoopConf == null ? "null" : "present",
        cosCredentialsProvider == null ? "null" : "present");
  }

  /**
   * 基于 catalog properties + Hadoop Configuration 尝试构造 {@link LanceCOSCredentialsProvider}。
   *
   * <p>构造成功的前提：
   *
   * <ul>
   *   <li>传入了非空的 {@code hadoopConf}；
   *   <li>{@code hadoopConf} 中至少包含 TCLake 申请临时密钥必需的两个参数：{@code tclake.cos.credentials.server.url} 与
   *       {@code tclake.cos.credentials.user.appid}。
   * </ul>
   *
   * <p>cos region 取值优先级：catalog properties 中的 {@code cos.region} &gt; Hadoop Configuration 中的
   * {@code fs.cosn.bucket.region}，取不到则为空字符串（Lance 底层在 region 为空时会再尝试使用环境变量
   * TENCENTCLOUD_REGION，但不建议依赖）。
   *
   * @return 成功构造的 provider，失败或条件不满足时返回 null（表示不走 TCLake 动态申请逻辑）
   */
  private static LanceCOSCredentialsProvider buildCosCredentialsProvider(
      Map<String, String> properties, Configuration hadoopConf) {
    if (hadoopConf == null) {
      LOG.warn("Hadoop Configuration is null, skip initializing LanceCOSCredentialsProvider");
      return null;
    }

    String tclakeServerUrl = hadoopConf.get(TCLAKE_CRED_SERVER_URL_KEY);
    String tclakeAppId = hadoopConf.get(TCLAKE_CRED_USER_APPID_KEY);
    if (StringUtils.isBlank(tclakeServerUrl) || StringUtils.isBlank(tclakeAppId)) {
      LOG.warn(
          "Skip initializing LanceCOSCredentialsProvider: missing TCLake config."
              + " {}={}, {}={}. If this catalog is not backed by TCLake, this is expected.",
          TCLAKE_CRED_SERVER_URL_KEY,
          StringUtils.isBlank(tclakeServerUrl) ? "<blank>" : "<set>",
          TCLAKE_CRED_USER_APPID_KEY,
          StringUtils.isBlank(tclakeAppId) ? "<blank>" : tclakeAppId);
      return null;
    }

    // cos.region 取值：catalog properties > hadoopConf.fs.cosn.bucket.region > ""
    String cosRegion = properties == null ? null : properties.get(LanceProperties.COS_REGION);
    String regionSource = "catalogProperties";
    if (StringUtils.isBlank(cosRegion)) {
      cosRegion = hadoopConf.get(HADOOP_COS_REGION_KEY, "");
      regionSource = HADOOP_COS_REGION_KEY;
    }
    if (StringUtils.isBlank(cosRegion)) {
      LOG.warn(
          "COS region is not configured (neither catalog property '{}' nor hadoopConf '{}');"
              + " Lance native layer will require TENCENTCLOUD_REGION env to be set, otherwise"
              + " dataset open will fail with 'COS endpoint or region is required'.",
          LanceProperties.COS_REGION,
          HADOOP_COS_REGION_KEY);
    }

    try {
      LanceCOSCredentialsProvider provider = new LanceCOSCredentialsProvider(hadoopConf, cosRegion);
      LOG.info(
          "Initialized LanceCOSCredentialsProvider: cosRegion='{}' (source={}), tclakeServerUrl={},"
              + " tclakeAppId={}",
          cosRegion,
          regionSource,
          tclakeServerUrl,
          tclakeAppId);
      return provider;
    } catch (Throwable t) {
      LOG.warn(
          "Failed to initialize LanceCOSCredentialsProvider (cosRegion='{}'): {}. Fall back to"
              + " catalog-level COS credentials only.",
          cosRegion,
          t.getMessage(),
          t);
      return null;
    }
  }

  /**
   * Return the catalog name.
   *
   * @return catalog name, defaults to "hive" if not specified
   */
  public String name() {
    if (catalogName == null || catalogName.isEmpty()) {
      return "hive";
    }
    return catalogName;
  }

  /**
   * Factory method to create a LanceHiveCatalog from catalog properties and metastore
   * configuration.
   *
   * @param catalogName catalog name
   * @param metastoreType metastore type, must be {@link CatalogMetaProperties#CATALOG_TYPE_HIVE}
   * @param properties catalog properties including warehouse location
   * @param metaStore authentication context with Hadoop configuration
   * @return a new LanceHiveCatalog instance
   * @throws IllegalArgumentException if metastoreType is not "hive"
   */
  // for gravitino
  public static LanceHiveCatalog createCatalog(
      String catalogName,
      String metastoreType,
      Map<String, String> properties,
      TableMetaStore metaStore) {
    if (!CatalogMetaProperties.CATALOG_TYPE_HIVE.equalsIgnoreCase(metastoreType)) {
      throw new IllegalArgumentException("Unsupported metastore type: " + metastoreType);
    }
    Map<String, String> lanceCatalogProperties = Maps.newHashMap(properties);
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    Hive3Namespace namespace = new Hive3Namespace();
    namespace.setHadoopConf(metaStore.getConfiguration());
    String warehouse = lanceCatalogProperties.get(CatalogMetaProperties.KEY_WAREHOUSE);
    if (warehouse != null && !warehouse.isEmpty()) {
      lanceCatalogProperties.putIfAbsent(
          Hive3NamespaceConfig.ROOT, LanceProperties.normalizeRootUrl(warehouse));
    }
    namespace.initialize(lanceCatalogProperties, allocator);
    return new LanceHiveCatalog(
        catalogName, namespace, lanceCatalogProperties, allocator, metaStore.getConfiguration());
  }

  @Override
  public List<String> listTables(String database) {
    ListTablesRequest request = new ListTablesRequest();
    request.setId(Arrays.asList(name(), database));
    try {
      ListTablesResponse response = hive3Namespace.listTables(request);
      return new ArrayList<>(response.getTables());
    } catch (NamespaceNotFoundException e) {
      throw new NoSuchDatabaseException("Database does not exist: " + database);
    }
  }

  @Override
  public List<String> listDatabases() {
    ListNamespacesRequest request = new ListNamespacesRequest();
    request.setId(Arrays.asList(name()));
    ListNamespacesResponse response = hive3Namespace.listNamespaces(request);
    return new ArrayList<>(response.getNamespaces());
  }

  @Override
  public boolean databaseExists(String database) {
    ListNamespacesRequest request = new ListNamespacesRequest();
    request.setId(Arrays.asList(name()));
    ListNamespacesResponse response = hive3Namespace.listNamespaces(request);
    return response.getNamespaces().contains(database);
  }

  @Override
  public boolean tableExists(String database, String table) {
    return listTables(database).contains(table);
  }

  @Override
  public void createDatabase(String database) throws AlreadyExistsException {
    if (databaseExists(database)) {
      throw new AlreadyExistsException("Database already exists: " + database);
    }
    CreateNamespaceRequest request = new CreateNamespaceRequest();
    request.setId(Arrays.asList(name(), database));
    hive3Namespace.createNamespace(request);
  }

  @Override
  public void createDatabase(String database, Map<String, String> metadata)
      throws AlreadyExistsException {
    if (databaseExists(database)) {
      throw new AlreadyExistsException("Database already exists: " + database);
    }
    CreateNamespaceRequest request = new CreateNamespaceRequest();
    request.setId(Arrays.asList(name(), database));
    request.setProperties(metadata);
    hive3Namespace.createNamespace(request);
  }

  @Override
  public Map<String, String> loadDatabaseMetadata(String database) {
    DescribeNamespaceRequest request = new DescribeNamespaceRequest();
    request.setId(Arrays.asList(name(), database));
    try {
      DescribeNamespaceResponse response = hive3Namespace.describeNamespace(request);
      return response.getProperties();
    } catch (NamespaceNotFoundException e) {
      throw new NoSuchDatabaseException("Database does not exist: " + database);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void dropDatabase(String database) throws NoSuchDatabaseException {
    if (!databaseExists(database)) {
      throw new NoSuchDatabaseException("Database does not exist: " + database);
    }
    if (!listTables(database).isEmpty()) {
      throw new DatabaseNotEmptyException("Cannot drop non-empty database: " + database);
    }

    DropNamespaceRequest request = new DropNamespaceRequest();
    request.setId(Arrays.asList(name(), database));
    hive3Namespace.dropNamespace(request);
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

  /**
   * Load a table with extra properties that may contain storage configurations.
   *
   * <p>Storage options merge priority (low to high):
   *
   * <ol>
   *   <li>COSN configurations from catalog properties
   *   <li>{@code lance.storage.} prefixed configurations from extraProperties (prefix stripped)
   * </ol>
   *
   * @param database database name
   * @param table table name
   * @param storageOptions contain {@code lance.storage.} prefixed storage configurations
   * @return loaded table instance
   * @throws IllegalArgumentException if table location is null or empty
   */
  public AmoroTable<?> loadTable(String database, String table, Map<String, String> storageOptions)
      throws NoSuchTableException {
    long start = System.currentTimeMillis();
    if (!tableExists(database, table)) {
      throw new NoSuchTableException("Table does not exist: " + database + "." + table);
    }
    List<String> tableId = Arrays.asList(name(), database, table);
    // Use namespace to describe table and get location
    DescribeTableRequest request = new DescribeTableRequest();
    request.setId(tableId);
    DescribeTableResponse response = hive3Namespace.describeTable(request);

    String location = response.getLocation();
    if (location == null || location.isEmpty()) {
      throw new IllegalArgumentException(
          "Failed to get table location for " + database + "." + table);
    }

    // 合并存储配置：catalog 级别 + extraProperties 中 lance.storage. 前缀的配置
    Map<String, String> storageOptionsCopy = new HashMap<>(storageOptions);
    Map<String, String> mergedStorageOptions =
        LanceProperties.mergeStorageOptions(properties, storageOptionsCopy);
    TableIdentifier tableIdentifier = TableIdentifier.of(name(), database, table);

    // 将 HMS 返回的 location 转换为 Lance SDK 使用的 cos:// 协议
    String lanceLocation = LanceProperties.toCosScheme(location);

    Dataset dataset;
    try {
      ReadOptions.Builder optionsBuilder = new ReadOptions.Builder();
      optionsBuilder.setStorageOptions(mergedStorageOptions);
      dataset = Dataset.open(lanceLocation, optionsBuilder.build());

    } catch (Exception e) {
      // 表仅在 HMS 中注册但尚未创建实际数据，返回空 LanceTable
      LOG.warn(
          "Failed to open dataset for {}.{} at {}, returning empty table: {}",
          database,
          table,
          lanceLocation,
          e.getMessage());
      Map<String, String> emptyProperties = new HashMap<>();
      emptyProperties.put(LanceProperties.TABLE_LOCATION, lanceLocation);
      emptyProperties.put(LanceProperties.TABLE_EMPTY, "true");
      LOG.info(
          "loadTable success (empty table): {}.{}, location={}, elapsed={}ms",
          database,
          table,
          lanceLocation,
          System.currentTimeMillis() - start);
      return new LanceTable(tableIdentifier, null, emptyProperties);
    }

    // 合并三个 metadata 源到统一的 tableProperties
    Map<String, String> mergedProperties = LanceProperties.mergeDatasetMetadata(dataset);

    // 检查并补全 properties 中的 location 字段
    checkAndInjectLocation(dataset, mergedProperties);

    LanceTable lanceTable = new LanceTable(tableIdentifier, dataset, mergedProperties);
    LOG.info(
        "loadTable success: {}.{}, location={}, elapsed={}ms",
        database,
        table,
        lanceLocation,
        System.currentTimeMillis() - start);
    return lanceTable;
  }

  @Override
  public AmoroTable<?> loadTable(String database, String table) throws NoSuchTableException {
    if (!tableExists(database, table)) {
      throw new NoSuchTableException("Table does not exist: " + database + "." + table);
    }

    Map<String, String> storageOptions = new HashMap<>();
    if (cosCredentialsProvider != null) {
      // 取表 location 用于解析 bucket
      List<String> tableId = Arrays.asList(name(), database, table);
      DescribeTableRequest request = new DescribeTableRequest();
      request.setId(tableId);
      DescribeTableResponse response = hive3Namespace.describeTable(request);
      String location = response.getLocation();
      if (location == null || location.isEmpty()) {
        throw new IllegalArgumentException(
            "Failed to get table location for " + database + "." + table);
      }
      String lanceLocation = LanceProperties.toCosScheme(location);

      try {
        // LanceCOSCredentialsProvider 返回的是无前缀 key，而 loadTable(db,table,storageOptions)
        // 期望带 lance.storage. 前缀的参数，所以这里统一加上前缀再传入。
        Map<String, String> dynamic = cosCredentialsProvider.getStorageOptions(lanceLocation);
        for (Map.Entry<String, String> e : dynamic.entrySet()) {
          storageOptions.put(LanceProperties.LANCE_STORAGE_PREFIX + e.getKey(), e.getValue());
        }
        LOG.info(
            "Applied TCLake dynamic COS credentials for loadTable({}.{}): location={}",
            database,
            table,
            lanceLocation);
      } catch (RuntimeException e) {
        LOG.error(
            "Failed to acquire TCLake dynamic COS credentials for {}.{} at {}: {}",
            database,
            table,
            lanceLocation,
            e.getMessage(),
            e);
        throw e;
      }
    }

    return loadTable(database, table, storageOptions);
  }

  /**
   * 获取表的存储位置（仅查询 HMS，不涉及 COS 临时密钥或 Lance 数据加载）。
   *
   * <p>返回的 location 会统一转换为 Lance SDK 使用的 {@code cos://} 协议。
   *
   * @param database database name
   * @param table table name
   * @return table storage location in {@code cos://} scheme
   * @throws NoSuchTableException if the table does not exist
   * @throws IllegalArgumentException if HMS returns a null/empty location
   */
  public String getTableLocation(String database, String table) throws NoSuchTableException {
    if (!tableExists(database, table)) {
      throw new NoSuchTableException("Table does not exist: " + database + "." + table);
    }
    DescribeTableRequest request = new DescribeTableRequest();
    request.setId(Arrays.asList(name(), database, table));
    DescribeTableResponse response = hive3Namespace.describeTable(request);

    String location = response.getLocation();
    if (location == null || location.isEmpty()) {
      throw new IllegalArgumentException(
          "Failed to get table location for " + database + "." + table);
    }
    return LanceProperties.toCosScheme(location);
  }

  /**
   * Drop a table from the catalog.
   *
   * <p>When {@code purge} is true, both the HMS registration and the underlying data files are
   * deleted. When {@code purge} is false, only the HMS registration is removed (deregister), and
   * the data files are preserved.
   *
   * @param database database name
   * @param table table name
   * @param purge if true, delete data files; if false, only deregister from HMS
   * @return true if the operation succeeds
   */
  @Override
  public boolean dropTable(String database, String table, boolean purge) {
    if (!tableExists(database, table)) {
      throw new NoSuchTableException("Table does not exist: " + database + "." + table);
    }
    List<String> tableId = Arrays.asList(name(), database, table);
    if (purge) {
      DropTableRequest request = new DropTableRequest();
      request.setId(tableId);
      hive3Namespace.dropTable(request);
      LOG.info("dropTable success (purge=true, data files deleted): {}.{}", database, table);
    } else {
      deregisterTable(database, table);
      LOG.info(
          "dropTable success (purge=false, deregister only, data files preserved): {}.{}",
          database,
          table);
    }
    return true;
  }

  /**
   * Deregister a table from Hive Metastore without deleting the underlying data files.
   *
   * @param database database name
   * @param table table name
   * @return deregister response from HMS
   */
  public DeregisterTableResponse deregisterTable(String database, String table) {
    if (!tableExists(database, table)) {
      throw new NoSuchTableException("Table does not exist: " + database + "." + table);
    }
    DeregisterTableRequest request = new DeregisterTableRequest();
    List<String> tableId = Arrays.asList(name(), database, table);
    request.setId(tableId);
    DeregisterTableResponse response = hive3Namespace.deregisterTable(request);
    response.setLocation(toCosScheme(response.getLocation()));
    return response;
  }

  /**
   * Create a new Lance table with the given schema.
   *
   * <p>Since Hive namespace does not support creating tables with schema directly, this method
   * first declares the table in HMS via {@link #declareTable}, then creates the actual Lance
   * dataset using the native SDK.
   *
   * <p>Storage options with {@code lance.storage.} prefix in tableProperties will be extracted,
   * merged with catalog-level storage options, and removed from tableProperties before persisting.
   *
   * @param tableIdentifier table identifier (catalog, database, table)
   * @param location table storage location
   * @param schema Arrow schema for the table
   * @param tableProperties table properties (will be modified: storage keys removed)
   * @return the created LanceTable instance
   * @throws IOException if the Lance dataset creation fails
   */
  public LanceTable createTable(
      TableIdentifier tableIdentifier,
      String location,
      Schema schema,
      Map<String, String> tableProperties)
      throws IOException, AlreadyExistsException {
    if (tableExists(tableIdentifier.getDatabase(), tableIdentifier.getTableName())) {
      throw new AlreadyExistsException("Table already exists: " + tableIdentifier);
    }
    String dataStorageVersion =
        tableProperties.getOrDefault(DATA_STORAGE_VERSION, DATA_STORAGE_VERSION_STABLE);

    if (location == null || location.isEmpty()) {
      location =
          getDefaultTableLocation(tableIdentifier.getDatabase(), tableIdentifier.getTableName());
    }
    // 传给 HMS 的 location 使用 cosn:// 协议（Hive 需要通过 Hadoop FileSystem 识别）
    String hmsLocation = LanceProperties.toCosnScheme(location);
    // 1. 先通过 declareTable 在 Hive Metastore 中注册表元数据
    DeclareTableResponse declareResponse =
        declareTable(tableIdentifier, hmsLocation, tableProperties);

    String resolvedLocation = declareResponse.getLocation();
    tableProperties.put(LanceProperties.TABLE_LOCATION, resolvedLocation);

    // 2. 从 tableProperties 中提取 lance.storage. 前缀的存储配置，并从 properties 中移除
    Map<String, String> storageOptions =
        LanceProperties.mergeStorageOptions(properties, tableProperties);

    Dataset dataset =
        Dataset.write()
            .allocator(allocator)
            .uri(resolvedLocation)
            .schema(schema)
            .storageOptions(storageOptions)
            .mode(WriteParams.WriteMode.CREATE)
            .dataStorageVersion(dataStorageVersion)
            .execute();

    // 3. 通过 UpdateConfig 更新表元数据（已移除 storage 参数）
    UpdateConfig config =
        UpdateConfig.builder()
            .tableMetadataUpdates(
                UpdateMap.builder().updates(tableProperties).replace(true).build())
            .build();
    dataset.newTransactionBuilder().operation(config).build().commit();

    LanceTable lanceTable = new LanceTable(tableIdentifier, dataset, tableProperties);
    LOG.info(
        "createTable success: {}, location={}, dataStorageVersion={}",
        tableIdentifier,
        resolvedLocation,
        dataStorageVersion);
    return lanceTable;
  }

  /**
   * 根据 root 路径拼接默认的表存储位置。
   *
   * <p>路径格式：
   *
   * <ul>
   *   <li>catalogName 为 "hive" 时：{root}/{dbName}.db/{tableName}.lance
   *   <li>其他 catalogName：{root}/{catalogName}/{dbName}.db/{tableName}.lance
   * </ul>
   *
   * @param dbName 数据库名
   * @param tableName 表名
   * @return 默认的表存储位置，如果 root 未配置则返回 null
   */
  public String getDefaultTableLocation(String dbName, String tableName) {
    if (root == null || root.isEmpty()) {
      LOG.warn(
          "未配置 ROOT 参数（{}），无法生成默认 location，将使用 Hive 默认的 location 分配", Hive3NamespaceConfig.ROOT);
      return null;
    }
    if ("hive".equalsIgnoreCase(name())) {
      return String.format(
          "%s/%s.db/%s.lance", root, dbName.toLowerCase(), tableName.toLowerCase());
    }
    return String.format(
        "%s/%s/%s.db/%s.lance",
        root, name().toLowerCase(), dbName.toLowerCase(), tableName.toLowerCase());
  }

  /**
   * declareTable 不会具体保存表参数，后续通过 namespace 写入表也不会自动注入 location 导致下游 loadTable 参数缺失。 检查传入的 properties
   * 中是否包含 location 字段，如果缺失则通过 dataset.uri() 直接注入到 properties 中（不产生 commit）。
   *
   * @param dataset 已打开的 Lance 数据集
   * @param properties 待补全的 properties，方法会原地修改该 Map
   */
  private void checkAndInjectLocation(Dataset dataset, Map<String, String> properties) {
    if (properties == null) {
      return;
    }
    String existing = properties.get(LanceProperties.TABLE_LOCATION);
    if (existing != null && !existing.isEmpty()) {
      return;
    }

    String uri = dataset.uri();
    if (uri == null || uri.isEmpty()) {
      LOG.warn("Dataset uri is null or empty, skip injecting location into properties");
      return;
    }

    String injected = LanceProperties.toCosScheme(uri);
    LOG.info("Injecting location into properties from dataset.uri(): {}", injected);
    properties.put(LanceProperties.TABLE_LOCATION, injected);
  }

  /**
   * Declare (register) a table in Hive Metastore without creating the actual data.
   *
   * <p>If location is provided, the table is registered as an external table. If location is null
   * or empty, HMS will assign a default location.
   *
   * @param tableIdentifier table identifier (catalog, database, table)
   * @param location table storage location, or null for HMS-assigned location
   * @param tableProperties table properties to register
   * @return declare response containing the resolved location
   */
  public DeclareTableResponse declareTable(
      TableIdentifier tableIdentifier, String location, Map<String, String> tableProperties) {
    if (tableExists(tableIdentifier.getDatabase(), tableIdentifier.getTableName())) {
      throw new AlreadyExistsException("Table already exists: " + tableIdentifier);
    }
    DeclareTableRequest request = new DeclareTableRequest();
    request.setId(
        Arrays.asList(name(), tableIdentifier.getDatabase(), tableIdentifier.getTableName()));
    if (location == null || location.isEmpty()) {
      location =
          getDefaultTableLocation(tableIdentifier.getDatabase(), tableIdentifier.getTableName());
    }
    // 传给 HMS 的 location 使用 cosn:// 协议（Hive 需要通过 Hadoop FileSystem 识别）
    request.setLocation(LanceProperties.toCosnScheme(location));
    // 底层hive不会对tableProperties 做任何操作
    request.setProperties(tableProperties);
    DeclareTableResponse response = this.hive3Namespace.declareTable(request);

    // 将 HMS 返回的 location 转换为 Lance SDK 使用的 cos:// 协议
    response.setLocation(LanceProperties.toCosScheme(response.getLocation()));
    return response;
  }
}
