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
import org.lance.namespace.errors.NamespaceAlreadyExistsException;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.TableNotFoundException;
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
import org.lance.namespace.model.NamespaceExistsRequest;
import org.lance.namespace.model.TableExistsRequest;
import org.lance.operation.UpdateConfig;
import org.lance.operation.UpdateMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
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

  /** Hadoop Configuration fallback key for COS region. */
  private static final String HADOOP_COS_REGION_KEY = "fs.cosn.bucket.region";

  /**
   * TCLake server URL key for COS temporary credentials; read from hadoopConf by
   * LanceCOSCredentialsProvider.
   */
  private static final String TCLAKE_CRED_SERVER_URL_KEY = "tclake.cos.credentials.server.url";

  /** TCLake appid key; read from hadoopConf by LanceCOSCredentialsProvider. */
  private static final String TCLAKE_CRED_USER_APPID_KEY = "tclake.cos.credentials.user.appid";

  private static final String SELF_OPTIMIZING_GROUP_KEY = "table.self-optimizing.group";

  private final String catalogName;
  private final LanceNamespace hive3Namespace;
  private final Map<String, String> properties;
  private final String root;

  private final BufferAllocator allocator;

  /**
   * Lance COS temporary credentials provider; non-null only when TCLake is enabled. When {@link
   * #loadTable(String, String)} cannot obtain cos credentials at the catalog level, this provider
   * dynamically requests temporary credentials from TCLake based on the table location's bucket.
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
   * <p>When {@code hadoopConf} is provided, a {@link LanceCOSCredentialsProvider} is built from the
   * TCLake configuration in it, used to dynamically request COS temporary credentials per-table
   * bucket in {@link #loadTable(String, String)}.
   *
   * @param catalogName catalog name, falls back to "hive" if null or empty
   * @param hive3Namespace the Hive3 namespace client for HMS operations
   * @param properties catalog-level properties including storage and HMS configurations
   * @param allocator Arrow memory allocator for Lance dataset operations
   * @param hadoopConf Hadoop Configuration, may be null (when provided, used to request COS
   *     temporary credentials)
   */
  public LanceHiveCatalog(
      String catalogName,
      LanceNamespace hive3Namespace,
      Map<String, String> properties,
      BufferAllocator allocator,
      Configuration hadoopConf) {
    this.catalogName = resolveHmsCatalogName(catalogName, properties);
    this.hive3Namespace = hive3Namespace;
    this.properties = properties;
    this.allocator = allocator;
    this.root = properties.get(Hive3NamespaceConfig.ROOT);
    this.cosCredentialsProvider = buildCosCredentialsProvider(properties, hadoopConf);
    if (!"hive".equals(name())) {
      // Enable hive3 feature: use three-level namespace catalog-db-table
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
      // Initialize the catalog in HMS
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
   * Build a {@link LanceCOSCredentialsProvider} from catalog properties + Hadoop Configuration.
   *
   * <p>Prerequisites for successful construction:
   *
   * <ul>
   *   <li>a non-null {@code hadoopConf} is provided;
   *   <li>{@code hadoopConf} contains at least the two parameters required by TCLake to request
   *       temporary credentials: {@code tclake.cos.credentials.server.url} and {@code
   *       tclake.cos.credentials.user.appid}.
   * </ul>
   *
   * <p>COS region resolution priority: {@code cos.region} in catalog properties &gt; {@code
   * fs.cosn.bucket.region} in Hadoop Configuration; defaults to empty string if neither is present
   * (the Lance underlying layer will fall back to the {@code TENCENTCLOUD_REGION} environment
   * variable when region is empty, but relying on this is not recommended).
   *
   * @return the constructed provider, or null if conditions are not met (meaning TCLake dynamic
   *     credential request is skipped)
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

    // cos.region resolution: catalog properties > hadoopConf.fs.cosn.bucket.region > ""
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

  static String resolveHmsCatalogName(String catalogName, Map<String, String> properties) {
    if (StringUtils.isBlank(catalogName) || properties == null) {
      return catalogName;
    }
    String group = properties.get(SELF_OPTIMIZING_GROUP_KEY);
    if (StringUtils.isBlank(group)) {
      return catalogName;
    }
    int idx = group.indexOf('-');
    String appid = idx > 0 ? group.substring(0, idx) : null;
    if (StringUtils.isBlank(appid)) {
      return catalogName;
    }
    String prefix = appid + "_";
    if (!catalogName.startsWith(prefix)) {
      return catalogName;
    }
    String stripped = catalogName.substring(prefix.length());
    LOG.info(
        "Stripped appid prefix '{}' from Amoro catalog '{}', HMS catalog name resolved to '{}'",
        prefix,
        catalogName,
        stripped);
    return stripped;
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
    Set<String> allTables = new LinkedHashSet<>();
    String pageToken = null;
    try {
      do {
        ListTablesRequest request = new ListTablesRequest();
        request.setId(Arrays.asList(name(), database));
        request.setPageToken(pageToken);
        request.setLimit(LanceProperties.DEFAULT_PAGE_SIZE);
        ListTablesResponse response = hive3Namespace.listTables(request);
        allTables.addAll(response.getTables());
        pageToken = response.getPageToken();
      } while (pageToken != null && !pageToken.isEmpty());
      return new ArrayList<>(allTables);
    } catch (NamespaceNotFoundException e) {
      throw new NoSuchDatabaseException("Database does not exist: " + database);
    }
  }

  @Override
  public List<String> listDatabases() {
    Set<String> allDatabases = new LinkedHashSet<>();
    String pageToken = null;
    do {
      ListNamespacesRequest request = new ListNamespacesRequest();
      request.setId(Arrays.asList(name()));
      request.setPageToken(pageToken);
      request.setLimit(LanceProperties.DEFAULT_PAGE_SIZE);
      ListNamespacesResponse response = hive3Namespace.listNamespaces(request);
      allDatabases.addAll(response.getNamespaces());
      pageToken = response.getPageToken();
    } while (pageToken != null && !pageToken.isEmpty());
    return new ArrayList<>(allDatabases);
  }

  @Override
  public boolean databaseExists(String database) {
    NamespaceExistsRequest request = new NamespaceExistsRequest();
    request.setId(Arrays.asList(name(), database));
    try {
      hive3Namespace.namespaceExists(request);
      return true;
    } catch (NamespaceNotFoundException e) {
      return false;
    }
  }

  @Override
  public boolean tableExists(String database, String table) {
    TableExistsRequest request = new TableExistsRequest();
    request.setId(Arrays.asList(name(), database, table));
    try {
      hive3Namespace.tableExists(request);
      return true;
    } catch (TableNotFoundException e) {
      return false;
    }
  }

  @Override
  public void createDatabase(String database) throws AlreadyExistsException {
    CreateNamespaceRequest request = new CreateNamespaceRequest();
    request.setId(Arrays.asList(name(), database));
    try {
      hive3Namespace.createNamespace(request);
    } catch (NamespaceAlreadyExistsException e) {
      throw new AlreadyExistsException("Database already exists: " + database);
    }
  }

  @Override
  public void createDatabase(String database, Map<String, String> metadata)
      throws AlreadyExistsException {
    CreateNamespaceRequest request = new CreateNamespaceRequest();
    request.setId(Arrays.asList(name(), database));
    request.setProperties(metadata);
    try {
      hive3Namespace.createNamespace(request);
    } catch (NamespaceAlreadyExistsException e) {
      throw new AlreadyExistsException("Database already exists: " + database);
    }
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
    if (!listTables(database).isEmpty()) {
      throw new DatabaseNotEmptyException("Cannot drop non-empty database: " + database);
    }
    DropNamespaceRequest request = new DropNamespaceRequest();
    request.setId(Arrays.asList(name(), database));
    try {
      hive3Namespace.dropNamespace(request);
    } catch (NamespaceNotFoundException e) {
      throw new NoSuchDatabaseException("Database does not exist: " + database);
    }
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
    if (!databaseExists(database)) {
      throw new NoSuchDatabaseException("Database does not exist: " + database);
    }

    List<String> tableId = Arrays.asList(name(), database, table);
    // Use namespace to describe table and get location
    DescribeTableRequest request = new DescribeTableRequest();
    request.setId(tableId);
    DescribeTableResponse response;
    try {
      response = hive3Namespace.describeTable(request);
    } catch (TableNotFoundException e) {
      throw new NoSuchTableException("Table does not exist: " + database + "." + table);
    }

    String location = response.getLocation();
    if (location == null || location.isEmpty()) {
      throw new IllegalArgumentException(
          "Failed to get table location for " + database + "." + table);
    }

    // Merge storage options: catalog-level + lance.storage. prefixed options from extraProperties
    Map<String, String> storageOptionsCopy = new HashMap<>(storageOptions);
    Map<String, String> mergedStorageOptions =
        LanceProperties.mergeStorageOptions(properties, storageOptionsCopy);
    TableIdentifier tableIdentifier = TableIdentifier.of(name(), database, table);

    // Convert HMS-returned location to the cos:// scheme used by the Lance SDK
    String lanceLocation = LanceProperties.toCosScheme(location);

    Dataset dataset;
    try {
      ReadOptions.Builder optionsBuilder = new ReadOptions.Builder();
      optionsBuilder.setStorageOptions(mergedStorageOptions);
      dataset = Dataset.open(lanceLocation, optionsBuilder.build());

    } catch (Exception e) {
      String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
      if (msg.contains("was not found")) {
        LOG.info(
            "Table {}.{} is registered but dataset not yet created at {}, returning empty table",
            database,
            table,
            lanceLocation);
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
      throw e;
    }

    // Merge the three metadata sources into unified tableProperties
    Map<String, String> mergedProperties = LanceProperties.mergeDatasetMetadata(dataset);

    // Check and inject the location field into properties if missing
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
    if (!databaseExists(database)) {
      throw new NoSuchDatabaseException("Database does not exist: " + database);
    }

    Map<String, String> storageOptions = new HashMap<>();
    if (cosCredentialsProvider != null) {
      // Get table location to resolve the bucket
      List<String> tableId = Arrays.asList(name(), database, table);
      DescribeTableRequest request = new DescribeTableRequest();
      request.setId(tableId);
      DescribeTableResponse response;
      try {
        response = hive3Namespace.describeTable(request);
      } catch (TableNotFoundException e) {
        throw new NoSuchTableException("Table does not exist: " + database + "." + table);
      }
      String location = response.getLocation();
      if (location == null || location.isEmpty()) {
        throw new IllegalArgumentException(
            "Failed to get table location for " + database + "." + table);
      }
      String lanceLocation = LanceProperties.toCosScheme(location);

      try {
        // LanceCOSCredentialsProvider returns unprefixed keys, but
        // loadTable(db,table,storageOptions)
        // expects lance.storage. prefixed parameters, so add the prefix here before passing them
        // in.
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
   * Get the table storage location (queries HMS only, no COS temporary credentials or Lance data
   * loading).
   *
   * <p>The returned location is converted to the {@code cos://} scheme used by the Lance SDK.
   *
   * @param database database name
   * @param table table name
   * @return table storage location in {@code cos://} scheme
   * @throws NoSuchTableException if the table does not exist
   * @throws IllegalArgumentException if HMS returns a null/empty location
   */
  public String getTableLocation(String database, String table) throws NoSuchTableException {
    if (!databaseExists(database)) {
      throw new NoSuchDatabaseException("Database does not exist: " + database);
    }

    DescribeTableRequest request = new DescribeTableRequest();
    request.setId(Arrays.asList(name(), database, table));
    DescribeTableResponse response;
    try {
      response = hive3Namespace.describeTable(request);
    } catch (TableNotFoundException e) {
      throw new NoSuchTableException("Table does not exist: " + database + "." + table);
    }

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
    if (!databaseExists(database)) {
      throw new NoSuchDatabaseException("Database does not exist: " + database);
    }

    List<String> tableId = Arrays.asList(name(), database, table);
    if (purge) {
      DropTableRequest request = new DropTableRequest();
      request.setId(tableId);
      try {
        hive3Namespace.dropTable(request);
      } catch (TableNotFoundException e) {
        throw new NoSuchTableException("Table does not exist: " + database + "." + table);
      }
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
    if (!databaseExists(database)) {
      throw new NoSuchDatabaseException("Database does not exist: " + database);
    }

    DeregisterTableRequest request = new DeregisterTableRequest();
    List<String> tableId = Arrays.asList(name(), database, table);
    request.setId(tableId);
    DeregisterTableResponse response;
    try {
      response = hive3Namespace.deregisterTable(request);
    } catch (TableNotFoundException e) {
      throw new NoSuchTableException("Table does not exist: " + database + "." + table);
    }
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
    // The location passed to HMS uses the cosn:// scheme (Hive needs it for Hadoop FileSystem
    // recognition)
    String hmsLocation = LanceProperties.toCosnScheme(location);
    // 1. Register table metadata in Hive Metastore via declareTable
    DeclareTableResponse declareResponse =
        declareTable(tableIdentifier, hmsLocation, tableProperties);

    String resolvedLocation = declareResponse.getLocation();
    tableProperties.put(LanceProperties.TABLE_LOCATION, resolvedLocation);

    // 2. Extract lance.storage. prefixed storage options from tableProperties and remove them
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

    // 3. Update table metadata via UpdateConfig (storage options already removed)
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
   * Build the default table storage location from the root path.
   *
   * <p>Path format:
   *
   * <ul>
   *   <li>when catalogName is "hive": {root}/{dbName}.db/{tableName}.lance
   *   <li>otherwise: {root}/{catalogName}/{dbName}.db/{tableName}.lance
   * </ul>
   *
   * @param dbName database name
   * @param tableName table name
   * @return the default table storage location, or null if root is not configured
   */
  public String getDefaultTableLocation(String dbName, String tableName) {
    if (root == null || root.isEmpty()) {
      LOG.warn(
          "ROOT parameter ({}) is not configured, cannot generate default location, will use Hive default location allocation",
          Hive3NamespaceConfig.ROOT);
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
   * declareTable does not persist table properties, and subsequent writes via the namespace do not
   * automatically inject location, causing downstream loadTable to miss the location parameter.
   * Check whether the passed-in properties contain the location field; if missing, inject it
   * directly from dataset.uri() into properties (without producing a commit).
   *
   * @param dataset the opened Lance dataset
   * @param properties the properties to complete; this method modifies the Map in place
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
    // The location passed to HMS uses the cosn:// scheme (Hive needs it for Hadoop FileSystem
    // recognition)
    request.setLocation(LanceProperties.toCosnScheme(location));
    // The underlying hive does not perform any operation on tableProperties
    request.setProperties(tableProperties);
    DeclareTableResponse response = this.hive3Namespace.declareTable(request);

    // Convert HMS-returned location to the cos:// scheme used by the Lance SDK
    response.setLocation(LanceProperties.toCosScheme(response.getLocation()));
    return response;
  }
}
