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

import static org.apache.amoro.formats.lance.LanceProperties.COS_REGION;
import static org.apache.amoro.formats.lance.LanceProperties.COS_SECRET_ID;
import static org.apache.amoro.formats.lance.LanceProperties.COS_SECRET_KEY;
import static org.apache.amoro.formats.lance.LanceProperties.COS_SECURITY_TOKEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.amoro.AlreadyExistsException;
import org.apache.amoro.AmoroTable;
import org.apache.amoro.DatabaseNotEmptyException;
import org.apache.amoro.NoSuchDatabaseException;
import org.apache.amoro.NoSuchTableException;
import org.apache.amoro.hive.TestHMS;
import org.apache.amoro.table.TableIdentifier;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.ipc.LanceScanner;
import org.lance.namespace.hive3.Hive3Namespace;
import org.lance.namespace.hive3.Hive3NamespaceConfig;
import org.lance.namespace.model.DeclareTableResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration tests for LanceHiveCatalog (using a real HMSMockServer environment)
 *
 * <p>This test class creates a real Hive Metastore environment via {@link TestHMS} (HMSMockServer),
 * connects to HMSMockServer using {@link Hive3Namespace}, builds a real {@link LanceHiveCatalog}
 * instance, and tests all public methods of {@link LanceHiveCatalog}.
 *
 * <p>Differences from {@link TestLanceHiveCatalogMock}:
 *
 * <ul>
 *   <li>Mock tests use Mockito to mock LanceNamespace and verify method call parameters
 *   <li>This test uses a real Hive3Namespace + HMSMockServer to verify end-to-end functionality
 * </ul>
 */
public class TestLanceHiveCatalogIT {

  private static final Logger LOG = LoggerFactory.getLogger(TestLanceHiveCatalogIT.class);

  private static final String CATALOG_NAME = "hive_catalog";
  private static final String TEST_DB = "test_db";
  private static final String TEST_DB2 = "test_db2";
  private static final String TEST_TABLE = "test_table";

  /** Arrow Schema definition, used for creating Lance tables */
  private static final Schema ARROW_SCHEMA =
      new Schema(
          Arrays.asList(
              Field.nullable("id", new ArrowType.Int(64, true)),
              Field.nullable("name", ArrowType.Utf8.INSTANCE),
              Field.nullable("age", new ArrowType.Int(32, true))));

  /** Use HMSMockServer to simulate Hive Metastore */
  @ClassRule public static TestHMS TEST_HMS = new TestHMS();

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private LanceHiveCatalog catalog;
  private BufferAllocator allocator;
  private String warehouseLocation;

  @Before
  public void setUp() throws IOException {
    allocator = new RootAllocator(Long.MAX_VALUE);
    warehouseLocation = tempFolder.newFolder("lance_warehouse").getAbsolutePath();

    // Use a real Hive3Namespace to connect to HMSMockServer
    Hive3Namespace namespace = new Hive3Namespace();
    namespace.setHadoopConf(TEST_HMS.getHiveConf());

    Map<String, String> properties = new HashMap<>();
    properties.put(Hive3NamespaceConfig.ROOT, warehouseLocation);
    properties.put("uri", TEST_HMS.getHiveConf().get(HiveConf.ConfVars.METASTOREURIS.varname));
    namespace.initialize(properties, allocator);

    catalog = new LanceHiveCatalog(CATALOG_NAME, namespace, properties, allocator);

    LOG.info(
        "Test setup complete. HMS port: {}, Warehouse: {}",
        TEST_HMS.getMetastorePort(),
        warehouseLocation);
  }

  @After
  public void tearDown() {
    // Clean up databases and tables created by tests
    try {
      for (String db : catalog.listDatabases()) {
        if ("default".equals(db)) {
          continue;
        }
        try {
          List<String> tables = catalog.listTables(db);
          for (String table : tables) {
            try {
              catalog.dropTable(db, table, true);
            } catch (Exception e) {
              LOG.warn("Failed to clean up table {}.{}: {}", db, table, e.getMessage());
            }
          }
          catalog.dropDatabase(db);
        } catch (Exception e) {
          LOG.warn("Failed to clean up database {}: {}", db, e.getMessage());
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to clean up test data: {}", e.getMessage());
    }

    if (allocator != null) {
      allocator.close();
    }
  }

  // ==================== name() Tests ====================

  @Test
  public void testName() {
    assertEquals(CATALOG_NAME, catalog.name());
  }

  @Test
  public void testNameWithNullCatalogName() {
    LanceHiveCatalog nullNameCatalog = new LanceHiveCatalog(null, null, new HashMap<>(), allocator);
    assertEquals("hive", nullNameCatalog.name());
  }

  // ==================== createDatabase Tests ====================

  @Test
  public void testCreateDatabase() {
    catalog.createDatabase(TEST_DB);

    List<String> databases = catalog.listDatabases();
    assertTrue("created database should appear in the list", databases.contains(TEST_DB));
  }

  @Test
  public void testCreateDatabaseWithMetadata() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("owner", "test_user");
    metadata.put("description", "test database");

    catalog.createDatabase(TEST_DB, metadata);

    List<String> databases = catalog.listDatabases();
    assertTrue("created database should appear in the list", databases.contains(TEST_DB));
  }

  @Test
  public void testCreateDatabaseAlreadyExists() {
    catalog.createDatabase(TEST_DB);
    assertTrue("database should already exist", catalog.databaseExists(TEST_DB));

    // Creating a duplicate database should throw AlreadyExistsException
    assertThrows(
        "creating a duplicate database should throw AlreadyExistsException",
        AlreadyExistsException.class,
        () -> catalog.createDatabase(TEST_DB));
  }

  @Test
  public void testCreateDatabaseWithMetadataAlreadyExists() {
    catalog.createDatabase(TEST_DB);
    assertTrue("database should already exist", catalog.databaseExists(TEST_DB));

    Map<String, String> metadata = new HashMap<>();
    metadata.put("owner", "test_user");

    // Creating a duplicate database with metadata should also throw AlreadyExistsException
    assertThrows(
        "creating a duplicate database with metadata should throw AlreadyExistsException",
        AlreadyExistsException.class,
        () -> catalog.createDatabase(TEST_DB, metadata));
  }

  // ==================== listDatabases Tests ====================

  @Test
  public void testListDatabasesEmpty() {
    List<String> databases = catalog.listDatabases();
    assertNotNull("database list should not be null", databases);
    // HMSMockServer hive catalog has a default database by default
    assertTrue("should contain at least the default database", databases.contains("default"));
  }

  @Test
  public void testListDatabasesMultiple() {
    catalog.createDatabase(TEST_DB);
    catalog.createDatabase(TEST_DB2);

    List<String> databases = catalog.listDatabases();
    assertTrue("should contain test_db", databases.contains(TEST_DB));
    assertTrue("should contain test_db2", databases.contains(TEST_DB2));
  }

  // ==================== databaseExists Tests ====================

  @Test
  public void testDatabaseExistsTrue() {
    catalog.createDatabase(TEST_DB);
    assertTrue("created database should exist", catalog.databaseExists(TEST_DB));
  }

  @Test
  public void testDatabaseExistsFalse() {
    assertFalse(
        "non-existent database should return false", catalog.databaseExists("non_existent_db"));
  }

  @Test
  public void testDatabaseExistsDefault() {
    assertTrue("default database should exist", catalog.databaseExists("default"));
  }

  // ==================== dropDatabase Tests ====================

  @Test
  public void testDropDatabase() {
    catalog.createDatabase(TEST_DB);
    assertTrue("database should exist", catalog.databaseExists(TEST_DB));

    catalog.dropDatabase(TEST_DB);
    assertFalse("database should not exist after drop", catalog.databaseExists(TEST_DB));
  }

  @Test
  public void testDropDatabaseNotExists() {
    assertFalse("database should not exist", catalog.databaseExists("non_existent_db"));

    assertThrows(
        "dropping a non-existent database should throw NoSuchDatabaseException",
        NoSuchDatabaseException.class,
        () -> catalog.dropDatabase("non_existent_db"));
  }

  @Test
  public void testDropNonEmptyDatabase() throws IOException {
    catalog.createDatabase(TEST_DB);

    // Create a table in the database
    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "blocking_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "blocking_table");
    catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());
    assertTrue("table should exist", catalog.tableExists(TEST_DB, "blocking_table"));

    // Dropping a non-empty database should throw DatabaseNotEmptyException
    assertThrows(
        "dropping a non-empty database should throw DatabaseNotEmptyException",
        DatabaseNotEmptyException.class,
        () -> catalog.dropDatabase(TEST_DB));

    // Verify database and table still exist
    assertTrue("database should still exist", catalog.databaseExists(TEST_DB));
    assertTrue("table should still exist", catalog.tableExists(TEST_DB, "blocking_table"));
  }

  @Test
  public void testDropNonEmptyDatabaseWithMultipleTables() throws IOException {
    catalog.createDatabase(TEST_DB);

    // Create multiple tables in the database
    for (int i = 1; i <= 3; i++) {
      String loc = warehouseLocation + "/" + TEST_DB + "/" + "multi_table_" + i;
      TableIdentifier tid = TableIdentifier.of(CATALOG_NAME, TEST_DB, "multi_table_" + i);
      catalog.createTable(tid, loc, ARROW_SCHEMA, new HashMap<>());
    }
    assertEquals("should have 3 tables", 3, catalog.listTables(TEST_DB).size());

    // Dropping a non-empty database should throw an exception
    assertThrows(
        "dropping a database with multiple tables should throw DatabaseNotEmptyException",
        DatabaseNotEmptyException.class,
        () -> catalog.dropDatabase(TEST_DB));
  }

  // ==================== loadDatabaseMetadata Tests ====================

  @Test
  public void testLoadDatabaseMetadata() {
    catalog.createDatabase(TEST_DB);

    Map<String, String> metadata = catalog.loadDatabaseMetadata(TEST_DB);
    assertNotNull("database metadata should not be null", metadata);
  }

  @Test
  public void testLoadDatabaseMetadataWithProperties() {
    Map<String, String> props = new HashMap<>();
    props.put("custom.key", "custom.value");
    catalog.createDatabase(TEST_DB, props);

    Map<String, String> metadata = catalog.loadDatabaseMetadata(TEST_DB);
    assertEquals("database metadata read error", "custom.value", metadata.get("custom.key"));
  }

  @Test
  public void testLoadDatabaseMetadataNotExists() {
    // Loading metadata of a non-existent database should throw NoSuchDatabaseException
    assertThrows(
        "loading metadata of a non-existent database should throw NoSuchDatabaseException",
        NoSuchDatabaseException.class,
        () -> catalog.loadDatabaseMetadata("non_existent_db"));
  }

  // ==================== listTables Tests ====================

  @Test
  public void testListTablesEmpty() {
    catalog.createDatabase(TEST_DB);

    List<String> tables = catalog.listTables(TEST_DB);
    assertNotNull("table list should not be null", tables);
    assertTrue("table list of a new database should be empty", tables.isEmpty());
  }

  @Test
  public void testListTablesAfterCreateTable() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + TEST_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, TEST_TABLE);
    catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    List<String> tables = catalog.listTables(TEST_DB);
    assertTrue("should contain the created table", tables.contains(TEST_TABLE));
  }

  @Test
  public void testListTablesMoreThanPageSize() throws IOException {
    catalog.createDatabase(TEST_DB);
    int tableCount = 110;
    for (int i = 0; i < tableCount; i++) {
      String tableName = "page_table_" + i;
      String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + tableName;
      TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, tableName);
      catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());
    }

    List<String> tables = catalog.listTables(TEST_DB);
    assertEquals(
        "pagination fix should list all " + tableCount + " tables", tableCount, tables.size());
    assertTrue("should contain the first table", tables.contains("page_table_0"));
    assertTrue("should contain the last table", tables.contains("page_table_" + (tableCount - 1)));
  }

  // ==================== Operations on Non-Existent Database ====================

  @Test
  public void testLoadTableFromNonExistentDatabase() {
    // Loading a table from a non-existent database should throw NoSuchTableException
    assertFalse("database should not exist", catalog.databaseExists("non_existent_db"));
    assertThrows(
        "loading a table from a non-existent database should throw NoSuchDatabaseException",
        NoSuchDatabaseException.class,
        () -> catalog.loadTable("non_existent_db", "some_table"));
  }

  @Test
  public void testDropTableFromNonExistentDatabase() {
    // Dropping a table from a non-existent database should throw NoSuchTableException
    assertFalse("database should not exist", catalog.databaseExists("non_existent_db"));
    assertThrows(
        "dropping a table from a non-existent database should throw NoSuchDatabaseException",
        NoSuchDatabaseException.class,
        () -> catalog.dropTable("non_existent_db", "some_table", true));
  }

  @Test
  public void testDeregisterTableFromNonExistentDatabase() {
    // Deregistering a table from a non-existent database should throw NoSuchTableException
    assertFalse("database should not exist", catalog.databaseExists("non_existent_db"));
    assertThrows(
        "deregistering a table from a non-existent database should throw NoSuchDatabaseException",
        NoSuchDatabaseException.class,
        () -> catalog.deregisterTable("non_existent_db", "some_table"));
  }

  // ==================== setDatabaseProperties / removeDatabaseProperties Unsupported Operation
  // Tests
  // ====================

  @Test
  public void testSetDatabasePropertiesUnsupported() {
    catalog.createDatabase(TEST_DB);
    Map<String, String> newProps = new HashMap<>();
    newProps.put("key", "value");
    // TODO: hiveNamespace does not support this
    catalog.setDatabaseProperties(TEST_DB, newProps);
  }

  @Test
  public void testRemoveDatabasePropertiesUnsupported() {
    catalog.createDatabase(TEST_DB);

    java.util.Set<String> keysToRemove = new java.util.HashSet<>();
    keysToRemove.add("key");
    // TODO: hiveNamespace does not support this
    catalog.removeDatabaseProperties(TEST_DB, keysToRemove);
  }

  // ==================== tableExists Tests ====================

  @Test
  public void testTableExistsFalse() {
    catalog.createDatabase(TEST_DB);
    assertFalse(
        "non-existent table should return false",
        catalog.tableExists(TEST_DB, "non_existent_table"));
  }

  @Test
  public void testTableExistsTrue() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + TEST_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, TEST_TABLE);
    catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    assertTrue("created table should exist", catalog.tableExists(TEST_DB, TEST_TABLE));
  }

  // ==================== createTable Tests ====================

  @Test
  public void testCreateTable() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + TEST_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, TEST_TABLE);

    LanceTable lanceTable =
        catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    assertNotNull("created table should not be null", lanceTable);
    assertEquals("table identifier should match", tableId, lanceTable.id());
    assertNotNull("original Dataset should not be null", lanceTable.originalTable());
  }

  @Test
  public void testCreateTableWithProperties() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "prop_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "prop_table");
    Map<String, String> tableProps = new HashMap<>();
    tableProps.put("custom.key", "custom.value");

    LanceTable lanceTable = catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, tableProps);

    assertNotNull("created table should not be null", lanceTable);
    assertEquals(
        "table properties should match", "custom.value", lanceTable.properties().get("custom.key"));
  }

  @Test
  public void testCreateTableWithoutLocation() throws IOException {
    catalog.createDatabase(TEST_DB);

    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "no_loc_table");

    // Pass null location, let Hive auto-assign the path
    LanceTable lanceTable = catalog.createTable(tableId, null, ARROW_SCHEMA, new HashMap<>());
    assertNotNull("table created without location should not be null", lanceTable);
    assertEquals("table identifier should match", tableId, lanceTable.id());
    assertNotNull("original Dataset should not be null", lanceTable.originalTable());
    assertNotNull(
        "location should not be null", lanceTable.properties().get(LanceProperties.TABLE_LOCATION));

    // Verify the table is registered in Hive and can be loaded
    assertTrue("table should exist", catalog.tableExists(TEST_DB, "no_loc_table"));
    AmoroTable<?> loadedTable = catalog.loadTable(TEST_DB, "no_loc_table");
    assertNotNull("loaded table should not be null", loadedTable);
    assertEquals("table name should match", "no_loc_table", loadedTable.id().getTableName());
    assertNotNull(
        "location should not be null",
        loadedTable.properties().get(LanceProperties.TABLE_LOCATION));
  }

  @Test
  public void testCreateTableWithEmptyLocation() throws IOException {
    catalog.createDatabase(TEST_DB);

    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "empty_loc_table");

    // Pass empty string location, let Hive auto-assign the path
    LanceTable lanceTable = catalog.createTable(tableId, "", ARROW_SCHEMA, new HashMap<>());

    assertNotNull("table created with empty location should not be null", lanceTable);
    assertEquals("table identifier should match", tableId, lanceTable.id());

    // Verify the table is registered in Hive and can be loaded
    assertTrue("table should exist", catalog.tableExists(TEST_DB, "empty_loc_table"));
    AmoroTable<?> loadedTable = catalog.loadTable(TEST_DB, "empty_loc_table");
    assertNotNull("loaded table should not be null", loadedTable);
  }

  @Test
  public void testCreateTableAlreadyExists() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "dup_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "dup_table");
    catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    assertTrue("table should already exist", catalog.tableExists(TEST_DB, "dup_table"));

    // Creating a duplicate table should throw AlreadyExistsException
    String tableLocation2 = warehouseLocation + "/" + TEST_DB + "/" + "dup_table_2";
    assertThrows(
        "creating a duplicate table should throw AlreadyExistsException",
        AlreadyExistsException.class,
        () -> catalog.createTable(tableId, tableLocation2, ARROW_SCHEMA, new HashMap<>()));
  }

  // ==================== declareTable Tests ====================

  @Test
  public void testDeclareTable() {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "declared_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "declared_table");

    var response = catalog.declareTable(tableId, tableLocation, new HashMap<>());
    assertNotNull("declareTable response should not be null", response);

    // Verify table is registered in Hive
    assertTrue("declared table should exist", catalog.tableExists(TEST_DB, "declared_table"));
  }

  @Test
  public void testDeclareTableWithoutLocation() {
    catalog.createDatabase(TEST_DB);

    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "auto_loc_table");

    // No location provided, let Hive auto-assign
    var response = catalog.declareTable(tableId, null, new HashMap<>());
    assertNotNull("declareTable response should not be null", response);
  }

  @Test
  public void testDeclareTableAlreadyExists() {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "dup_declare_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "dup_declare_table");

    // First declare should succeed
    catalog.declareTable(tableId, tableLocation, new HashMap<>());
    assertTrue("table should already exist", catalog.tableExists(TEST_DB, "dup_declare_table"));

    // Declaring a duplicate table should throw AlreadyExistsException
    assertThrows(
        "declaring a duplicate table should throw AlreadyExistsException",
        AlreadyExistsException.class,
        () -> catalog.declareTable(tableId, tableLocation, new HashMap<>()));
  }

  // ==================== loadTable Tests ====================

  @Test
  public void testLoadTable() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + TEST_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, TEST_TABLE);
    catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    AmoroTable<?> loadedTable = catalog.loadTable(TEST_DB, TEST_TABLE);

    assertNotNull("loaded table should not be null", loadedTable);
    assertEquals("table name should match", TEST_TABLE, loadedTable.id().getTableName());
    assertEquals("database name should match", TEST_DB, loadedTable.id().getDatabase());
    assertEquals("catalog name should match", CATALOG_NAME, loadedTable.id().getCatalog());
  }

  @Test
  public void testLoadTableNotExists() {
    catalog.createDatabase(TEST_DB);

    assertThrows(
        "loading a non-existent table should throw NoSuchTableException",
        NoSuchTableException.class,
        () -> catalog.loadTable(TEST_DB, "non_existent_table"));
  }

  @Test
  public void testLoadTableAfterDeclareWithLocation() {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "declared_only_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "declared_only_table");

    // Register table only via declareTable, do not create actual data
    DeclareTableResponse declareResponse =
        catalog.declareTable(tableId, tableLocation, new HashMap<>());
    assertNotNull("declareTable response should not be null", declareResponse);

    // Table is registered in HMS
    assertTrue("declared table should exist", catalog.tableExists(TEST_DB, "declared_only_table"));

    // loadTable should return an empty LanceTable (actual data does not exist)
    AmoroTable<?> loadedTable = catalog.loadTable(TEST_DB, "declared_only_table");
    assertNotNull("loaded table should not be null", loadedTable);
    assertEquals("table name should match", "declared_only_table", loadedTable.id().getTableName());
    assertEquals("database name should match", TEST_DB, loadedTable.id().getDatabase());

    // Verify empty table flag
    assertEquals(
        "should be marked as empty table",
        "true",
        loadedTable.properties().get(LanceProperties.TABLE_EMPTY));
    assertNotNull(
        "properties should contain location",
        loadedTable.properties().get(LanceProperties.TABLE_LOCATION));

    // Verify originalTable is null (no actual data)
    LanceTable lanceTable = (LanceTable) loadedTable;
    assertNull("empty table Dataset should be null", lanceTable.originalTable());
    assertNull("empty table snapshot should be null", lanceTable.currentSnapshot());
  }

  @Test
  public void testLoadTableAfterDeclareWithoutLocation() {
    catalog.createDatabase(TEST_DB);

    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "declared_no_loc_table");

    // No location provided, let Hive auto-assign
    DeclareTableResponse declareResponse = catalog.declareTable(tableId, null, new HashMap<>());
    assertNotNull("declareTable response should not be null", declareResponse);

    // loadTable should return an empty LanceTable
    AmoroTable<?> loadedTable = catalog.loadTable(TEST_DB, "declared_no_loc_table");
    assertNotNull("loaded table should not be null", loadedTable);
    assertEquals(
        "table name should match", "declared_no_loc_table", loadedTable.id().getTableName());

    // Verify empty table flag
    assertEquals(
        "should be marked as empty table",
        "true",
        loadedTable.properties().get(LanceProperties.TABLE_EMPTY));
    assertNotNull(
        "properties should contain location",
        loadedTable.properties().get(LanceProperties.TABLE_LOCATION));

    // Verify originalTable is null
    LanceTable lanceTable = (LanceTable) loadedTable;
    assertNull("empty table Dataset should be null", lanceTable.originalTable());
  }

  // ==================== dropTable Tests ====================

  @Test
  public void testDropTableWithoutPurge() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + TEST_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, TEST_TABLE);
    catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    assertTrue("table should exist before drop", catalog.tableExists(TEST_DB, TEST_TABLE));

    // purge=false only deregisters, does not delete data
    boolean result = catalog.dropTable(TEST_DB, TEST_TABLE, false);
    assertTrue("dropTable should return true", result);
    assertFalse(
        "table should not exist after deregister", catalog.tableExists(TEST_DB, TEST_TABLE));

    // Verify data files still exist (only deregistered, data not deleted)
    File tableDir = new File(tableLocation);
    assertTrue("data files should still exist when purge=false", tableDir.exists());
  }

  @Test
  public void testDropTableWithPurge() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "purge_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "purge_table");
    catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    assertTrue("table should exist before drop", catalog.tableExists(TEST_DB, "purge_table"));

    // purge=true deletes data
    boolean result = catalog.dropTable(TEST_DB, "purge_table", true);
    assertTrue("dropTable with purge should return true", result);
    assertFalse(
        "table should not exist after purge drop", catalog.tableExists(TEST_DB, "purge_table"));

    // Verify data files have been deleted
    File tableDir = new File(tableLocation);
    assertFalse("data files should be deleted when purge=true", tableDir.exists());
  }

  @Test
  public void testDropTableNotExists() {
    catalog.createDatabase(TEST_DB);

    assertThrows(
        "dropping a non-existent table should throw NoSuchTableException (purge=true)",
        NoSuchTableException.class,
        () -> catalog.dropTable(TEST_DB, "non_existent_table", true));
  }

  @Test
  public void testDropTableNotExistsWithoutPurge() {
    catalog.createDatabase(TEST_DB);

    assertThrows(
        "dropping a non-existent table should throw NoSuchTableException (purge=false)",
        NoSuchTableException.class,
        () -> catalog.dropTable(TEST_DB, "non_existent_table", false));
  }

  // ==================== deregisterTable Tests ====================

  @Test
  public void testDeregisterTable() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "dereg_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "dereg_table");
    catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    assertTrue("table should exist before deregister", catalog.tableExists(TEST_DB, "dereg_table"));

    var response = catalog.deregisterTable(TEST_DB, "dereg_table");
    assertNotNull("deregisterTable response should not be null", response);
    assertFalse(
        "table should not exist after deregister", catalog.tableExists(TEST_DB, "dereg_table"));
  }

  @Test
  public void testDeregisterTableNotExists() {
    catalog.createDatabase(TEST_DB);

    assertThrows(
        "deregistering a non-existent table should throw NoSuchTableException",
        NoSuchTableException.class,
        () -> catalog.deregisterTable(TEST_DB, "non_existent_table"));
  }

  // ==================== getTableLocation Tests ====================

  @Test
  public void testGetTableLocation() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "loc_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "loc_table");
    LanceTable createdTable =
        catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());
    assertNotNull("created table should not be null", createdTable);

    String location = catalog.getTableLocation(TEST_DB, "loc_table");
    assertNotNull("location should not be null", location);
    assertFalse("location should not be empty", location.isEmpty());
    assertTrue(
        "location should contain table name or path fragment, actual: " + location,
        location.contains("loc_table"));
  }

  @Test
  public void testGetTableLocationMatchesLoadTable() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "loc_match_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "loc_match_table");
    catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    String directLocation = catalog.getTableLocation(TEST_DB, "loc_match_table");
    AmoroTable<?> loadedTable = catalog.loadTable(TEST_DB, "loc_match_table");
    String loadedLocation = loadedTable.properties().get(LanceProperties.TABLE_LOCATION);

    assertNotNull("getTableLocation return value should not be null", directLocation);
    assertNotNull("location from loadTable should not be null", loadedLocation);
    assertEquals(
        "getTableLocation should match location from loadTable",
        "file:" + loadedLocation,
        directLocation);
  }

  // ==================== extractStorageOptions Tests ====================

  @Test
  public void testExtractStorageOptionsEmpty() {
    Map<String, String> props = new HashMap<>();
    Map<String, String> storageOptions = LanceProperties.extractCatalogStorageOptions(props);
    assertNotNull("storage options should not be null", storageOptions);
    assertTrue("empty properties should return empty storage options", storageOptions.isEmpty());
  }

  @Test
  public void testExtractStorageOptionsWithCosConfig() {
    Map<String, String> props = new HashMap<>();
    props.put(COS_REGION, "ap-guangzhou");
    props.put(COS_SECRET_ID, "test-secret-id");
    props.put(COS_SECRET_KEY, "test-secret-key");
    props.put(COS_SECURITY_TOKEN, "test-security-token");
    props.put("unrelated.key", "unrelated.value");

    Map<String, String> storageOptions = LanceProperties.extractCatalogStorageOptions(props);
    assertEquals("should extract 4 COS configs", 4, storageOptions.size());
    assertEquals("region should match", "ap-guangzhou", storageOptions.get(COS_REGION));
    assertEquals("secret_id should match", "test-secret-id", storageOptions.get(COS_SECRET_ID));
    assertEquals("secret_key should match", "test-secret-key", storageOptions.get(COS_SECRET_KEY));
    assertEquals(
        "security_token should match",
        "test-security-token",
        storageOptions.get(COS_SECURITY_TOKEN));
    assertFalse("should not contain unrelated config", storageOptions.containsKey("unrelated.key"));
  }

  @Test
  public void testExtractStorageOptionsPartial() {
    Map<String, String> props = new HashMap<>();
    props.put(COS_REGION, "ap-beijing");
    // Only set region, not secret_id and secret_key

    Map<String, String> storageOptions = LanceProperties.extractCatalogStorageOptions(props);
    assertEquals("should extract only 1 config", 1, storageOptions.size());
    assertEquals("region should match", "ap-beijing", storageOptions.get(COS_REGION));
  }

  // ==================== End-to-End Flow Tests ====================

  @Test
  public void testEndToEndCreateAndLoadTable() throws IOException {
    // 1. Create database
    catalog.createDatabase(TEST_DB);
    assertTrue("database should exist", catalog.databaseExists(TEST_DB));

    // 2. Create table
    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + TEST_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, TEST_TABLE);
    LanceTable createdTable =
        catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());
    assertNotNull("created table should not be null", createdTable);

    // 3. Verify table exists
    assertTrue("table should exist", catalog.tableExists(TEST_DB, TEST_TABLE));
    List<String> tables = catalog.listTables(TEST_DB);
    assertTrue("table list should contain the created table", tables.contains(TEST_TABLE));

    // 4. Load table
    AmoroTable<?> loadedTable = catalog.loadTable(TEST_DB, TEST_TABLE);
    assertNotNull("loaded table should not be null", loadedTable);
    assertEquals("table identifier should match", tableId, loadedTable.id());

    // 5. Drop table
    boolean dropped = catalog.dropTable(TEST_DB, TEST_TABLE, false);
    assertTrue("drop should succeed", dropped);
    assertFalse("table should not exist after drop", catalog.tableExists(TEST_DB, TEST_TABLE));

    // 6. Drop database
    catalog.dropDatabase(TEST_DB);
    assertFalse("database should not exist after drop", catalog.databaseExists(TEST_DB));
  }

  @Test
  public void testEndToEndMultipleDatabasesAndTables() throws IOException {
    // Create multiple databases
    catalog.createDatabase(TEST_DB);
    catalog.createDatabase(TEST_DB2);

    // Create tables in different databases
    String table1Location = warehouseLocation + "/" + TEST_DB + "/" + "table1";
    TableIdentifier tableId1 = TableIdentifier.of(CATALOG_NAME, TEST_DB, "table1");
    catalog.createTable(tableId1, table1Location, ARROW_SCHEMA, new HashMap<>());

    String table2Location = warehouseLocation + "/" + TEST_DB + "/" + "table2";
    TableIdentifier tableId2 = TableIdentifier.of(CATALOG_NAME, TEST_DB, "table2");
    catalog.createTable(tableId2, table2Location, ARROW_SCHEMA, new HashMap<>());

    String table3Location = warehouseLocation + "/" + TEST_DB2 + "/" + "table3";
    TableIdentifier tableId3 = TableIdentifier.of(CATALOG_NAME, TEST_DB2, "table3");
    catalog.createTable(tableId3, table3Location, ARROW_SCHEMA, new HashMap<>());

    // Verify table lists of each database
    List<String> db1Tables = catalog.listTables(TEST_DB);
    assertEquals("test_db should have 2 tables", 2, db1Tables.size());
    assertTrue("should contain table1", db1Tables.contains("table1"));
    assertTrue("should contain table2", db1Tables.contains("table2"));

    List<String> db2Tables = catalog.listTables(TEST_DB2);
    assertEquals("test_db2 should have 1 table", 1, db2Tables.size());
    assertTrue("should contain table3", db2Tables.contains("table3"));

    // Verify database list
    List<String> databases = catalog.listDatabases();
    assertTrue("should contain test_db", databases.contains(TEST_DB));
    assertTrue("should contain test_db2", databases.contains(TEST_DB2));
  }

  // ==================== Data Write and Read Tests ====================

  @Test
  public void testCreateTableWriteDataAndRead() throws IOException {
    catalog.createDatabase(TEST_DB);

    // 1. Create default table
    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "rw_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "rw_table");
    LanceTable createdTable =
        catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());
    assertNotNull("created table should not be null", createdTable);

    // 2. Write mock data via Dataset API
    try (VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, allocator)) {
      BigIntVector idVector = (BigIntVector) root.getVector("id");
      VarCharVector nameVector = (VarCharVector) root.getVector("name");
      IntVector ageVector = (IntVector) root.getVector("age");

      // Write 3 rows of data
      root.allocateNew();
      idVector.setSafe(0, 1L);
      nameVector.setSafe(0, "Alice".getBytes(StandardCharsets.UTF_8));
      ageVector.setSafe(0, 30);

      idVector.setSafe(1, 2L);
      nameVector.setSafe(1, "Bob".getBytes(StandardCharsets.UTF_8));
      ageVector.setSafe(1, 25);

      idVector.setSafe(2, 3L);
      nameVector.setSafe(2, "Charlie".getBytes(StandardCharsets.UTF_8));
      ageVector.setSafe(2, 35);

      root.setRowCount(3);

      // Write data to existing Dataset in APPEND mode
      Dataset appendedDataset =
          Dataset.write()
              .allocator(allocator)
              .uri(tableLocation)
              .reader(vectorSchemaRootToArrowReader(root, allocator))
              .mode(WriteParams.WriteMode.APPEND)
              .execute();
      assertNotNull("Dataset after append should not be null", appendedDataset);
      appendedDataset.close();
    }

    // 3. Reopen Dataset and verify by reading
    Dataset readDataset = Dataset.open(tableLocation, allocator);
    assertEquals("should have 3 rows of data", 3, readDataset.countRows());

    // Read all data via Scanner
    LanceScanner scanner = readDataset.newScan();
    try (ArrowReader reader = scanner.scanBatches()) {
      List<Long> ids = new ArrayList<>();
      List<String> names = new ArrayList<>();
      List<Integer> ages = new ArrayList<>();

      while (reader.loadNextBatch()) {
        VectorSchemaRoot batchRoot = reader.getVectorSchemaRoot();
        BigIntVector idVec = (BigIntVector) batchRoot.getVector("id");
        VarCharVector nameVec = (VarCharVector) batchRoot.getVector("name");
        IntVector ageVec = (IntVector) batchRoot.getVector("age");

        for (int i = 0; i < batchRoot.getRowCount(); i++) {
          ids.add(idVec.get(i));
          names.add(new String(nameVec.get(i), StandardCharsets.UTF_8));
          ages.add(ageVec.get(i));
        }
      }

      assertEquals("should read 3 rows of id", 3, ids.size());
      assertTrue("should contain id=1", ids.contains(1L));
      assertTrue("should contain id=2", ids.contains(2L));
      assertTrue("should contain id=3", ids.contains(3L));

      assertTrue("should contain Alice", names.contains("Alice"));
      assertTrue("should contain Bob", names.contains("Bob"));
      assertTrue("should contain Charlie", names.contains("Charlie"));

      assertTrue("should contain age=30", ages.contains(30));
      assertTrue("should contain age=25", ages.contains(25));
      assertTrue("should contain age=35", ages.contains(35));
    }

    readDataset.close();
  }

  // ==================== DATA_STORAGE_VERSION Tests ====================

  @Test
  public void testCreateTableWithDataStorageVersion22WriteAndRead() throws IOException {
    catalog.createDatabase(TEST_DB);

    // 1. Create a Lance table with DATA_STORAGE_VERSION = 2.2
    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "v22_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "v22_table");
    Map<String, String> tableProps = new HashMap<>();
    tableProps.put(LanceProperties.DATA_STORAGE_VERSION, "2.2");

    LanceTable createdTable = catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, tableProps);
    assertNotNull("created v2.2 table should not be null", createdTable);

    // 2. Write mock data via Dataset API
    try (VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, allocator)) {
      BigIntVector idVector = (BigIntVector) root.getVector("id");
      VarCharVector nameVector = (VarCharVector) root.getVector("name");
      IntVector ageVector = (IntVector) root.getVector("age");

      // Write 3 rows of data
      root.allocateNew();
      idVector.setSafe(0, 100L);
      nameVector.setSafe(0, "David".getBytes(StandardCharsets.UTF_8));
      ageVector.setSafe(0, 28);

      idVector.setSafe(1, 200L);
      nameVector.setSafe(1, "Eve".getBytes(StandardCharsets.UTF_8));
      ageVector.setSafe(1, 32);

      idVector.setSafe(2, 300L);
      nameVector.setSafe(2, "Frank".getBytes(StandardCharsets.UTF_8));
      ageVector.setSafe(2, 40);

      root.setRowCount(3);

      // Write data to existing Dataset in APPEND mode
      Dataset appendedDataset =
          Dataset.write()
              .allocator(allocator)
              .uri(tableLocation)
              .reader(vectorSchemaRootToArrowReader(root, allocator))
              .mode(WriteParams.WriteMode.APPEND)
              .execute();
      assertNotNull("Dataset after append should not be null", appendedDataset);
      appendedDataset.close();
    }

    // 3. Reopen Dataset and verify by reading data
    Dataset readDataset = Dataset.open(tableLocation, allocator);
    assertEquals("should have 3 rows of data", 3, readDataset.countRows());

    // Read all data via Scanner
    LanceScanner scanner = readDataset.newScan();
    try (ArrowReader reader = scanner.scanBatches()) {
      List<Long> ids = new ArrayList<>();
      List<String> names = new ArrayList<>();
      List<Integer> ages = new ArrayList<>();

      while (reader.loadNextBatch()) {
        VectorSchemaRoot batchRoot = reader.getVectorSchemaRoot();
        BigIntVector idVec = (BigIntVector) batchRoot.getVector("id");
        VarCharVector nameVec = (VarCharVector) batchRoot.getVector("name");
        IntVector ageVec = (IntVector) batchRoot.getVector("age");

        for (int i = 0; i < batchRoot.getRowCount(); i++) {
          ids.add(idVec.get(i));
          names.add(new String(nameVec.get(i), StandardCharsets.UTF_8));
          ages.add(ageVec.get(i));
        }
      }

      assertEquals("should read 3 rows of id", 3, ids.size());
      assertTrue("should contain id=100", ids.contains(100L));
      assertTrue("should contain id=200", ids.contains(200L));
      assertTrue("should contain id=300", ids.contains(300L));

      assertTrue("should contain David", names.contains("David"));
      assertTrue("should contain Eve", names.contains("Eve"));
      assertTrue("should contain Frank", names.contains("Frank"));

      assertTrue("should contain age=28", ages.contains(28));
      assertTrue("should contain age=32", ages.contains(32));
      assertTrue("should contain age=40", ages.contains(40));
    }

    readDataset.close();

    // 4. Load the table via loadTable and verify DATA_STORAGE_VERSION is 2.2
    AmoroTable<?> loadedTable = catalog.loadTable(TEST_DB, "v22_table");
    assertNotNull("loaded table should not be null", loadedTable);
    assertEquals("table name should match", "v22_table", loadedTable.id().getTableName());
    assertEquals("database name should match", TEST_DB, loadedTable.id().getDatabase());

    // Verify lance.data-storage-version in properties is 2.2
    String dataStorageVersion = loadedTable.properties().get(LanceProperties.DATA_STORAGE_VERSION);
    assertEquals("DATA_STORAGE_VERSION should be 2.2", "2.2", dataStorageVersion);
  }

  // ==================== Helper Methods ====================

  /**
   * Convert a VectorSchemaRoot to an ArrowReader.
   *
   * <p>Serialize the VectorSchemaRoot to an in-memory byte array via the Arrow IPC streaming
   * protocol, then deserialize it back to an ArrowStreamReader.
   *
   * @param root the VectorSchemaRoot to convert
   * @param alloc BufferAllocator for memory allocation
   * @return ArrowReader containing the root data
   * @throws IOException if an IO error occurs during serialization/deserialization
   */
  private ArrowReader vectorSchemaRootToArrowReader(VectorSchemaRoot root, BufferAllocator alloc)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
      writer.start();
      writer.writeBatch();
      writer.end();
    }
    return new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), alloc);
  }
}
