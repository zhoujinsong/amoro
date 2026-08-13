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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.CommonUnifiedCatalog;
import org.apache.amoro.NoSuchDatabaseException;
import org.apache.amoro.NoSuchTableException;
import org.apache.amoro.OptimizedCommonUnifiedCatalog;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableIDWithFormat;
import org.apache.amoro.hive.TestHMS;
import org.apache.amoro.properties.CatalogMetaProperties;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.TableMetaStore;
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
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.types.Types;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Integration tests for CommonUnifiedCatalog, configured to support both ICEBERG and LANCE table
 * formats.
 *
 * <p>This test class creates a real Hive Metastore environment via {@link TestHMS} (HMSMockServer),
 * using {@link CommonUnifiedCatalog} to manage both Iceberg and Lance format tables.
 *
 * <p>Test coverage:
 *
 * <ul>
 *   <li>Lance table schema creation and loading
 *   <li>Iceberg table creation and loading
 *   <li>Listing tables of different formats via CommonUnifiedCatalog
 *   <li>Verify loadTable can correctly identify table format
 * </ul>
 */
public class TestUnifiedCatalogWithLanceIT {

  private static final Logger LOG = LoggerFactory.getLogger(TestUnifiedCatalogWithLanceIT.class);

  private static final String CATALOG_NAME = "hive";
  private static final String TEST_DB = "unified_test_db";
  private static final String LANCE_TABLE = "lance_table";
  private static final String ICEBERG_TABLE = "iceberg_table";

  /** Arrow Schema definition, used for creating Lance tables */
  private static final Schema ARROW_SCHEMA =
      new Schema(
          Arrays.asList(
              Field.nullable("id", new ArrowType.Int(64, true)),
              Field.nullable("name", ArrowType.Utf8.INSTANCE),
              Field.nullable("age", new ArrowType.Int(32, true))));

  /** Iceberg Schema definition, used for creating Iceberg tables */
  private static final org.apache.iceberg.Schema ICEBERG_SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.optional(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "age", Types.IntegerType.get()));

  /** Use HMSMockServer to simulate Hive Metastore */
  @ClassRule public static TestHMS TEST_HMS = new TestHMS();

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private OptimizedCommonUnifiedCatalog unifiedCatalog;
  private LanceHiveCatalog lanceCatalog;
  private BufferAllocator allocator;
  private String warehouseLocation;

  @Before
  public void setUp() throws IOException {
    allocator = new RootAllocator(Long.MAX_VALUE);
    warehouseLocation = tempFolder.newFolder("unified_warehouse").getAbsolutePath();

    String hmsUri = TEST_HMS.getHiveConf().get(HiveConf.ConfVars.METASTOREURIS.varname);

    // Build catalog properties supporting both ICEBERG and LANCE
    Map<String, String> catalogProperties = new HashMap<>();
    catalogProperties.put(
        CatalogMetaProperties.TABLE_FORMATS,
        TableFormat.ICEBERG.name()
            + ","
            + TableFormat.HIVE_V2.name()
            + ","
            + TableFormat.LANCE.name());
    catalogProperties.put(CatalogMetaProperties.KEY_WAREHOUSE, warehouseLocation);
    catalogProperties.put(CatalogProperties.URI, hmsUri);
    catalogProperties.put(Hive3NamespaceConfig.ROOT, warehouseLocation);

    // Build TableMetaStore (including HiveConf)
    TableMetaStore tableMetaStore =
        TableMetaStore.builder().withConfiguration(TEST_HMS.getHiveConf()).build();

    // Create CommonUnifiedCatalog
    unifiedCatalog =
        new OptimizedCommonUnifiedCatalog(
            CATALOG_NAME,
            CatalogMetaProperties.CATALOG_TYPE_HIVE,
            catalogProperties,
            tableMetaStore);

    // Also create a standalone LanceHiveCatalog for directly creating Lance tables, sharing
    // HiveConf with unifiedCatalog
    Hive3Namespace namespace = new Hive3Namespace();
    namespace.setHadoopConf(TEST_HMS.getHiveConf());
    Map<String, String> lanceProperties = new HashMap<>(catalogProperties);
    namespace.initialize(lanceProperties, allocator);
    lanceCatalog = new LanceHiveCatalog(CATALOG_NAME, namespace, lanceProperties, allocator);

    LOG.info(
        "Test setup complete. HMS port: {}, Warehouse: {}",
        TEST_HMS.getMetastorePort(),
        warehouseLocation);
  }

  @After
  public void tearDown() {
    // Clean up databases and tables created by tests
    try {
      for (String db : unifiedCatalog.listDatabases()) {
        if ("default".equals(db)) {
          continue;
        }
        try {
          List<TableIDWithFormat> tables = unifiedCatalog.listTables(db);
          for (TableIDWithFormat tableId : tables) {
            try {
              unifiedCatalog.dropTable(db, tableId.table(), true);
            } catch (Exception e) {
              LOG.warn("Failed to clean up table {}.{}: {}", db, tableId.table(), e.getMessage());
            }
          }
          unifiedCatalog.dropDatabase(db);
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

  // ==================== Basic Functionality Tests ====================

  @Test
  public void testUnifiedCatalogName() {
    assertEquals("catalog name should match", CATALOG_NAME, unifiedCatalog.name());
  }

  @Test
  public void testUnifiedCatalogMetastoreType() {
    assertEquals(
        "metastore type should be hive",
        CatalogMetaProperties.CATALOG_TYPE_HIVE,
        unifiedCatalog.metastoreType());
  }

  @Test
  public void testCreateAndListDatabases() {
    unifiedCatalog.createDatabase(TEST_DB);

    List<String> databases = unifiedCatalog.listDatabases();
    assertTrue("should contain the created database", databases.contains(TEST_DB));
    assertTrue("should contain the default database", databases.contains("default"));
  }

  @Test
  public void testDatabaseExists() {
    unifiedCatalog.createDatabase(TEST_DB);
    assertTrue("created database should exist", unifiedCatalog.databaseExists(TEST_DB));
    assertFalse(
        "non-existent database should return false",
        unifiedCatalog.databaseExists("non_existent_db"));
  }

  // ==================== createDatabase Tests ====================

  @Test
  public void testCreateDatabaseWithMetadata() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("owner", "test_user");
    metadata.put("description", "test database with metadata");
    unifiedCatalog.createDatabase(TEST_DB, metadata);

    assertTrue(
        "database created with metadata should exist", unifiedCatalog.databaseExists(TEST_DB));
    List<String> databases = unifiedCatalog.listDatabases();
    assertTrue(
        "listDatabases should contain the database created with metadata",
        databases.contains(TEST_DB));
  }

  @Test
  public void testCreateDuplicateDatabase() {
    unifiedCatalog.createDatabase(TEST_DB);
    assertTrue("first database creation should succeed", unifiedCatalog.databaseExists(TEST_DB));

    // Creating a duplicate database should throw an exception
    org.junit.Assert.assertThrows(
        "creating a duplicate database should throw an exception",
        Exception.class,
        () -> unifiedCatalog.createDatabase(TEST_DB));
  }

  @Test
  public void testCreateMultipleDatabases() {
    String db1 = "test_db_1";
    String db2 = "test_db_2";
    String db3 = "test_db_3";

    unifiedCatalog.createDatabase(db1);
    unifiedCatalog.createDatabase(db2);
    unifiedCatalog.createDatabase(db3);

    List<String> databases = unifiedCatalog.listDatabases();
    assertTrue("should contain db1", databases.contains(db1));
    assertTrue("should contain db2", databases.contains(db2));
    assertTrue("should contain db3", databases.contains(db3));
  }

  // ==================== dropDatabase Tests ====================

  @Test
  public void testDropDatabase() {
    unifiedCatalog.createDatabase(TEST_DB);
    assertTrue("database should exist before drop", unifiedCatalog.databaseExists(TEST_DB));

    unifiedCatalog.dropDatabase(TEST_DB);
    assertFalse("database should not exist after drop", unifiedCatalog.databaseExists(TEST_DB));
  }

  @Test
  public void testDropAndRecreateDatabase() {
    // Create database
    unifiedCatalog.createDatabase(TEST_DB);
    assertTrue("database should exist after creation", unifiedCatalog.databaseExists(TEST_DB));

    // Drop database
    unifiedCatalog.dropDatabase(TEST_DB);
    assertFalse("database should not exist after drop", unifiedCatalog.databaseExists(TEST_DB));

    // Recreate the same database
    unifiedCatalog.createDatabase(TEST_DB);
    assertTrue("database should exist after recreation", unifiedCatalog.databaseExists(TEST_DB));
  }

  // ==================== listDatabases Tests ====================

  @Test
  public void testListDatabasesContainsDefault() {
    List<String> databases = unifiedCatalog.listDatabases();
    assertNotNull("listDatabases should not return null", databases);
    assertTrue("should contain the default database", databases.contains("default"));
  }

  @Test
  public void testListDatabasesAfterCreateAndDrop() {
    // Initial state
    List<String> initialDatabases = unifiedCatalog.listDatabases();
    int initialSize = initialDatabases.size();

    // List should grow after creating a database
    unifiedCatalog.createDatabase(TEST_DB);
    List<String> afterCreate = unifiedCatalog.listDatabases();
    assertEquals(
        "list size should increase by 1 after creating a database",
        initialSize + 1,
        afterCreate.size());
    assertTrue("list should contain the newly created database", afterCreate.contains(TEST_DB));

    // List should restore after dropping a database
    unifiedCatalog.dropDatabase(TEST_DB);
    List<String> afterDrop = unifiedCatalog.listDatabases();
    assertEquals(
        "list size should restore after dropping a database", initialSize, afterDrop.size());
    assertFalse("list should not contain the dropped database", afterDrop.contains(TEST_DB));
  }

  // ==================== databaseExists Tests ====================

  @Test
  public void testDatabaseExistsForDefault() {
    assertTrue("default database should always exist", unifiedCatalog.databaseExists("default"));
  }

  @Test
  public void testDatabaseExistsAfterDrop() {
    unifiedCatalog.createDatabase(TEST_DB);
    assertTrue(
        "databaseExists should return true after creation", unifiedCatalog.databaseExists(TEST_DB));

    unifiedCatalog.dropDatabase(TEST_DB);
    assertFalse(
        "databaseExists should return false after drop", unifiedCatalog.databaseExists(TEST_DB));
  }

  @Test
  public void testDatabaseExistsWithEmptyName() {
    assertFalse(
        "empty string database name should return false", unifiedCatalog.databaseExists(""));
  }

  // ==================== Lance Table Creation and Loading Tests ====================

  @Test
  public void testCreateAndLoadLanceTable() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    // Create Lance table via LanceHiveCatalog
    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + LANCE_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, LANCE_TABLE);
    LanceTable createdTable =
        lanceCatalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());
    assertNotNull("created Lance table should not be null", createdTable);

    // Load Lance table via CommonUnifiedCatalog
    AmoroTable<?> loadedTable = unifiedCatalog.loadTable(TEST_DB, LANCE_TABLE);
    assertNotNull("Lance table loaded via UnifiedCatalog should not be null", loadedTable);
    assertEquals("table format should be LANCE", TableFormat.LANCE, loadedTable.format());
    assertEquals("table name should match", LANCE_TABLE, loadedTable.id().getTableName());
    assertEquals("database name should match", TEST_DB, loadedTable.id().getDatabase());
  }

  @Test
  public void testCreateLanceTableWithSchema() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    // Create a Lance table with multiple field types
    Schema complexSchema =
        new Schema(
            Arrays.asList(
                Field.nullable("id", new ArrowType.Int(64, true)),
                Field.nullable("name", ArrowType.Utf8.INSTANCE),
                Field.nullable("age", new ArrowType.Int(32, true)),
                Field.nullable("active", ArrowType.Bool.INSTANCE)));

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "complex_lance_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "complex_lance_table");
    LanceTable createdTable =
        lanceCatalog.createTable(tableId, tableLocation, complexSchema, new HashMap<>());
    assertNotNull("Lance table with complex schema should not be null", createdTable);
    assertNotNull("original Dataset should not be null", createdTable.originalTable());

    // Load and verify via CommonUnifiedCatalog
    AmoroTable<?> loadedTable = unifiedCatalog.loadTable(TEST_DB, "complex_lance_table");
    assertNotNull("table loaded via UnifiedCatalog should not be null", loadedTable);
    assertEquals("table format should be LANCE", TableFormat.LANCE, loadedTable.format());
  }

  @Test
  public void testCreateLanceTableWithProperties() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "prop_lance_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "prop_lance_table");
    Map<String, String> tableProps = new HashMap<>();
    tableProps.put("custom.key", "custom.value");
    tableProps.put("description", "test lance table");

    LanceTable createdTable =
        lanceCatalog.createTable(tableId, tableLocation, ARROW_SCHEMA, tableProps);
    assertNotNull("Lance table with properties should not be null", createdTable);

    // Load and verify via CommonUnifiedCatalog
    AmoroTable<?> loadedTable = unifiedCatalog.loadTable(TEST_DB, "prop_lance_table");
    assertNotNull("table loaded via UnifiedCatalog should not be null", loadedTable);
    assertEquals("table format should be LANCE", TableFormat.LANCE, loadedTable.format());
  }

  @Test
  public void testLoadTableFormat() throws Exception {
    unifiedCatalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + LANCE_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, LANCE_TABLE);
    Map<String, String> tableProps = new HashMap<>();
    lanceCatalog.createTable(tableId, tableLocation, ARROW_SCHEMA, tableProps);

    TableFormat format = unifiedCatalog.loadTableFormat(TEST_DB, LANCE_TABLE);
    assertEquals("should be identified as LANCE format", TableFormat.LANCE, format);
  }

  @Test
  public void testCreateLanceTableWithoutLocation() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    // No location provided, let Hive auto-assign
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "auto_loc_lance_table");
    LanceTable createdTable =
        lanceCatalog.createTable(tableId, null, ARROW_SCHEMA, new HashMap<>());
    assertNotNull("Lance table created without location should not be null", createdTable);

    // Load via CommonUnifiedCatalog
    AmoroTable<?> loadedTable = unifiedCatalog.loadTable(TEST_DB, "auto_loc_lance_table");
    assertNotNull("table loaded via UnifiedCatalog should not be null", loadedTable);
    assertEquals("table format should be LANCE", TableFormat.LANCE, loadedTable.format());
  }

  // ==================== Iceberg Table Creation and Loading Tests ====================

  @Test
  public void testCreateAndLoadIcebergTable() {
    unifiedCatalog.createDatabase(TEST_DB);

    // Create Iceberg table via Iceberg Catalog
    createIcebergTable(TEST_DB, ICEBERG_TABLE);

    // Load Iceberg table via CommonUnifiedCatalog
    AmoroTable<?> loadedTable = unifiedCatalog.loadTable(TEST_DB, ICEBERG_TABLE);
    assertNotNull("Iceberg table loaded via UnifiedCatalog should not be null", loadedTable);
    assertEquals("table format should be ICEBERG", TableFormat.ICEBERG, loadedTable.format());
    assertEquals("table name should match", ICEBERG_TABLE, loadedTable.id().getTableName());
    assertEquals("database name should match", TEST_DB, loadedTable.id().getDatabase());
  }

  // ==================== Mixed Format Table Listing Tests ====================

  @Test
  public void testListTablesWithMixedFormats() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    // Create Lance table
    String lanceLocation = warehouseLocation + "/" + TEST_DB + "/" + LANCE_TABLE;
    TableIdentifier lanceTableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, LANCE_TABLE);
    lanceCatalog.createTable(lanceTableId, lanceLocation, ARROW_SCHEMA, new HashMap<>());

    // Create Iceberg table
    createIcebergTable(TEST_DB, ICEBERG_TABLE);

    // List all tables via CommonUnifiedCatalog
    List<TableIDWithFormat> tables = unifiedCatalog.listTables(TEST_DB);
    assertNotNull("table list should not be null", tables);

    // Verify both format tables are present
    Map<String, TableFormat> tableFormatMap =
        tables.stream()
            .collect(Collectors.toMap(TableIDWithFormat::table, TableIDWithFormat::getTableFormat));

    assertTrue("should contain Lance table", tableFormatMap.containsKey(LANCE_TABLE));
    assertTrue("should contain Iceberg table", tableFormatMap.containsKey(ICEBERG_TABLE));
    assertEquals(
        "Lance table format should be correct", TableFormat.LANCE, tableFormatMap.get(LANCE_TABLE));
    assertEquals(
        "Iceberg table format should be correct",
        TableFormat.ICEBERG,
        tableFormatMap.get(ICEBERG_TABLE));
  }

  @Test
  public void testTableExistsWithMixedFormats() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    // Create Lance table
    String lanceLocation = warehouseLocation + "/" + TEST_DB + "/" + LANCE_TABLE;
    TableIdentifier lanceTableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, LANCE_TABLE);
    lanceCatalog.createTable(lanceTableId, lanceLocation, ARROW_SCHEMA, new HashMap<>());

    // Create Iceberg table
    createIcebergTable(TEST_DB, ICEBERG_TABLE);

    // Verify both format tables exist
    assertTrue("Lance table should exist", unifiedCatalog.tableExists(TEST_DB, LANCE_TABLE));
    assertTrue("Iceberg table should exist", unifiedCatalog.tableExists(TEST_DB, ICEBERG_TABLE));
    assertFalse(
        "non-existent table should return false",
        unifiedCatalog.tableExists(TEST_DB, "non_existent_table"));
  }

  // ==================== Table Format Identification Tests ====================

  @Test
  public void testLoadTableFormatForLance() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + LANCE_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, LANCE_TABLE);
    lanceCatalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    // Verify loadTableFormat can correctly identify Lance format
    TableFormat format = unifiedCatalog.loadTableFormat(TEST_DB, LANCE_TABLE);
    assertEquals("should be identified as LANCE format", TableFormat.LANCE, format);
  }

  @Test
  public void testLoadTableFormatForIceberg() {
    unifiedCatalog.createDatabase(TEST_DB);
    createIcebergTable(TEST_DB, ICEBERG_TABLE);

    // Verify loadTableFormat can correctly identify Iceberg format
    TableFormat format = unifiedCatalog.loadTableFormat(TEST_DB, ICEBERG_TABLE);
    assertEquals("should be identified as ICEBERG format", TableFormat.ICEBERG, format);
  }

  @Test
  public void testLoadTableFormatForIcebergWithoutTableFormatProperty() throws Exception {
    unifiedCatalog.createDatabase(TEST_DB);
    createIcebergTable(TEST_DB, ICEBERG_TABLE);

    org.apache.hadoop.hive.metastore.api.Table hiveTable =
        TEST_HMS.getHiveClient().getTable(TEST_DB, ICEBERG_TABLE);
    hiveTable.getParameters().remove(CatalogMetaProperties.TABLE_FORMAT);
    hiveTable.getParameters().put(BaseMetastoreTableOperations.TABLE_TYPE_PROP, "ICEBERG");
    TEST_HMS.getHiveClient().alter_table(TEST_DB, ICEBERG_TABLE, hiveTable);

    TableFormat format = unifiedCatalog.loadTableFormat(TEST_DB, ICEBERG_TABLE);
    assertEquals("should still be identified as ICEBERG format", TableFormat.ICEBERG, format);
  }

  // ==================== Exception Tests ====================

  @Test
  public void testLoadNonExistentTable() {
    unifiedCatalog.createDatabase(TEST_DB);

    assertThrows(
        "loading a non-existent table should throw NoSuchTableException",
        NoSuchTableException.class,
        () -> unifiedCatalog.loadTable(TEST_DB, "non_existent_table"));
  }

  @Test
  public void testLoadTableFromNonExistentDatabase() {
    // Loading a table from a non-existent database should throw an exception
    assertFalse("database should not exist", unifiedCatalog.databaseExists("non_existent_db"));
    assertThrows(
        "loading a table from a non-existent database should throw NoSuchDatabaseException",
        NoSuchDatabaseException.class,
        () -> unifiedCatalog.loadTable("non_existent_db", "some_table"));
  }

  @Test
  public void testDropNonExistentDatabase() {
    assertFalse("database should not exist", unifiedCatalog.databaseExists("non_existent_db"));
    // iceberg catalog does not throw on drop, lance catalog throws; UnifiedCatalog may not select
    // lance
    unifiedCatalog.dropDatabase("non_existent_db");
  }

  @Test
  public void testDropNonEmptyDatabaseWithLanceTable() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    // Create a Lance table in the database
    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "blocking_lance_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "blocking_lance_table");
    lanceCatalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());
    assertTrue(
        "Lance table should exist", unifiedCatalog.tableExists(TEST_DB, "blocking_lance_table"));

    // Dropping a non-empty database: iceberg throws NamespaceNotEmptyException, lance should throw
    // DatabaseNotEmptyException
    assertThrows(
        "dropping a non-empty database with Lance table should throw NamespaceNotEmptyException or DatabaseNotEmptyException",
        Exception.class,
        () -> unifiedCatalog.dropDatabase(TEST_DB));

    // Verify database and table still exist
    assertTrue("database should still exist", unifiedCatalog.databaseExists(TEST_DB));
    assertTrue(
        "Lance table should still exist",
        unifiedCatalog.tableExists(TEST_DB, "blocking_lance_table"));
  }

  @Test
  public void testDropNonEmptyDatabaseWithIcebergTable() {
    unifiedCatalog.createDatabase(TEST_DB);

    // Create an Iceberg table in the database
    createIcebergTable(TEST_DB, "blocking_iceberg_table");
    assertTrue(
        "Iceberg table should exist",
        unifiedCatalog.tableExists(TEST_DB, "blocking_iceberg_table"));

    // Dropping a non-empty database should throw DatabaseNotEmptyException
    assertThrows(
        "dropping a non-empty database with Lance table should throw NamespaceNotEmptyException or DatabaseNotEmptyException",
        Exception.class,
        () -> unifiedCatalog.dropDatabase(TEST_DB));
  }

  @Test
  public void testDropNonEmptyDatabaseWithMixedFormatTables() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    // Create both Lance and Iceberg tables
    String lanceLocation = warehouseLocation + "/" + TEST_DB + "/" + LANCE_TABLE;
    TableIdentifier lanceTableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, LANCE_TABLE);
    lanceCatalog.createTable(lanceTableId, lanceLocation, ARROW_SCHEMA, new HashMap<>());
    createIcebergTable(TEST_DB, ICEBERG_TABLE);

    // Dropping a non-empty database with mixed format tables should throw an exception
    // Although sharing the same hive metastore, iceberg throws NamespaceNotEmptyException, lance
    // throws
    // DatabaseNotEmptyException
    assertThrows(
        "dropping a non-empty database with mixed format tables should throw NamespaceNotEmptyException | DatabaseNotEmptyException",
        Exception.class,
        () -> unifiedCatalog.dropDatabase(TEST_DB));

    // Verify all tables still exist
    assertTrue("Lance table should still exist", unifiedCatalog.tableExists(TEST_DB, LANCE_TABLE));
    assertTrue(
        "Iceberg table should still exist", unifiedCatalog.tableExists(TEST_DB, ICEBERG_TABLE));
  }

  @Test
  public void testDropNonExistentTable() {
    unifiedCatalog.createDatabase(TEST_DB);

    assertFalse(
        "dropping a non-existent table should return false",
        unifiedCatalog.dropTable(TEST_DB, "non_existent_table", true));
  }

  @Test
  public void testLoadTableFormatFromNonExistentDatabase() {
    assertFalse("database should not exist", unifiedCatalog.databaseExists("non_existent_db"));
    assertThrows(
        "loading table format from a non-existent database should throw an exception",
        Exception.class,
        () -> unifiedCatalog.loadTableFormat("non_existent_db", "some_table"));
  }

  // ==================== Drop Table Tests ====================

  @Test
  public void testDropLanceTable() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + LANCE_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, LANCE_TABLE);
    lanceCatalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    assertTrue(
        "Lance table should exist before drop", unifiedCatalog.tableExists(TEST_DB, LANCE_TABLE));

    boolean result = unifiedCatalog.dropTable(TEST_DB, LANCE_TABLE, true);
    assertTrue("dropTable should return true", result);
    assertFalse(
        "Lance table should not exist after drop",
        unifiedCatalog.tableExists(TEST_DB, LANCE_TABLE));
  }

  @Test
  public void testDropIcebergTable() {
    unifiedCatalog.createDatabase(TEST_DB);
    createIcebergTable(TEST_DB, ICEBERG_TABLE);

    assertTrue(
        "Iceberg table should exist before drop",
        unifiedCatalog.tableExists(TEST_DB, ICEBERG_TABLE));

    boolean result = unifiedCatalog.dropTable(TEST_DB, ICEBERG_TABLE, true);
    assertTrue("dropTable should return true", result);
    assertFalse(
        "Iceberg table should not exist after drop",
        unifiedCatalog.tableExists(TEST_DB, ICEBERG_TABLE));
  }

  // ==================== End-to-End Flow Tests ====================

  @Test
  public void testEndToEndMixedFormatWorkflow() throws IOException {
    // 1. Create database
    unifiedCatalog.createDatabase(TEST_DB);
    assertTrue("database should exist", unifiedCatalog.databaseExists(TEST_DB));

    // 2. Create Lance table
    String lanceLocation = warehouseLocation + "/" + TEST_DB + "/" + LANCE_TABLE;
    TableIdentifier lanceTableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, LANCE_TABLE);
    lanceCatalog.createTable(lanceTableId, lanceLocation, ARROW_SCHEMA, new HashMap<>());

    // 3. Create Iceberg table
    createIcebergTable(TEST_DB, ICEBERG_TABLE);

    // 4. List all tables via UnifiedCatalog
    List<TableIDWithFormat> tables = unifiedCatalog.listTables(TEST_DB);
    assertEquals("should have 2 tables", 2, tables.size());

    // 5. Load and verify formats separately
    AmoroTable<?> lanceTable = unifiedCatalog.loadTable(TEST_DB, LANCE_TABLE);
    assertEquals("Lance table format should be correct", TableFormat.LANCE, lanceTable.format());

    AmoroTable<?> icebergTable = unifiedCatalog.loadTable(TEST_DB, ICEBERG_TABLE);
    assertEquals(
        "Iceberg table format should be correct", TableFormat.ICEBERG, icebergTable.format());

    // 6. Drop Lance table
    unifiedCatalog.dropTable(TEST_DB, LANCE_TABLE, true);
    assertFalse(
        "Lance table should have been dropped", unifiedCatalog.tableExists(TEST_DB, LANCE_TABLE));
    assertTrue(
        "Iceberg table should still exist", unifiedCatalog.tableExists(TEST_DB, ICEBERG_TABLE));

    // 7. Drop Iceberg table
    unifiedCatalog.dropTable(TEST_DB, ICEBERG_TABLE, true);
    assertFalse(
        "Iceberg table should have been dropped",
        unifiedCatalog.tableExists(TEST_DB, ICEBERG_TABLE));

    // 8. Drop database
    unifiedCatalog.dropDatabase(TEST_DB);
    assertFalse("database should have been dropped", unifiedCatalog.databaseExists(TEST_DB));
  }

  // ==================== Helper Methods ====================

  /**
   * Create an Iceberg table via the native Iceberg Catalog.
   *
   * @param database database name
   * @param table table name
   */
  private void createIcebergTable(String database, String table) {
    Map<String, String> icebergProps = new HashMap<>();
    icebergProps.put(
        CatalogProperties.URI, TEST_HMS.getHiveConf().get(HiveConf.ConfVars.METASTOREURIS.varname));
    icebergProps.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);

    Catalog icebergCatalog =
        org.apache.iceberg.CatalogUtil.buildIcebergCatalog(
            CATALOG_NAME, icebergProps, TEST_HMS.getHiveConf());

    icebergCatalog.createTable(
        org.apache.iceberg.catalog.TableIdentifier.of(Namespace.of(database), table),
        ICEBERG_SCHEMA);
  }

  // ==================== Data Write and Read Tests ====================

  @Test
  public void testCreateLanceTableWriteDataAndRead() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    // 1. Create default Lance table
    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "rw_lance_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "rw_lance_table");
    LanceTable createdTable =
        lanceCatalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());
    assertNotNull("created Lance table should not be null", createdTable);

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

    // 4. Load and verify via UnifiedCatalog
    AmoroTable<?> loadedTable = unifiedCatalog.loadTable(TEST_DB, "rw_lance_table");
    assertNotNull("table loaded via UnifiedCatalog should not be null", loadedTable);
    assertEquals("table format should be LANCE", TableFormat.LANCE, loadedTable.format());
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
