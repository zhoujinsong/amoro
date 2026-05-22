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
 * LanceHiveCatalog 集成测试类（使用真实 HMSMockServer 环境）
 *
 * <p>本测试类通过 {@link TestHMS}（HMSMockServer）创建真实的 Hive Metastore 环境， 使用 {@link Hive3Namespace} 连接
 * HMSMockServer，构建真实的 {@link LanceHiveCatalog} 实例， 测试 {@link LanceHiveCatalog} 的所有公开接口方法。
 *
 * <p>与 {@link TestLanceHiveCatalogMock} 的区别：
 *
 * <ul>
 *   <li>Mock 测试使用 Mockito mock LanceNamespace，验证方法调用参数
 *   <li>本测试使用真实的 Hive3Namespace + HMSMockServer，验证端到端功能
 * </ul>
 */
public class TestLanceHiveCatalogIT {

  private static final Logger LOG = LoggerFactory.getLogger(TestLanceHiveCatalogIT.class);

  private static final String CATALOG_NAME = "hive_catalog";
  private static final String TEST_DB = "test_db";
  private static final String TEST_DB2 = "test_db2";
  private static final String TEST_TABLE = "test_table";

  /** Arrow Schema 定义，用于创建 Lance 表 */
  private static final Schema ARROW_SCHEMA =
      new Schema(
          Arrays.asList(
              Field.nullable("id", new ArrowType.Int(64, true)),
              Field.nullable("name", ArrowType.Utf8.INSTANCE),
              Field.nullable("age", new ArrowType.Int(32, true))));

  /** 使用 HMSMockServer 模拟 Hive Metastore */
  @ClassRule public static TestHMS TEST_HMS = new TestHMS();

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private LanceHiveCatalog catalog;
  private BufferAllocator allocator;
  private String warehouseLocation;

  @Before
  public void setUp() throws IOException {
    allocator = new RootAllocator(Long.MAX_VALUE);
    warehouseLocation = tempFolder.newFolder("lance_warehouse").getAbsolutePath();

    // 使用真实的 Hive3Namespace 连接 HMSMockServer
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
    // 清理测试创建的数据库和表
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
              LOG.warn("清理表 {}.{} 失败: {}", db, table, e.getMessage());
            }
          }
          catalog.dropDatabase(db);
        } catch (Exception e) {
          LOG.warn("清理数据库 {} 失败: {}", db, e.getMessage());
        }
      }
    } catch (Exception e) {
      LOG.warn("清理测试数据失败: {}", e.getMessage());
    }

    if (allocator != null) {
      allocator.close();
    }
  }

  // ==================== name() 测试 ====================

  @Test
  public void testName() {
    assertEquals(CATALOG_NAME, catalog.name());
  }

  @Test
  public void testNameWithNullCatalogName() {
    LanceHiveCatalog nullNameCatalog = new LanceHiveCatalog(null, null, new HashMap<>(), allocator);
    assertEquals("hive", nullNameCatalog.name());
  }

  // ==================== createDatabase 测试 ====================

  @Test
  public void testCreateDatabase() {
    catalog.createDatabase(TEST_DB);

    List<String> databases = catalog.listDatabases();
    assertTrue("创建的数据库应出现在列表中", databases.contains(TEST_DB));
  }

  @Test
  public void testCreateDatabaseWithMetadata() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("owner", "test_user");
    metadata.put("description", "test database");

    catalog.createDatabase(TEST_DB, metadata);

    List<String> databases = catalog.listDatabases();
    assertTrue("创建的数据库应出现在列表中", databases.contains(TEST_DB));
  }

  @Test
  public void testCreateDatabaseAlreadyExists() {
    catalog.createDatabase(TEST_DB);
    assertTrue("数据库应已存在", catalog.databaseExists(TEST_DB));

    // 重复创建同名数据库应抛出 AlreadyExistsException
    assertThrows(
        "重复创建数据库应抛出 AlreadyExistsException",
        AlreadyExistsException.class,
        () -> catalog.createDatabase(TEST_DB));
  }

  @Test
  public void testCreateDatabaseWithMetadataAlreadyExists() {
    catalog.createDatabase(TEST_DB);
    assertTrue("数据库应已存在", catalog.databaseExists(TEST_DB));

    Map<String, String> metadata = new HashMap<>();
    metadata.put("owner", "test_user");

    // 带 metadata 重复创建同名数据库也应抛出 AlreadyExistsException
    assertThrows(
        "带 metadata 重复创建数据库应抛出 AlreadyExistsException",
        AlreadyExistsException.class,
        () -> catalog.createDatabase(TEST_DB, metadata));
  }

  // ==================== listDatabases 测试 ====================

  @Test
  public void testListDatabasesEmpty() {
    List<String> databases = catalog.listDatabases();
    assertNotNull("数据库列表不应为 null", databases);
    // HMSMockServer hive catalog 默认有 default 数据库
    assertTrue("应至少包含 default 数据库", databases.contains("default"));
  }

  @Test
  public void testListDatabasesMultiple() {
    catalog.createDatabase(TEST_DB);
    catalog.createDatabase(TEST_DB2);

    List<String> databases = catalog.listDatabases();
    assertTrue("应包含 test_db", databases.contains(TEST_DB));
    assertTrue("应包含 test_db2", databases.contains(TEST_DB2));
  }

  // ==================== databaseExists 测试 ====================

  @Test
  public void testDatabaseExistsTrue() {
    catalog.createDatabase(TEST_DB);
    assertTrue("已创建的数据库应存在", catalog.databaseExists(TEST_DB));
  }

  @Test
  public void testDatabaseExistsFalse() {
    assertFalse("不存在的数据库应返回 false", catalog.databaseExists("non_existent_db"));
  }

  @Test
  public void testDatabaseExistsDefault() {
    assertTrue("default 数据库应存在", catalog.databaseExists("default"));
  }

  // ==================== dropDatabase 测试 ====================

  @Test
  public void testDropDatabase() {
    catalog.createDatabase(TEST_DB);
    assertTrue("数据库应存在", catalog.databaseExists(TEST_DB));

    catalog.dropDatabase(TEST_DB);
    assertFalse("删除后数据库不应存在", catalog.databaseExists(TEST_DB));
  }

  @Test
  public void testDropDatabaseNotExists() {
    assertFalse("数据库不应存在", catalog.databaseExists("non_existent_db"));

    assertThrows(
        "删除不存在的数据库应抛出 NoSuchDatabaseException",
        NoSuchDatabaseException.class,
        () -> catalog.dropDatabase("non_existent_db"));
  }

  @Test
  public void testDropNonEmptyDatabase() throws IOException {
    catalog.createDatabase(TEST_DB);

    // 在数据库中创建一张表
    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "blocking_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "blocking_table");
    catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());
    assertTrue("表应存在", catalog.tableExists(TEST_DB, "blocking_table"));

    // 删除非空数据库应抛出 DatabaseNotEmptyException
    assertThrows(
        "删除非空数据库应抛出 DatabaseNotEmptyException",
        DatabaseNotEmptyException.class,
        () -> catalog.dropDatabase(TEST_DB));

    // 验证数据库和表仍然存在
    assertTrue("数据库应仍然存在", catalog.databaseExists(TEST_DB));
    assertTrue("表应仍然存在", catalog.tableExists(TEST_DB, "blocking_table"));
  }

  @Test
  public void testDropNonEmptyDatabaseWithMultipleTables() throws IOException {
    catalog.createDatabase(TEST_DB);

    // 在数据库中创建多张表
    for (int i = 1; i <= 3; i++) {
      String loc = warehouseLocation + "/" + TEST_DB + "/" + "multi_table_" + i;
      TableIdentifier tid = TableIdentifier.of(CATALOG_NAME, TEST_DB, "multi_table_" + i);
      catalog.createTable(tid, loc, ARROW_SCHEMA, new HashMap<>());
    }
    assertEquals("应有 3 张表", 3, catalog.listTables(TEST_DB).size());

    // 删除非空数据库应抛出异常
    assertThrows(
        "删除含多张表的数据库应抛出 DatabaseNotEmptyException",
        DatabaseNotEmptyException.class,
        () -> catalog.dropDatabase(TEST_DB));
  }

  // ==================== loadDatabaseMetadata 测试 ====================

  @Test
  public void testLoadDatabaseMetadata() {
    catalog.createDatabase(TEST_DB);

    Map<String, String> metadata = catalog.loadDatabaseMetadata(TEST_DB);
    assertNotNull("数据库元数据不应为 null", metadata);
  }

  @Test
  public void testLoadDatabaseMetadataWithProperties() {
    Map<String, String> props = new HashMap<>();
    props.put("custom.key", "custom.value");
    catalog.createDatabase(TEST_DB, props);

    Map<String, String> metadata = catalog.loadDatabaseMetadata(TEST_DB);
    assertEquals("数据库元数据读取异常", "custom.value", metadata.get("custom.key"));
  }

  @Test
  public void testLoadDatabaseMetadataNotExists() {
    // 加载不存在的数据库的元数据应抛出 NoSuchDatabaseException
    assertThrows(
        "加载不存在的数据库元数据应抛出 NoSuchDatabaseException",
        NoSuchDatabaseException.class,
        () -> catalog.loadDatabaseMetadata("non_existent_db"));
  }

  // ==================== listTables 测试 ====================

  @Test
  public void testListTablesEmpty() {
    catalog.createDatabase(TEST_DB);

    List<String> tables = catalog.listTables(TEST_DB);
    assertNotNull("表列表不应为 null", tables);
    assertTrue("新数据库的表列表应为空", tables.isEmpty());
  }

  @Test
  public void testListTablesAfterCreateTable() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + TEST_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, TEST_TABLE);
    catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    List<String> tables = catalog.listTables(TEST_DB);
    assertTrue("应包含创建的表", tables.contains(TEST_TABLE));
  }

  // ==================== 在不存在的数据库中操作表 ====================

  @Test
  public void testLoadTableFromNonExistentDatabase() {
    // 在不存在的数据库中加载表应抛出 NoSuchTableException
    assertFalse("数据库不应存在", catalog.databaseExists("non_existent_db"));
    assertThrows(
        "从不存在的数据库加载表应抛出 NoSuchDatabaseException",
        NoSuchDatabaseException.class,
        () -> catalog.loadTable("non_existent_db", "some_table"));
  }

  @Test
  public void testDropTableFromNonExistentDatabase() {
    // 在不存在的数据库中删除表应抛出 NoSuchTableException
    assertFalse("数据库不应存在", catalog.databaseExists("non_existent_db"));
    assertThrows(
        "从不存在的数据库删除表应抛出 NoSuchDatabaseException",
        NoSuchDatabaseException.class,
        () -> catalog.dropTable("non_existent_db", "some_table", true));
  }

  @Test
  public void testDeregisterTableFromNonExistentDatabase() {
    // 在不存在的数据库中取消注册表应抛出 NoSuchTableException
    assertFalse("数据库不应存在", catalog.databaseExists("non_existent_db"));
    assertThrows(
        "从不存在的数据库取消注册表应抛出 NoSuchDatabaseException",
        NoSuchDatabaseException.class,
        () -> catalog.deregisterTable("non_existent_db", "some_table"));
  }

  // ==================== setDatabaseProperties / removeDatabaseProperties 不支持操作测试
  // ====================

  @Test
  public void testSetDatabasePropertiesUnsupported() {
    catalog.createDatabase(TEST_DB);
    Map<String, String> newProps = new HashMap<>();
    newProps.put("key", "value");
    // todo hiveNamespace不支持
    catalog.setDatabaseProperties(TEST_DB, newProps);
  }

  @Test
  public void testRemoveDatabasePropertiesUnsupported() {
    catalog.createDatabase(TEST_DB);

    java.util.Set<String> keysToRemove = new java.util.HashSet<>();
    keysToRemove.add("key");
    // todo hiveNamespace不支持
    catalog.removeDatabaseProperties(TEST_DB, keysToRemove);
  }

  // ==================== tableExists 测试 ====================

  @Test
  public void testTableExistsFalse() {
    catalog.createDatabase(TEST_DB);
    assertFalse("不存在的表应返回 false", catalog.tableExists(TEST_DB, "non_existent_table"));
  }

  @Test
  public void testTableExistsTrue() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + TEST_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, TEST_TABLE);
    catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    assertTrue("已创建的表应存在", catalog.tableExists(TEST_DB, TEST_TABLE));
  }

  // ==================== createTable 测试 ====================

  @Test
  public void testCreateTable() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + TEST_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, TEST_TABLE);

    LanceTable lanceTable =
        catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    assertNotNull("创建的表不应为 null", lanceTable);
    assertEquals("表标识符应匹配", tableId, lanceTable.id());
    assertNotNull("原始 Dataset 不应为 null", lanceTable.originalTable());
  }

  @Test
  public void testCreateTableWithProperties() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "prop_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "prop_table");
    Map<String, String> tableProps = new HashMap<>();
    tableProps.put("custom.key", "custom.value");

    LanceTable lanceTable = catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, tableProps);

    assertNotNull("创建的表不应为 null", lanceTable);
    assertEquals("表属性应匹配", "custom.value", lanceTable.properties().get("custom.key"));
  }

  @Test
  public void testCreateTableWithoutLocation() throws IOException {
    catalog.createDatabase(TEST_DB);

    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "no_loc_table");

    // 不传递 location（传 null），由 Hive 自动分配路径
    LanceTable lanceTable = catalog.createTable(tableId, null, ARROW_SCHEMA, new HashMap<>());
    assertNotNull("不传 location 创建的表不应为 null", lanceTable);
    assertEquals("表标识符应匹配", tableId, lanceTable.id());
    assertNotNull("原始 Dataset 不应为 null", lanceTable.originalTable());
    assertNotNull("location 应不为 null", lanceTable.properties().get(LanceProperties.TABLE_LOCATION));

    // 验证表已在 Hive 中注册且可正常加载
    assertTrue("表应存在", catalog.tableExists(TEST_DB, "no_loc_table"));
    AmoroTable<?> loadedTable = catalog.loadTable(TEST_DB, "no_loc_table");
    assertNotNull("加载的表不应为 null", loadedTable);
    assertEquals("表名应匹配", "no_loc_table", loadedTable.id().getTableName());
    assertNotNull(
        "location 应不为 null", loadedTable.properties().get(LanceProperties.TABLE_LOCATION));
  }

  @Test
  public void testCreateTableWithEmptyLocation() throws IOException {
    catalog.createDatabase(TEST_DB);

    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "empty_loc_table");

    // 传递空字符串 location，由 Hive 自动分配路径
    LanceTable lanceTable = catalog.createTable(tableId, "", ARROW_SCHEMA, new HashMap<>());

    assertNotNull("空 location 创建的表不应为 null", lanceTable);
    assertEquals("表标识符应匹配", tableId, lanceTable.id());

    // 验证表已在 Hive 中注册且可正常加载
    assertTrue("表应存在", catalog.tableExists(TEST_DB, "empty_loc_table"));
    AmoroTable<?> loadedTable = catalog.loadTable(TEST_DB, "empty_loc_table");
    assertNotNull("加载的表不应为 null", loadedTable);
  }

  @Test
  public void testCreateTableAlreadyExists() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "dup_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "dup_table");
    catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    assertTrue("表应已存在", catalog.tableExists(TEST_DB, "dup_table"));

    // 重复创建同名表应抛出 AlreadyExistsException
    String tableLocation2 = warehouseLocation + "/" + TEST_DB + "/" + "dup_table_2";
    assertThrows(
        "重复创建表应抛出 AlreadyExistsException",
        AlreadyExistsException.class,
        () -> catalog.createTable(tableId, tableLocation2, ARROW_SCHEMA, new HashMap<>()));
  }

  // ==================== declareTable 测试 ====================

  @Test
  public void testDeclareTable() {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "declared_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "declared_table");

    var response = catalog.declareTable(tableId, tableLocation, new HashMap<>());
    assertNotNull("declareTable 响应不应为 null", response);

    // 验证表已在 Hive 中注册
    assertTrue("声明的表应存在", catalog.tableExists(TEST_DB, "declared_table"));
  }

  @Test
  public void testDeclareTableWithoutLocation() {
    catalog.createDatabase(TEST_DB);

    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "auto_loc_table");

    // 不提供 location，由 Hive 自动分配
    var response = catalog.declareTable(tableId, null, new HashMap<>());
    assertNotNull("declareTable 响应不应为 null", response);
  }

  @Test
  public void testDeclareTableAlreadyExists() {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "dup_declare_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "dup_declare_table");

    // 第一次声明应成功
    catalog.declareTable(tableId, tableLocation, new HashMap<>());
    assertTrue("表应已存在", catalog.tableExists(TEST_DB, "dup_declare_table"));

    // 重复声明同名表应抛出 AlreadyExistsException
    assertThrows(
        "重复声明表应抛出 AlreadyExistsException",
        AlreadyExistsException.class,
        () -> catalog.declareTable(tableId, tableLocation, new HashMap<>()));
  }

  // ==================== loadTable 测试 ====================

  @Test
  public void testLoadTable() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + TEST_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, TEST_TABLE);
    catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    AmoroTable<?> loadedTable = catalog.loadTable(TEST_DB, TEST_TABLE);

    assertNotNull("加载的表不应为 null", loadedTable);
    assertEquals("表名应匹配", TEST_TABLE, loadedTable.id().getTableName());
    assertEquals("数据库名应匹配", TEST_DB, loadedTable.id().getDatabase());
    assertEquals("Catalog 名应匹配", CATALOG_NAME, loadedTable.id().getCatalog());
  }

  @Test
  public void testLoadTableNotExists() {
    catalog.createDatabase(TEST_DB);

    assertThrows(
        "加载不存在的表应抛出 NoSuchTableException",
        NoSuchTableException.class,
        () -> catalog.loadTable(TEST_DB, "non_existent_table"));
  }

  @Test
  public void testLoadTableAfterDeclareWithLocation() {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "declared_only_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "declared_only_table");

    // 仅通过 declareTable 注册表，不创建实际数据
    DeclareTableResponse declareResponse =
        catalog.declareTable(tableId, tableLocation, new HashMap<>());
    assertNotNull("declareTable 响应不应为 null", declareResponse);

    // 表已在 HMS 中注册
    assertTrue("声明的表应存在", catalog.tableExists(TEST_DB, "declared_only_table"));

    // loadTable 应返回空 LanceTable（因为实际数据不存在）
    AmoroTable<?> loadedTable = catalog.loadTable(TEST_DB, "declared_only_table");
    assertNotNull("加载的表不应为 null", loadedTable);
    assertEquals("表名应匹配", "declared_only_table", loadedTable.id().getTableName());
    assertEquals("数据库名应匹配", TEST_DB, loadedTable.id().getDatabase());

    // 验证空表标志
    assertEquals("应标记为空表", "true", loadedTable.properties().get(LanceProperties.TABLE_EMPTY));
    assertNotNull(
        "properties 应包含 location", loadedTable.properties().get(LanceProperties.TABLE_LOCATION));

    // 验证 originalTable 为 null（因为没有实际数据）
    LanceTable lanceTable = (LanceTable) loadedTable;
    assertNull("空表的 Dataset 应为 null", lanceTable.originalTable());
    assertNull("空表的 snapshot 应为 null", lanceTable.currentSnapshot());
  }

  @Test
  public void testLoadTableAfterDeclareWithoutLocation() {
    catalog.createDatabase(TEST_DB);

    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "declared_no_loc_table");

    // 不提供 location，由 Hive 自动分配
    DeclareTableResponse declareResponse = catalog.declareTable(tableId, null, new HashMap<>());
    assertNotNull("declareTable 响应不应为 null", declareResponse);

    // loadTable 应返回空 LanceTable
    AmoroTable<?> loadedTable = catalog.loadTable(TEST_DB, "declared_no_loc_table");
    assertNotNull("加载的表不应为 null", loadedTable);
    assertEquals("表名应匹配", "declared_no_loc_table", loadedTable.id().getTableName());

    // 验证空表标志
    assertEquals("应标记为空表", "true", loadedTable.properties().get(LanceProperties.TABLE_EMPTY));
    assertNotNull(
        "properties 应包含 location", loadedTable.properties().get(LanceProperties.TABLE_LOCATION));

    // 验证 originalTable 为 null
    LanceTable lanceTable = (LanceTable) loadedTable;
    assertNull("空表的 Dataset 应为 null", lanceTable.originalTable());
  }

  // ==================== dropTable 测试 ====================

  @Test
  public void testDropTableWithoutPurge() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + TEST_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, TEST_TABLE);
    catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    assertTrue("删除前表应存在", catalog.tableExists(TEST_DB, TEST_TABLE));

    // purge=false 仅取消注册，不删除数据
    boolean result = catalog.dropTable(TEST_DB, TEST_TABLE, false);
    assertTrue("dropTable 应返回 true", result);
    assertFalse("取消注册后表不应存在", catalog.tableExists(TEST_DB, TEST_TABLE));

    // 验证数据文件仍然存在（仅取消注册，不删除数据）
    File tableDir = new File(tableLocation);
    assertTrue("purge=false 时数据文件应仍然存在", tableDir.exists());
  }

  @Test
  public void testDropTableWithPurge() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "purge_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "purge_table");
    catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    assertTrue("删除前表应存在", catalog.tableExists(TEST_DB, "purge_table"));

    // purge=true 删除数据
    boolean result = catalog.dropTable(TEST_DB, "purge_table", true);
    assertTrue("dropTable with purge 应返回 true", result);
    assertFalse("purge 删除后表不应存在", catalog.tableExists(TEST_DB, "purge_table"));

    // 验证数据文件已被删除
    File tableDir = new File(tableLocation);
    assertFalse("purge=true 时数据文件应被删除", tableDir.exists());
  }

  @Test
  public void testDropTableNotExists() {
    catalog.createDatabase(TEST_DB);

    assertThrows(
        "删除不存在的表应抛出 NoSuchTableException（purge=true）",
        NoSuchTableException.class,
        () -> catalog.dropTable(TEST_DB, "non_existent_table", true));
  }

  @Test
  public void testDropTableNotExistsWithoutPurge() {
    catalog.createDatabase(TEST_DB);

    assertThrows(
        "删除不存在的表应抛出 NoSuchTableException（purge=false）",
        NoSuchTableException.class,
        () -> catalog.dropTable(TEST_DB, "non_existent_table", false));
  }

  // ==================== deregisterTable 测试 ====================

  @Test
  public void testDeregisterTable() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "dereg_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "dereg_table");
    catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    assertTrue("取消注册前表应存在", catalog.tableExists(TEST_DB, "dereg_table"));

    var response = catalog.deregisterTable(TEST_DB, "dereg_table");
    assertNotNull("deregisterTable 响应不应为 null", response);
    assertFalse("取消注册后表不应存在", catalog.tableExists(TEST_DB, "dereg_table"));
  }

  @Test
  public void testDeregisterTableNotExists() {
    catalog.createDatabase(TEST_DB);

    assertThrows(
        "取消注册不存在的表应抛出 NoSuchTableException",
        NoSuchTableException.class,
        () -> catalog.deregisterTable(TEST_DB, "non_existent_table"));
  }

  // ==================== getTableLocation 测试 ====================

  @Test
  public void testGetTableLocation() throws IOException {
    catalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "loc_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "loc_table");
    LanceTable createdTable =
        catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());
    assertNotNull("创建的表不应为 null", createdTable);

    String location = catalog.getTableLocation(TEST_DB, "loc_table");
    assertNotNull("location 不应为 null", location);
    assertFalse("location 不应为空", location.isEmpty());
    assertTrue("location 应包含表名或表路径片段，实际: " + location, location.contains("loc_table"));
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

    assertNotNull("getTableLocation 返回值不应为 null", directLocation);
    assertNotNull("loadTable 返回的 location 不应为 null", loadedLocation);
    assertEquals(
        "getTableLocation 应与 loadTable 得到的 location 一致", "file:" + loadedLocation, directLocation);
  }

  // ==================== extractStorageOptions 测试 ====================

  @Test
  public void testExtractStorageOptionsEmpty() {
    Map<String, String> props = new HashMap<>();
    Map<String, String> storageOptions = LanceProperties.extractCatalogStorageOptions(props);
    assertNotNull("存储选项不应为 null", storageOptions);
    assertTrue("空属性应返回空存储选项", storageOptions.isEmpty());
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
    assertEquals("应提取 4 个 COS 配置", 4, storageOptions.size());
    assertEquals("region 应匹配", "ap-guangzhou", storageOptions.get(COS_REGION));
    assertEquals("secret_id 应匹配", "test-secret-id", storageOptions.get(COS_SECRET_ID));
    assertEquals("secret_key 应匹配", "test-secret-key", storageOptions.get(COS_SECRET_KEY));
    assertEquals(
        "security_token 应匹配", "test-security-token", storageOptions.get(COS_SECURITY_TOKEN));
    assertFalse("不应包含无关配置", storageOptions.containsKey("unrelated.key"));
  }

  @Test
  public void testExtractStorageOptionsPartial() {
    Map<String, String> props = new HashMap<>();
    props.put(COS_REGION, "ap-beijing");
    // 只设置 region，不设置 secret_id 和 secret_key

    Map<String, String> storageOptions = LanceProperties.extractCatalogStorageOptions(props);
    assertEquals("应只提取 1 个配置", 1, storageOptions.size());
    assertEquals("region 应匹配", "ap-beijing", storageOptions.get(COS_REGION));
  }

  // ==================== 端到端流程测试 ====================

  @Test
  public void testEndToEndCreateAndLoadTable() throws IOException {
    // 1. 创建数据库
    catalog.createDatabase(TEST_DB);
    assertTrue("数据库应存在", catalog.databaseExists(TEST_DB));

    // 2. 创建表
    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + TEST_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, TEST_TABLE);
    LanceTable createdTable =
        catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());
    assertNotNull("创建的表不应为 null", createdTable);

    // 3. 验证表存在
    assertTrue("表应存在", catalog.tableExists(TEST_DB, TEST_TABLE));
    List<String> tables = catalog.listTables(TEST_DB);
    assertTrue("表列表应包含创建的表", tables.contains(TEST_TABLE));

    // 4. 加载表
    AmoroTable<?> loadedTable = catalog.loadTable(TEST_DB, TEST_TABLE);
    assertNotNull("加载的表不应为 null", loadedTable);
    assertEquals("表标识符应匹配", tableId, loadedTable.id());

    // 5. 删除表
    boolean dropped = catalog.dropTable(TEST_DB, TEST_TABLE, false);
    assertTrue("删除应成功", dropped);
    assertFalse("删除后表不应存在", catalog.tableExists(TEST_DB, TEST_TABLE));

    // 6. 删除数据库
    catalog.dropDatabase(TEST_DB);
    assertFalse("删除后数据库不应存在", catalog.databaseExists(TEST_DB));
  }

  @Test
  public void testEndToEndMultipleDatabasesAndTables() throws IOException {
    // 创建多个数据库
    catalog.createDatabase(TEST_DB);
    catalog.createDatabase(TEST_DB2);

    // 在不同数据库中创建表
    String table1Location = warehouseLocation + "/" + TEST_DB + "/" + "table1";
    TableIdentifier tableId1 = TableIdentifier.of(CATALOG_NAME, TEST_DB, "table1");
    catalog.createTable(tableId1, table1Location, ARROW_SCHEMA, new HashMap<>());

    String table2Location = warehouseLocation + "/" + TEST_DB + "/" + "table2";
    TableIdentifier tableId2 = TableIdentifier.of(CATALOG_NAME, TEST_DB, "table2");
    catalog.createTable(tableId2, table2Location, ARROW_SCHEMA, new HashMap<>());

    String table3Location = warehouseLocation + "/" + TEST_DB2 + "/" + "table3";
    TableIdentifier tableId3 = TableIdentifier.of(CATALOG_NAME, TEST_DB2, "table3");
    catalog.createTable(tableId3, table3Location, ARROW_SCHEMA, new HashMap<>());

    // 验证各数据库的表列表
    List<String> db1Tables = catalog.listTables(TEST_DB);
    assertEquals("test_db 应有 2 张表", 2, db1Tables.size());
    assertTrue("应包含 table1", db1Tables.contains("table1"));
    assertTrue("应包含 table2", db1Tables.contains("table2"));

    List<String> db2Tables = catalog.listTables(TEST_DB2);
    assertEquals("test_db2 应有 1 张表", 1, db2Tables.size());
    assertTrue("应包含 table3", db2Tables.contains("table3"));

    // 验证数据库列表
    List<String> databases = catalog.listDatabases();
    assertTrue("应包含 test_db", databases.contains(TEST_DB));
    assertTrue("应包含 test_db2", databases.contains(TEST_DB2));
  }

  // ==================== 数据写入和读取测试 ====================

  @Test
  public void testCreateTableWriteDataAndRead() throws IOException {
    catalog.createDatabase(TEST_DB);

    // 1. 创建默认表
    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "rw_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "rw_table");
    LanceTable createdTable =
        catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());
    assertNotNull("创建的表不应为 null", createdTable);

    // 2. 通过 Dataset API 写入模拟数据
    try (VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, allocator)) {
      BigIntVector idVector = (BigIntVector) root.getVector("id");
      VarCharVector nameVector = (VarCharVector) root.getVector("name");
      IntVector ageVector = (IntVector) root.getVector("age");

      // 写入 3 行数据
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

      // 使用 APPEND 模式写入数据到已有的 Dataset
      Dataset appendedDataset =
          Dataset.write()
              .allocator(allocator)
              .uri(tableLocation)
              .reader(vectorSchemaRootToArrowReader(root, allocator))
              .mode(WriteParams.WriteMode.APPEND)
              .execute();
      assertNotNull("追加写入后的 Dataset 不应为 null", appendedDataset);
      appendedDataset.close();
    }

    // 3. 重新打开 Dataset 并读取验证
    Dataset readDataset = Dataset.open(tableLocation, allocator);
    assertEquals("应有 3 行数据", 3, readDataset.countRows());

    // 通过 Scanner 读取全部数据
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

      assertEquals("应读取 3 行 id", 3, ids.size());
      assertTrue("应包含 id=1", ids.contains(1L));
      assertTrue("应包含 id=2", ids.contains(2L));
      assertTrue("应包含 id=3", ids.contains(3L));

      assertTrue("应包含 Alice", names.contains("Alice"));
      assertTrue("应包含 Bob", names.contains("Bob"));
      assertTrue("应包含 Charlie", names.contains("Charlie"));

      assertTrue("应包含 age=30", ages.contains(30));
      assertTrue("应包含 age=25", ages.contains(25));
      assertTrue("应包含 age=35", ages.contains(35));
    }

    readDataset.close();
  }

  // ==================== DATA_STORAGE_VERSION 测试 ====================

  @Test
  public void testCreateTableWithDataStorageVersion22WriteAndRead() throws IOException {
    catalog.createDatabase(TEST_DB);

    // 1. 创建 DATA_STORAGE_VERSION 为 2.2 的 Lance 表
    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "v22_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "v22_table");
    Map<String, String> tableProps = new HashMap<>();
    tableProps.put(LanceProperties.DATA_STORAGE_VERSION, "2.2");

    LanceTable createdTable = catalog.createTable(tableId, tableLocation, ARROW_SCHEMA, tableProps);
    assertNotNull("创建的 v2.2 表不应为 null", createdTable);

    // 2. 通过 Dataset API 写入模拟数据
    try (VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, allocator)) {
      BigIntVector idVector = (BigIntVector) root.getVector("id");
      VarCharVector nameVector = (VarCharVector) root.getVector("name");
      IntVector ageVector = (IntVector) root.getVector("age");

      // 写入 3 行数据
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

      // 使用 APPEND 模式写入数据到已有的 Dataset
      Dataset appendedDataset =
          Dataset.write()
              .allocator(allocator)
              .uri(tableLocation)
              .reader(vectorSchemaRootToArrowReader(root, allocator))
              .mode(WriteParams.WriteMode.APPEND)
              .execute();
      assertNotNull("追加写入后的 Dataset 不应为 null", appendedDataset);
      appendedDataset.close();
    }

    // 3. 重新打开 Dataset 并读取验证数据
    Dataset readDataset = Dataset.open(tableLocation, allocator);
    assertEquals("应有 3 行数据", 3, readDataset.countRows());

    // 通过 Scanner 读取全部数据
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

      assertEquals("应读取 3 行 id", 3, ids.size());
      assertTrue("应包含 id=100", ids.contains(100L));
      assertTrue("应包含 id=200", ids.contains(200L));
      assertTrue("应包含 id=300", ids.contains(300L));

      assertTrue("应包含 David", names.contains("David"));
      assertTrue("应包含 Eve", names.contains("Eve"));
      assertTrue("应包含 Frank", names.contains("Frank"));

      assertTrue("应包含 age=28", ages.contains(28));
      assertTrue("应包含 age=32", ages.contains(32));
      assertTrue("应包含 age=40", ages.contains(40));
    }

    readDataset.close();

    // 4. 通过 loadTable 加载表并验证 DATA_STORAGE_VERSION 为 2.2
    AmoroTable<?> loadedTable = catalog.loadTable(TEST_DB, "v22_table");
    assertNotNull("加载的表不应为 null", loadedTable);
    assertEquals("表名应匹配", "v22_table", loadedTable.id().getTableName());
    assertEquals("数据库名应匹配", TEST_DB, loadedTable.id().getDatabase());

    // 验证 properties 中 lance.data-storage-version 为 2.2
    String dataStorageVersion = loadedTable.properties().get(LanceProperties.DATA_STORAGE_VERSION);
    assertEquals("DATA_STORAGE_VERSION 应为 2.2", "2.2", dataStorageVersion);
  }

  // ==================== 辅助方法 ====================

  /**
   * 将 VectorSchemaRoot 转换为 ArrowReader。
   *
   * <p>通过 Arrow IPC 流式协议，先将 VectorSchemaRoot 序列化到内存字节数组， 再从字节数组反序列化为 ArrowStreamReader。
   *
   * @param root 要转换的 VectorSchemaRoot
   * @param alloc 用于分配内存的 BufferAllocator
   * @return 包含 root 数据的 ArrowReader
   * @throws IOException 如果序列化/反序列化过程中发生 IO 错误
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
