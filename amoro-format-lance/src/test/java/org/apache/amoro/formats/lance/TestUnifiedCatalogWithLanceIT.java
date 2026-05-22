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
 * CommonUnifiedCatalog 集成测试类，配置同时支持 ICEBERG 和 LANCE 两种表格式。
 *
 * <p>本测试类通过 {@link TestHMS}（HMSMockServer）创建真实的 Hive Metastore 环境， 使用 {@link CommonUnifiedCatalog}
 * 统一管理 Iceberg 和 Lance 两种格式的表。
 *
 * <p>测试内容：
 *
 * <ul>
 *   <li>Lance 表的 schema 创建和加载
 *   <li>Iceberg 表的创建和加载
 *   <li>通过 CommonUnifiedCatalog 统一列出不同格式的表
 *   <li>验证 loadTable 能正确识别表格式
 * </ul>
 */
public class TestUnifiedCatalogWithLanceIT {

  private static final Logger LOG = LoggerFactory.getLogger(TestUnifiedCatalogWithLanceIT.class);

  private static final String CATALOG_NAME = "hive";
  private static final String TEST_DB = "unified_test_db";
  private static final String LANCE_TABLE = "lance_table";
  private static final String ICEBERG_TABLE = "iceberg_table";

  /** Arrow Schema 定义，用于创建 Lance 表 */
  private static final Schema ARROW_SCHEMA =
      new Schema(
          Arrays.asList(
              Field.nullable("id", new ArrowType.Int(64, true)),
              Field.nullable("name", ArrowType.Utf8.INSTANCE),
              Field.nullable("age", new ArrowType.Int(32, true))));

  /** Iceberg Schema 定义，用于创建 Iceberg 表 */
  private static final org.apache.iceberg.Schema ICEBERG_SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.optional(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "age", Types.IntegerType.get()));

  /** 使用 HMSMockServer 模拟 Hive Metastore */
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

    // 构建 catalog 属性，同时支持 ICEBERG 和 LANCE
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

    // 构建 TableMetaStore（包含 HiveConf）
    TableMetaStore tableMetaStore =
        TableMetaStore.builder().withConfiguration(TEST_HMS.getHiveConf()).build();

    // 创建 CommonUnifiedCatalog
    unifiedCatalog =
        new OptimizedCommonUnifiedCatalog(
            CATALOG_NAME,
            CatalogMetaProperties.CATALOG_TYPE_HIVE,
            catalogProperties,
            tableMetaStore);

    // 同时创建一个独立的 LanceHiveCatalog 用于直接创建 Lance 表，与 unifiedCatalog 共享 HiveConf
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
    // 清理测试创建的数据库和表
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
              LOG.warn("清理表 {}.{} 失败: {}", db, tableId.table(), e.getMessage());
            }
          }
          unifiedCatalog.dropDatabase(db);
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

  // ==================== 基础功能测试 ====================

  @Test
  public void testUnifiedCatalogName() {
    assertEquals("Catalog 名称应匹配", CATALOG_NAME, unifiedCatalog.name());
  }

  @Test
  public void testUnifiedCatalogMetastoreType() {
    assertEquals(
        "Metastore 类型应为 hive",
        CatalogMetaProperties.CATALOG_TYPE_HIVE,
        unifiedCatalog.metastoreType());
  }

  @Test
  public void testCreateAndListDatabases() {
    unifiedCatalog.createDatabase(TEST_DB);

    List<String> databases = unifiedCatalog.listDatabases();
    assertTrue("应包含创建的数据库", databases.contains(TEST_DB));
    assertTrue("应包含 default 数据库", databases.contains("default"));
  }

  @Test
  public void testDatabaseExists() {
    unifiedCatalog.createDatabase(TEST_DB);
    assertTrue("已创建的数据库应存在", unifiedCatalog.databaseExists(TEST_DB));
    assertFalse("不存在的数据库应返回 false", unifiedCatalog.databaseExists("non_existent_db"));
  }

  // ==================== createDatabase 测试 ====================

  @Test
  public void testCreateDatabaseWithMetadata() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("owner", "test_user");
    metadata.put("description", "test database with metadata");
    unifiedCatalog.createDatabase(TEST_DB, metadata);

    assertTrue("带 metadata 创建的数据库应存在", unifiedCatalog.databaseExists(TEST_DB));
    List<String> databases = unifiedCatalog.listDatabases();
    assertTrue("listDatabases 应包含带 metadata 创建的数据库", databases.contains(TEST_DB));
  }

  @Test
  public void testCreateDuplicateDatabase() {
    unifiedCatalog.createDatabase(TEST_DB);
    assertTrue("第一次创建数据库应成功", unifiedCatalog.databaseExists(TEST_DB));

    // 重复创建同名数据库应抛出异常
    org.junit.Assert.assertThrows(
        "重复创建数据库应抛出异常", Exception.class, () -> unifiedCatalog.createDatabase(TEST_DB));
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
    assertTrue("应包含 db1", databases.contains(db1));
    assertTrue("应包含 db2", databases.contains(db2));
    assertTrue("应包含 db3", databases.contains(db3));
  }

  // ==================== dropDatabase 测试 ====================

  @Test
  public void testDropDatabase() {
    unifiedCatalog.createDatabase(TEST_DB);
    assertTrue("删除前数据库应存在", unifiedCatalog.databaseExists(TEST_DB));

    unifiedCatalog.dropDatabase(TEST_DB);
    assertFalse("删除后数据库不应存在", unifiedCatalog.databaseExists(TEST_DB));
  }

  @Test
  public void testDropAndRecreateDatabase() {
    // 创建数据库
    unifiedCatalog.createDatabase(TEST_DB);
    assertTrue("创建后数据库应存在", unifiedCatalog.databaseExists(TEST_DB));

    // 删除数据库
    unifiedCatalog.dropDatabase(TEST_DB);
    assertFalse("删除后数据库不应存在", unifiedCatalog.databaseExists(TEST_DB));

    // 重新创建同名数据库
    unifiedCatalog.createDatabase(TEST_DB);
    assertTrue("重新创建后数据库应存在", unifiedCatalog.databaseExists(TEST_DB));
  }

  // ==================== listDatabases 测试 ====================

  @Test
  public void testListDatabasesContainsDefault() {
    List<String> databases = unifiedCatalog.listDatabases();
    assertNotNull("listDatabases 不应返回 null", databases);
    assertTrue("应包含 default 数据库", databases.contains("default"));
  }

  @Test
  public void testListDatabasesAfterCreateAndDrop() {
    // 初始状态
    List<String> initialDatabases = unifiedCatalog.listDatabases();
    int initialSize = initialDatabases.size();

    // 创建数据库后列表应增加
    unifiedCatalog.createDatabase(TEST_DB);
    List<String> afterCreate = unifiedCatalog.listDatabases();
    assertEquals("创建数据库后列表大小应增加 1", initialSize + 1, afterCreate.size());
    assertTrue("列表应包含新创建的数据库", afterCreate.contains(TEST_DB));

    // 删除数据库后列表应恢复
    unifiedCatalog.dropDatabase(TEST_DB);
    List<String> afterDrop = unifiedCatalog.listDatabases();
    assertEquals("删除数据库后列表大小应恢复", initialSize, afterDrop.size());
    assertFalse("列表不应包含已删除的数据库", afterDrop.contains(TEST_DB));
  }

  // ==================== databaseExists 测试 ====================

  @Test
  public void testDatabaseExistsForDefault() {
    assertTrue("default 数据库应始终存在", unifiedCatalog.databaseExists("default"));
  }

  @Test
  public void testDatabaseExistsAfterDrop() {
    unifiedCatalog.createDatabase(TEST_DB);
    assertTrue("创建后 databaseExists 应返回 true", unifiedCatalog.databaseExists(TEST_DB));

    unifiedCatalog.dropDatabase(TEST_DB);
    assertFalse("删除后 databaseExists 应返回 false", unifiedCatalog.databaseExists(TEST_DB));
  }

  @Test
  public void testDatabaseExistsWithEmptyName() {
    assertFalse("空字符串数据库名应返回 false", unifiedCatalog.databaseExists(""));
  }

  // ==================== Lance 表创建和加载测试 ====================

  @Test
  public void testCreateAndLoadLanceTable() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    // 通过 LanceHiveCatalog 创建 Lance 表
    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + LANCE_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, LANCE_TABLE);
    LanceTable createdTable =
        lanceCatalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());
    assertNotNull("Lance 表创建不应为 null", createdTable);

    // 通过 CommonUnifiedCatalog 加载 Lance 表
    AmoroTable<?> loadedTable = unifiedCatalog.loadTable(TEST_DB, LANCE_TABLE);
    assertNotNull("通过 UnifiedCatalog 加载的 Lance 表不应为 null", loadedTable);
    assertEquals("表格式应为 LANCE", TableFormat.LANCE, loadedTable.format());
    assertEquals("表名应匹配", LANCE_TABLE, loadedTable.id().getTableName());
    assertEquals("数据库名应匹配", TEST_DB, loadedTable.id().getDatabase());
  }

  @Test
  public void testCreateLanceTableWithSchema() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    // 创建带有多种字段类型的 Lance 表
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
    assertNotNull("复杂 schema 的 Lance 表创建不应为 null", createdTable);
    assertNotNull("原始 Dataset 不应为 null", createdTable.originalTable());

    // 通过 CommonUnifiedCatalog 加载并验证
    AmoroTable<?> loadedTable = unifiedCatalog.loadTable(TEST_DB, "complex_lance_table");
    assertNotNull("通过 UnifiedCatalog 加载的表不应为 null", loadedTable);
    assertEquals("表格式应为 LANCE", TableFormat.LANCE, loadedTable.format());
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
    assertNotNull("带属性的 Lance 表创建不应为 null", createdTable);

    // 通过 CommonUnifiedCatalog 加载并验证
    AmoroTable<?> loadedTable = unifiedCatalog.loadTable(TEST_DB, "prop_lance_table");
    assertNotNull("通过 UnifiedCatalog 加载的表不应为 null", loadedTable);
    assertEquals("表格式应为 LANCE", TableFormat.LANCE, loadedTable.format());
  }

  @Test
  public void testLoadTableFormat() throws Exception {
    unifiedCatalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + LANCE_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, LANCE_TABLE);
    Map<String, String> tableProps = new HashMap<>();
    lanceCatalog.createTable(tableId, tableLocation, ARROW_SCHEMA, tableProps);

    TableFormat format = unifiedCatalog.loadTableFormat(TEST_DB, LANCE_TABLE);
    assertEquals("应识别为 LANCE 格式", TableFormat.LANCE, format);
  }

  @Test
  public void testCreateLanceTableWithoutLocation() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    // 不传递 location，由 Hive 自动分配
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "auto_loc_lance_table");
    LanceTable createdTable =
        lanceCatalog.createTable(tableId, null, ARROW_SCHEMA, new HashMap<>());
    assertNotNull("不传 location 创建的 Lance 表不应为 null", createdTable);

    // 通过 CommonUnifiedCatalog 加载
    AmoroTable<?> loadedTable = unifiedCatalog.loadTable(TEST_DB, "auto_loc_lance_table");
    assertNotNull("通过 UnifiedCatalog 加载的表不应为 null", loadedTable);
    assertEquals("表格式应为 LANCE", TableFormat.LANCE, loadedTable.format());
  }

  // ==================== Iceberg 表创建和加载测试 ====================

  @Test
  public void testCreateAndLoadIcebergTable() {
    unifiedCatalog.createDatabase(TEST_DB);

    // 通过 Iceberg Catalog 创建 Iceberg 表
    createIcebergTable(TEST_DB, ICEBERG_TABLE);

    // 通过 CommonUnifiedCatalog 加载 Iceberg 表
    AmoroTable<?> loadedTable = unifiedCatalog.loadTable(TEST_DB, ICEBERG_TABLE);
    assertNotNull("通过 UnifiedCatalog 加载的 Iceberg 表不应为 null", loadedTable);
    assertEquals("表格式应为 ICEBERG", TableFormat.ICEBERG, loadedTable.format());
    assertEquals("表名应匹配", ICEBERG_TABLE, loadedTable.id().getTableName());
    assertEquals("数据库名应匹配", TEST_DB, loadedTable.id().getDatabase());
  }

  // ==================== 混合格式表列表测试 ====================

  @Test
  public void testListTablesWithMixedFormats() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    // 创建 Lance 表
    String lanceLocation = warehouseLocation + "/" + TEST_DB + "/" + LANCE_TABLE;
    TableIdentifier lanceTableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, LANCE_TABLE);
    lanceCatalog.createTable(lanceTableId, lanceLocation, ARROW_SCHEMA, new HashMap<>());

    // 创建 Iceberg 表
    createIcebergTable(TEST_DB, ICEBERG_TABLE);

    // 通过 CommonUnifiedCatalog 列出所有表
    List<TableIDWithFormat> tables = unifiedCatalog.listTables(TEST_DB);
    assertNotNull("表列表不应为 null", tables);

    // 验证包含两种格式的表
    Map<String, TableFormat> tableFormatMap =
        tables.stream()
            .collect(Collectors.toMap(TableIDWithFormat::table, TableIDWithFormat::getTableFormat));

    assertTrue("应包含 Lance 表", tableFormatMap.containsKey(LANCE_TABLE));
    assertTrue("应包含 Iceberg 表", tableFormatMap.containsKey(ICEBERG_TABLE));
    assertEquals("Lance 表格式应正确", TableFormat.LANCE, tableFormatMap.get(LANCE_TABLE));
    assertEquals("Iceberg 表格式应正确", TableFormat.ICEBERG, tableFormatMap.get(ICEBERG_TABLE));
  }

  @Test
  public void testTableExistsWithMixedFormats() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    // 创建 Lance 表
    String lanceLocation = warehouseLocation + "/" + TEST_DB + "/" + LANCE_TABLE;
    TableIdentifier lanceTableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, LANCE_TABLE);
    lanceCatalog.createTable(lanceTableId, lanceLocation, ARROW_SCHEMA, new HashMap<>());

    // 创建 Iceberg 表
    createIcebergTable(TEST_DB, ICEBERG_TABLE);

    // 验证两种格式的表都存在
    assertTrue("Lance 表应存在", unifiedCatalog.tableExists(TEST_DB, LANCE_TABLE));
    assertTrue("Iceberg 表应存在", unifiedCatalog.tableExists(TEST_DB, ICEBERG_TABLE));
    assertFalse("不存在的表应返回 false", unifiedCatalog.tableExists(TEST_DB, "non_existent_table"));
  }

  // ==================== 表格式识别测试 ====================

  @Test
  public void testLoadTableFormatForLance() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + LANCE_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, LANCE_TABLE);
    lanceCatalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    // 验证 loadTableFormat 能正确识别 Lance 格式
    TableFormat format = unifiedCatalog.loadTableFormat(TEST_DB, LANCE_TABLE);
    assertEquals("应识别为 LANCE 格式", TableFormat.LANCE, format);
  }

  @Test
  public void testLoadTableFormatForIceberg() {
    unifiedCatalog.createDatabase(TEST_DB);
    createIcebergTable(TEST_DB, ICEBERG_TABLE);

    // 验证 loadTableFormat 能正确识别 Iceberg 格式
    TableFormat format = unifiedCatalog.loadTableFormat(TEST_DB, ICEBERG_TABLE);
    assertEquals("应识别为 ICEBERG 格式", TableFormat.ICEBERG, format);
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
    assertEquals("应仍识别为 ICEBERG 格式", TableFormat.ICEBERG, format);
  }

  // ==================== 异常测试 ====================

  @Test
  public void testLoadNonExistentTable() {
    unifiedCatalog.createDatabase(TEST_DB);

    assertThrows(
        "加载不存在的表应抛出 NoSuchTableException",
        NoSuchTableException.class,
        () -> unifiedCatalog.loadTable(TEST_DB, "non_existent_table"));
  }

  @Test
  public void testLoadTableFromNonExistentDatabase() {
    // 在不存在的数据库中加载表应抛出异常
    assertFalse("数据库不应存在", unifiedCatalog.databaseExists("non_existent_db"));
    assertThrows(
        "从不存在的数据库加载表应抛出 NoSuchDatabaseException",
        NoSuchDatabaseException.class,
        () -> unifiedCatalog.loadTable("non_existent_db", "some_table"));
  }

  @Test
  public void testDropNonExistentDatabase() {
    assertFalse("数据库不应存在", unifiedCatalog.databaseExists("non_existent_db"));
    // iceberg catalog 删除不抛异常， lance catalog 抛异常 UnifiedCatalog不一定会选中lance
    unifiedCatalog.dropDatabase("non_existent_db");
  }

  @Test
  public void testDropNonEmptyDatabaseWithLanceTable() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    // 在数据库中创建一张 Lance 表
    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "blocking_lance_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "blocking_lance_table");
    lanceCatalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());
    assertTrue("Lance 表应存在", unifiedCatalog.tableExists(TEST_DB, "blocking_lance_table"));

    // 删除非空数据库，iceberg 抛出 NamespaceNotEmptyException lance 应抛出 DatabaseNotEmptyException
    assertThrows(
        "删除含 Lance 表的非空数据库应抛出 NamespaceNotEmptyException 或 DatabaseNotEmptyException",
        Exception.class,
        () -> unifiedCatalog.dropDatabase(TEST_DB));

    // 验证数据库和表仍然存在
    assertTrue("数据库应仍然存在", unifiedCatalog.databaseExists(TEST_DB));
    assertTrue("Lance 表应仍然存在", unifiedCatalog.tableExists(TEST_DB, "blocking_lance_table"));
  }

  @Test
  public void testDropNonEmptyDatabaseWithIcebergTable() {
    unifiedCatalog.createDatabase(TEST_DB);

    // 在数据库中创建一张 Iceberg 表
    createIcebergTable(TEST_DB, "blocking_iceberg_table");
    assertTrue("Iceberg 表应存在", unifiedCatalog.tableExists(TEST_DB, "blocking_iceberg_table"));

    // 删除非空数据库应抛出 DatabaseNotEmptyException
    assertThrows(
        "删除含 Lance 表的非空数据库应抛出 NamespaceNotEmptyException 或 DatabaseNotEmptyException",
        Exception.class,
        () -> unifiedCatalog.dropDatabase(TEST_DB));
  }

  @Test
  public void testDropNonEmptyDatabaseWithMixedFormatTables() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    // 同时创建 Lance 表和 Iceberg 表
    String lanceLocation = warehouseLocation + "/" + TEST_DB + "/" + LANCE_TABLE;
    TableIdentifier lanceTableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, LANCE_TABLE);
    lanceCatalog.createTable(lanceTableId, lanceLocation, ARROW_SCHEMA, new HashMap<>());
    createIcebergTable(TEST_DB, ICEBERG_TABLE);

    // 删除含多种格式表的非空数据库应抛出异常
    // 虽然是公用一个hive metastore，iceberg 会抛出 NamespaceNotEmptyException，lance 会抛出
    // DatabaseNotEmptyException
    assertThrows(
        "删除含混合格式表的非空数据库应抛出 NamespaceNotEmptyException | DatabaseNotEmptyException",
        Exception.class,
        () -> unifiedCatalog.dropDatabase(TEST_DB));

    // 验证所有表仍然存在
    assertTrue("Lance 表应仍然存在", unifiedCatalog.tableExists(TEST_DB, LANCE_TABLE));
    assertTrue("Iceberg 表应仍然存在", unifiedCatalog.tableExists(TEST_DB, ICEBERG_TABLE));
  }

  @Test
  public void testDropNonExistentTable() {
    unifiedCatalog.createDatabase(TEST_DB);

    assertFalse("删除不存在的表应返回 false", unifiedCatalog.dropTable(TEST_DB, "non_existent_table", true));
  }

  @Test
  public void testLoadTableFormatFromNonExistentDatabase() {
    assertFalse("数据库不应存在", unifiedCatalog.databaseExists("non_existent_db"));
    assertThrows(
        "从不存在的数据库加载表格式应抛出异常",
        Exception.class,
        () -> unifiedCatalog.loadTableFormat("non_existent_db", "some_table"));
  }

  // ==================== 删除表测试 ====================

  @Test
  public void testDropLanceTable() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + LANCE_TABLE;
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, LANCE_TABLE);
    lanceCatalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());

    assertTrue("删除前 Lance 表应存在", unifiedCatalog.tableExists(TEST_DB, LANCE_TABLE));

    boolean result = unifiedCatalog.dropTable(TEST_DB, LANCE_TABLE, true);
    assertTrue("dropTable 应返回 true", result);
    assertFalse("删除后 Lance 表不应存在", unifiedCatalog.tableExists(TEST_DB, LANCE_TABLE));
  }

  @Test
  public void testDropIcebergTable() {
    unifiedCatalog.createDatabase(TEST_DB);
    createIcebergTable(TEST_DB, ICEBERG_TABLE);

    assertTrue("删除前 Iceberg 表应存在", unifiedCatalog.tableExists(TEST_DB, ICEBERG_TABLE));

    boolean result = unifiedCatalog.dropTable(TEST_DB, ICEBERG_TABLE, true);
    assertTrue("dropTable 应返回 true", result);
    assertFalse("删除后 Iceberg 表不应存在", unifiedCatalog.tableExists(TEST_DB, ICEBERG_TABLE));
  }

  // ==================== 端到端流程测试 ====================

  @Test
  public void testEndToEndMixedFormatWorkflow() throws IOException {
    // 1. 创建数据库
    unifiedCatalog.createDatabase(TEST_DB);
    assertTrue("数据库应存在", unifiedCatalog.databaseExists(TEST_DB));

    // 2. 创建 Lance 表
    String lanceLocation = warehouseLocation + "/" + TEST_DB + "/" + LANCE_TABLE;
    TableIdentifier lanceTableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, LANCE_TABLE);
    lanceCatalog.createTable(lanceTableId, lanceLocation, ARROW_SCHEMA, new HashMap<>());

    // 3. 创建 Iceberg 表
    createIcebergTable(TEST_DB, ICEBERG_TABLE);

    // 4. 通过 UnifiedCatalog 列出所有表
    List<TableIDWithFormat> tables = unifiedCatalog.listTables(TEST_DB);
    assertEquals("应有 2 张表", 2, tables.size());

    // 5. 分别加载并验证格式
    AmoroTable<?> lanceTable = unifiedCatalog.loadTable(TEST_DB, LANCE_TABLE);
    assertEquals("Lance 表格式应正确", TableFormat.LANCE, lanceTable.format());

    AmoroTable<?> icebergTable = unifiedCatalog.loadTable(TEST_DB, ICEBERG_TABLE);
    assertEquals("Iceberg 表格式应正确", TableFormat.ICEBERG, icebergTable.format());

    // 6. 删除 Lance 表
    unifiedCatalog.dropTable(TEST_DB, LANCE_TABLE, true);
    assertFalse("Lance 表应已删除", unifiedCatalog.tableExists(TEST_DB, LANCE_TABLE));
    assertTrue("Iceberg 表应仍存在", unifiedCatalog.tableExists(TEST_DB, ICEBERG_TABLE));

    // 7. 删除 Iceberg 表
    unifiedCatalog.dropTable(TEST_DB, ICEBERG_TABLE, true);
    assertFalse("Iceberg 表应已删除", unifiedCatalog.tableExists(TEST_DB, ICEBERG_TABLE));

    // 8. 删除数据库
    unifiedCatalog.dropDatabase(TEST_DB);
    assertFalse("数据库应已删除", unifiedCatalog.databaseExists(TEST_DB));
  }

  // ==================== 辅助方法 ====================

  /**
   * 通过 Iceberg 原生 Catalog 创建 Iceberg 表。
   *
   * @param database 数据库名
   * @param table 表名
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

  // ==================== 数据写入和读取测试 ====================

  @Test
  public void testCreateLanceTableWriteDataAndRead() throws IOException {
    unifiedCatalog.createDatabase(TEST_DB);

    // 1. 创建默认 Lance 表
    String tableLocation = warehouseLocation + "/" + TEST_DB + "/" + "rw_lance_table";
    TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, TEST_DB, "rw_lance_table");
    LanceTable createdTable =
        lanceCatalog.createTable(tableId, tableLocation, ARROW_SCHEMA, new HashMap<>());
    assertNotNull("创建的 Lance 表不应为 null", createdTable);

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

    // 4. 通过 UnifiedCatalog 加载并验证
    AmoroTable<?> loadedTable = unifiedCatalog.loadTable(TEST_DB, "rw_lance_table");
    assertNotNull("通过 UnifiedCatalog 加载的表不应为 null", loadedTable);
    assertEquals("表格式应为 LANCE", TableFormat.LANCE, loadedTable.format());
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
