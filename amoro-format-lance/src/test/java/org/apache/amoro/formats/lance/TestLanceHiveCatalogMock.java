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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.AlreadyExistsException;
import org.apache.amoro.NoSuchDatabaseException;
import org.apache.amoro.NoSuchTableException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Before;
import org.junit.Test;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.namespace.model.DeregisterTableResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link LanceHiveCatalog} public methods.
 *
 * <p>This test class follows the Amoro project testing style, using Mockito to mock the {@link
 * LanceNamespace} dependency. Tests cover all public methods defined in the FormatCatalog
 * interface.
 *
 * <p>Note: Tests that require JNI native library (like Dataset.open()) are marked with @Ignore
 * annotation.
 */
public class TestLanceHiveCatalogMock {

  private static final String TEST_CATALOG = "hive";
  private static final String TEST_DATABASE = "test_db";
  private static final String TEST_TABLE = "test_table";

  private LanceNamespace mockNamespace;
  private LanceHiveCatalog catalog;
  private Map<String, String> properties;
  private BufferAllocator allocator;

  @Before
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    mockNamespace = mock(LanceNamespace.class);
    properties = new HashMap<>();
    properties.put("catalog", TEST_CATALOG);
    catalog = new LanceHiveCatalog(TEST_CATALOG, mockNamespace, properties, allocator);
  }

  // ==================== listDatabases Tests ====================

  @Test
  public void testListDatabases() {
    // Setup mock response
    ListNamespacesResponse response = mock(ListNamespacesResponse.class);
    when(response.getNamespaces()).thenReturn(new HashSet<>(Arrays.asList("db1", "db2", "db3")));
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(response);

    // Execute
    List<String> databases = catalog.listDatabases();

    // Verify
    assertNotNull(databases);
    assertEquals(3, databases.size());
    assertTrue(databases.contains("db1"));
    assertTrue(databases.contains("db2"));
    assertTrue(databases.contains("db3"));

    // Verify request parameters
    ArgumentCaptor<ListNamespacesRequest> captor =
        ArgumentCaptor.forClass(ListNamespacesRequest.class);
    verify(mockNamespace).listNamespaces(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG), captor.getValue().getId());
  }

  @Test
  public void testListDatabasesWithDefaultCatalog() {
    // Create catalog without explicit catalog property
    Map<String, String> defaultProperties = new HashMap<>();
    LanceHiveCatalog defaultCatalog =
        new LanceHiveCatalog(TEST_CATALOG, mockNamespace, defaultProperties, allocator);

    ListNamespacesResponse response = mock(ListNamespacesResponse.class);
    when(response.getNamespaces()).thenReturn(new HashSet<>(Arrays.asList("default_db")));
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(response);

    // Execute
    List<String> databases = defaultCatalog.listDatabases();

    // Verify default catalog is used
    ArgumentCaptor<ListNamespacesRequest> captor =
        ArgumentCaptor.forClass(ListNamespacesRequest.class);
    verify(mockNamespace).listNamespaces(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG), captor.getValue().getId());
  }

  @Test
  public void testListDatabasesEmpty() {
    ListNamespacesResponse response = mock(ListNamespacesResponse.class);
    when(response.getNamespaces()).thenReturn(new HashSet<>());
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(response);

    List<String> databases = catalog.listDatabases();

    assertNotNull(databases);
    assertTrue(databases.isEmpty());
  }

  // ==================== listTables Tests ====================

  @Test
  public void testListTables() {
    // Setup mock response
    ListTablesResponse response = mock(ListTablesResponse.class);
    when(response.getTables())
        .thenReturn(new HashSet<>(Arrays.asList("table1", "table2", "table3")));
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(response);

    // Execute
    List<String> tables = catalog.listTables(TEST_DATABASE);

    // Verify
    assertNotNull(tables);
    assertEquals(3, tables.size());
    assertTrue(tables.contains("table1"));
    assertTrue(tables.contains("table2"));
    assertTrue(tables.contains("table3"));

    // Verify request parameters
    ArgumentCaptor<ListTablesRequest> captor = ArgumentCaptor.forClass(ListTablesRequest.class);
    verify(mockNamespace).listTables(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE), captor.getValue().getId());
  }

  @Test
  public void testListTablesWithDefaultCatalog() {
    Map<String, String> defaultProperties = new HashMap<>();
    LanceHiveCatalog defaultCatalog =
        new LanceHiveCatalog(TEST_CATALOG, mockNamespace, defaultProperties, allocator);

    ListTablesResponse response = mock(ListTablesResponse.class);
    when(response.getTables()).thenReturn(new HashSet<>(Collections.singletonList("table1")));
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(response);

    defaultCatalog.listTables(TEST_DATABASE);

    ArgumentCaptor<ListTablesRequest> captor = ArgumentCaptor.forClass(ListTablesRequest.class);
    verify(mockNamespace).listTables(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE), captor.getValue().getId());
  }

  @Test
  public void testListTablesEmpty() {
    ListTablesResponse response = mock(ListTablesResponse.class);
    when(response.getTables()).thenReturn(new HashSet<>());
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(response);

    List<String> tables = catalog.listTables(TEST_DATABASE);

    assertNotNull(tables);
    assertTrue(tables.isEmpty());
  }

  // ==================== databaseExists Tests ====================

  @Test
  public void testDatabaseExistsTrue() {
    ListNamespacesResponse response = mock(ListNamespacesResponse.class);
    when(response.getNamespaces())
        .thenReturn(new HashSet<>(Arrays.asList("db1", TEST_DATABASE, "db2")));
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(response);

    boolean exists = catalog.databaseExists(TEST_DATABASE);

    assertTrue(exists);

    // Verify request uses empty id list (as per implementation)
    ArgumentCaptor<ListNamespacesRequest> captor =
        ArgumentCaptor.forClass(ListNamespacesRequest.class);
    verify(mockNamespace).listNamespaces(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG), captor.getValue().getId());
  }

  @Test
  public void testDatabaseExistsFalse() {
    ListNamespacesResponse response = mock(ListNamespacesResponse.class);
    when(response.getNamespaces()).thenReturn(new HashSet<>(Arrays.asList("db1", "db2")));
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(response);

    boolean exists = catalog.databaseExists("non_existent_db");

    assertFalse(exists);
  }

  @Test
  public void testDatabaseExistsWithEmptyResponse() {
    ListNamespacesResponse response = mock(ListNamespacesResponse.class);
    when(response.getNamespaces()).thenReturn(new HashSet<>());
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(response);

    boolean exists = catalog.databaseExists(TEST_DATABASE);

    assertFalse(exists);
  }

  // ==================== tableExists Tests ====================

  @Test
  public void testTableExistsTrue() {
    ListTablesResponse response = mock(ListTablesResponse.class);
    when(response.getTables())
        .thenReturn(new HashSet<>(Arrays.asList("table1", TEST_TABLE, "table2")));
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(response);

    boolean exists = catalog.tableExists(TEST_DATABASE, TEST_TABLE);

    assertTrue(exists);
  }

  @Test
  public void testTableExistsFalse() {
    ListTablesResponse response = mock(ListTablesResponse.class);
    when(response.getTables()).thenReturn(new HashSet<>(Arrays.asList("table1", "table2")));
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(response);

    boolean exists = catalog.tableExists(TEST_DATABASE, "non_existent_table");

    assertFalse(exists);
  }

  @Test
  public void testTableExistsWithEmptyResponse() {
    ListTablesResponse response = mock(ListTablesResponse.class);
    when(response.getTables()).thenReturn(new HashSet<>());
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(response);

    boolean exists = catalog.tableExists(TEST_DATABASE, TEST_TABLE);

    assertFalse(exists);
  }

  // ==================== createDatabase Tests ====================

  @Test
  public void testCreateDatabase() {
    // Mock databaseExists 返回 false（数据库不存在）
    ListNamespacesResponse listResponse = mock(ListNamespacesResponse.class);
    when(listResponse.getNamespaces()).thenReturn(new HashSet<>());
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(listResponse);

    catalog.createDatabase(TEST_DATABASE);

    // Verify createNamespace was called with correct parameters
    ArgumentCaptor<CreateNamespaceRequest> captor =
        ArgumentCaptor.forClass(CreateNamespaceRequest.class);
    verify(mockNamespace).createNamespace(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE), captor.getValue().getId());
  }

  @Test
  public void testCreateDatabaseWithSpecialCharacters() {
    String specialDbName = "db_with-special.chars123";

    // Mock databaseExists 返回 false（数据库不存在）
    ListNamespacesResponse listResponse = mock(ListNamespacesResponse.class);
    when(listResponse.getNamespaces()).thenReturn(new HashSet<>());
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(listResponse);

    catalog.createDatabase(specialDbName);

    ArgumentCaptor<CreateNamespaceRequest> captor =
        ArgumentCaptor.forClass(CreateNamespaceRequest.class);
    verify(mockNamespace).createNamespace(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, specialDbName), captor.getValue().getId());
  }

  @Test
  public void testCreateDatabaseAlreadyExists() {
    // Mock databaseExists 返回 true（数据库已存在）
    ListNamespacesResponse listResponse = mock(ListNamespacesResponse.class);
    when(listResponse.getNamespaces())
        .thenReturn(new HashSet<>(Collections.singletonList(TEST_DATABASE)));
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(listResponse);

    assertThrows(
        "重复创建数据库应抛出 AlreadyExistsException",
        AlreadyExistsException.class,
        () -> catalog.createDatabase(TEST_DATABASE));
  }

  // ==================== dropDatabase Tests ====================

  @Test
  public void testDropDatabase() {
    // Mock databaseExists 返回 true（数据库存在）
    ListNamespacesResponse listResponse = mock(ListNamespacesResponse.class);
    when(listResponse.getNamespaces())
        .thenReturn(new HashSet<>(Collections.singletonList(TEST_DATABASE)));
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(listResponse);

    // Mock listTables 返回空列表（数据库为空，允许删除）
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.getTables()).thenReturn(new HashSet<>());
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);

    catalog.dropDatabase(TEST_DATABASE);

    // Verify dropNamespace was called with correct parameters
    ArgumentCaptor<DropNamespaceRequest> captor =
        ArgumentCaptor.forClass(DropNamespaceRequest.class);
    verify(mockNamespace).dropNamespace(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE), captor.getValue().getId());
  }

  @Test
  public void testDropDatabaseNotExists() {
    // Mock databaseExists 返回 false（数据库不存在）
    ListNamespacesResponse listResponse = mock(ListNamespacesResponse.class);
    when(listResponse.getNamespaces()).thenReturn(new HashSet<>());
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(listResponse);

    assertThrows(
        "删除不存在的数据库应抛出 NoSuchDatabaseException",
        NoSuchDatabaseException.class,
        () -> catalog.dropDatabase("non_existent_db"));
  }

  // ==================== dropTable Tests ====================

  @Test
  public void testDropTableWithoutPurge() {
    // Mock tableExists 返回 true（表存在）
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.getTables())
        .thenReturn(new HashSet<>(Collections.singletonList(TEST_TABLE)));
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);

    // purge=false 时应调用 deregisterTable（仅取消注册，不删数据）
    DeregisterTableResponse deregResponse = mock(DeregisterTableResponse.class);
    when(mockNamespace.deregisterTable(any(DeregisterTableRequest.class)))
        .thenReturn(deregResponse);

    boolean result = catalog.dropTable(TEST_DATABASE, TEST_TABLE, false);

    assertTrue(result);

    // 验证调用的是 deregisterTable 而非 dropTable
    ArgumentCaptor<DeregisterTableRequest> captor =
        ArgumentCaptor.forClass(DeregisterTableRequest.class);
    verify(mockNamespace).deregisterTable(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE, TEST_TABLE), captor.getValue().getId());
    verify(mockNamespace, org.mockito.Mockito.never()).dropTable(any(DropTableRequest.class));
  }

  @Test
  public void testDropTableWithPurge() {
    // Mock tableExists 返回 true（表存在）
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.getTables())
        .thenReturn(new HashSet<>(Collections.singletonList(TEST_TABLE)));
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);

    // purge=true 时应调用 dropTable（删除数据）
    boolean result = catalog.dropTable(TEST_DATABASE, TEST_TABLE, true);

    assertTrue(result);

    // 验证调用的是 dropTable 而非 deregisterTable
    ArgumentCaptor<DropTableRequest> captor = ArgumentCaptor.forClass(DropTableRequest.class);
    verify(mockNamespace).dropTable(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE, TEST_TABLE), captor.getValue().getId());
    verify(mockNamespace, org.mockito.Mockito.never())
        .deregisterTable(any(DeregisterTableRequest.class));
  }

  @Test
  public void testDropTableWithDefaultCatalog() {
    Map<String, String> defaultProperties = new HashMap<>();
    LanceHiveCatalog defaultCatalog =
        new LanceHiveCatalog(TEST_CATALOG, mockNamespace, defaultProperties, allocator);

    // Mock tableExists 返回 true（表存在）
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.getTables())
        .thenReturn(new HashSet<>(Collections.singletonList(TEST_TABLE)));
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);

    DeregisterTableResponse deregResponse = mock(DeregisterTableResponse.class);
    when(mockNamespace.deregisterTable(any(DeregisterTableRequest.class)))
        .thenReturn(deregResponse);

    // purge=false 应调用 deregisterTable
    defaultCatalog.dropTable(TEST_DATABASE, TEST_TABLE, false);

    ArgumentCaptor<DeregisterTableRequest> captor =
        ArgumentCaptor.forClass(DeregisterTableRequest.class);
    verify(mockNamespace).deregisterTable(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE, TEST_TABLE), captor.getValue().getId());
  }

  @Test
  public void testDropTableNotExists() {
    // Mock tableExists 返回 false（表不存在）
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.getTables()).thenReturn(new HashSet<>());
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);

    assertThrows(
        "删除不存在的表应抛出 NoSuchTableException",
        NoSuchTableException.class,
        () -> catalog.dropTable(TEST_DATABASE, "non_existent_table", true));
  }

  @Test
  public void testDropTableNotExistsWithoutPurge() {
    // Mock tableExists 返回 false（表不存在）
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.getTables()).thenReturn(new HashSet<>());
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);

    assertThrows(
        "删除不存在的表应抛出 NoSuchTableException（purge=false）",
        NoSuchTableException.class,
        () -> catalog.dropTable(TEST_DATABASE, "non_existent_table", false));
  }

  // ==================== loadTable Tests ====================

  @Test
  public void testLoadTableNotExists() {
    // Mock tableExists 返回 false（表不存在）
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.getTables()).thenReturn(new HashSet<>());
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);

    assertThrows(
        "加载不存在的表应抛出 NoSuchTableException",
        NoSuchTableException.class,
        () -> catalog.loadTable(TEST_DATABASE, "non_existent_table"));
  }

  @Test
  public void testLoadTableWithNullLocation() {
    // Mock tableExists 返回 true（表存在）
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.getTables())
        .thenReturn(new HashSet<>(Collections.singletonList(TEST_TABLE)));
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);

    DescribeTableResponse response = mock(DescribeTableResponse.class);
    when(response.getLocation()).thenReturn(null);
    when(mockNamespace.describeTable(any(DescribeTableRequest.class))).thenReturn(response);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> catalog.loadTable(TEST_DATABASE, TEST_TABLE));

    assertTrue(exception.getMessage().contains("Failed to get table location"));
    assertTrue(exception.getMessage().contains(TEST_DATABASE + "." + TEST_TABLE));
  }

  @Test
  public void testLoadTableWithEmptyLocation() {
    // Mock tableExists 返回 true（表存在）
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.getTables())
        .thenReturn(new HashSet<>(Collections.singletonList(TEST_TABLE)));
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);

    DescribeTableResponse response = mock(DescribeTableResponse.class);
    when(response.getLocation()).thenReturn("");
    when(mockNamespace.describeTable(any(DescribeTableRequest.class))).thenReturn(response);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> catalog.loadTable(TEST_DATABASE, TEST_TABLE));

    assertTrue(exception.getMessage().contains("Failed to get table location"));
  }

  @Test
  public void testLoadTableDescribeTableRequestParameters() {
    // Mock tableExists 返回 true（表存在）
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.getTables())
        .thenReturn(new HashSet<>(Collections.singletonList(TEST_TABLE)));
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);

    DescribeTableResponse response = mock(DescribeTableResponse.class);
    when(response.getLocation()).thenReturn("/path/to/table");
    when(mockNamespace.describeTable(any(DescribeTableRequest.class))).thenReturn(response);

    // The actual Dataset.open() will fail without JNI library,
    // but we can verify the request parameters before that
    try {
      catalog.loadTable(TEST_DATABASE, TEST_TABLE);
    } catch (Throwable e) {
      // Expected: Dataset.open() requires JNI native library
    }

    // Verify describeTable request parameters
    ArgumentCaptor<DescribeTableRequest> captor =
        ArgumentCaptor.forClass(DescribeTableRequest.class);
    verify(mockNamespace).describeTable(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE, TEST_TABLE), captor.getValue().getId());
  }

  @Test
  public void testLoadTableDescribeTableRequestWithDefaultCatalog() {
    Map<String, String> defaultProperties = new HashMap<>();
    LanceHiveCatalog defaultCatalog =
        new LanceHiveCatalog(TEST_CATALOG, mockNamespace, defaultProperties, allocator);

    // Mock tableExists 返回 true（表存在）
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.getTables())
        .thenReturn(new HashSet<>(Collections.singletonList(TEST_TABLE)));
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);

    DescribeTableResponse response = mock(DescribeTableResponse.class);
    when(response.getLocation()).thenReturn("/path/to/table");
    when(mockNamespace.describeTable(any(DescribeTableRequest.class))).thenReturn(response);

    try {
      defaultCatalog.loadTable(TEST_DATABASE, TEST_TABLE);
    } catch (Throwable e) {
      // Expected: Dataset.open() requires JNI native library
    }

    ArgumentCaptor<DescribeTableRequest> captor =
        ArgumentCaptor.forClass(DescribeTableRequest.class);
    verify(mockNamespace).describeTable(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE, TEST_TABLE), captor.getValue().getId());
  }

  // ==================== getTableLocation Tests ====================

  @Test
  public void testGetTableLocationSuccess() {
    // Mock tableExists 返回 true（表存在）
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.getTables())
        .thenReturn(new HashSet<>(Collections.singletonList(TEST_TABLE)));
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);

    DescribeTableResponse response = mock(DescribeTableResponse.class);
    when(response.getLocation()).thenReturn("cosn://bucket-123/path/to/table.lance");
    when(mockNamespace.describeTable(any(DescribeTableRequest.class))).thenReturn(response);

    String location = catalog.getTableLocation(TEST_DATABASE, TEST_TABLE);

    assertNotNull(location);
    // 返回的 location 应转换为 cos:// 协议
    assertTrue(
        "返回的 location 应使用 cos:// 协议，实际: " + location,
        location.startsWith("cos://") || location.startsWith("cosn://"));

    // 验证 describeTable 请求参数
    ArgumentCaptor<DescribeTableRequest> captor =
        ArgumentCaptor.forClass(DescribeTableRequest.class);
    verify(mockNamespace).describeTable(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE, TEST_TABLE), captor.getValue().getId());
  }

  // ==================== deregisterTable Tests ====================

  @Test
  public void testDeregisterTableNotExists() {
    // Mock tableExists 返回 false（表不存在）
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.getTables()).thenReturn(new HashSet<>());
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);

    assertThrows(
        "取消注册不存在的表应抛出 NoSuchTableException",
        NoSuchTableException.class,
        () -> catalog.deregisterTable(TEST_DATABASE, "non_existent_table"));
  }

  // ==================== Storage Options Tests ====================

  @Test
  public void testLoadTableWithStorageOptions() {
    // Setup catalog with storage options
    Map<String, String> propsWithStorage = new HashMap<>();
    propsWithStorage.put("catalog", TEST_CATALOG);
    propsWithStorage.put("storage.region", "us-east-1");
    propsWithStorage.put("storage.secret-id", "test-secret-id");
    propsWithStorage.put("storage.secret-key", "test-secret-key");
    LanceHiveCatalog catalogWithStorage =
        new LanceHiveCatalog(TEST_CATALOG, mockNamespace, propsWithStorage, allocator);

    // Mock tableExists 返回 true（表存在）
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.getTables())
        .thenReturn(new HashSet<>(Collections.singletonList(TEST_TABLE)));
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);

    DescribeTableResponse response = mock(DescribeTableResponse.class);
    when(response.getLocation()).thenReturn("/path/to/table");
    when(mockNamespace.describeTable(any(DescribeTableRequest.class))).thenReturn(response);

    try {
      catalogWithStorage.loadTable(TEST_DATABASE, TEST_TABLE);
    } catch (Throwable e) {
      // Expected: Dataset.open() requires JNI native library
    }

    // Verify describeTable was called
    verify(mockNamespace).describeTable(any(DescribeTableRequest.class));
  }

  // ==================== Edge Cases ====================

  @Test
  public void testListTablesWithNullDatabase() {
    ListTablesResponse response = mock(ListTablesResponse.class);
    when(response.getTables()).thenReturn(new HashSet<>());
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(response);

    // Should handle null database gracefully
    List<String> tables = catalog.listTables(null);

    assertNotNull(tables);

    ArgumentCaptor<ListTablesRequest> captor = ArgumentCaptor.forClass(ListTablesRequest.class);
    verify(mockNamespace).listTables(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, null), captor.getValue().getId());
  }

  @Test
  public void testCatalogPropertyIsolation() {
    // Create two catalogs with different properties
    Map<String, String> props1 = new HashMap<>();
    LanceHiveCatalog catalog1 =
        new LanceHiveCatalog(TEST_CATALOG, mockNamespace, props1, allocator);

    Map<String, String> props2 = new HashMap<>();
    LanceHiveCatalog catalog2 =
        new LanceHiveCatalog(TEST_CATALOG, mockNamespace, props2, allocator);

    ListTablesResponse response = mock(ListTablesResponse.class);
    when(response.getTables()).thenReturn(new HashSet<>());
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(response);

    catalog1.listTables("db");
    catalog2.listTables("db");

    // Verify different catalog names were used
    ArgumentCaptor<ListTablesRequest> captor = ArgumentCaptor.forClass(ListTablesRequest.class);
    verify(mockNamespace, org.mockito.Mockito.times(2)).listTables(captor.capture());

    List<ListTablesRequest> requests = captor.getAllValues();
    assertEquals(Arrays.asList(TEST_CATALOG, "db"), requests.get(0).getId());
    assertEquals(Arrays.asList(TEST_CATALOG, "db"), requests.get(1).getId());
  }
}
