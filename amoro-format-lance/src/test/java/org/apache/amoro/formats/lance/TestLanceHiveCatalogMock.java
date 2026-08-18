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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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
import org.lance.namespace.errors.NamespaceAlreadyExistsException;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.TableNotFoundException;
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
import org.lance.namespace.model.NamespaceExistsRequest;
import org.lance.namespace.model.TableExistsRequest;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/** Unit tests for {@link LanceHiveCatalog} public methods using Mockito mocks. */
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
    ListNamespacesResponse response = mock(ListNamespacesResponse.class);
    when(response.getNamespaces()).thenReturn(new HashSet<>(Arrays.asList("db1", "db2", "db3")));
    when(response.getPageToken()).thenReturn(null);
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(response);

    List<String> databases = catalog.listDatabases();

    assertNotNull(databases);
    assertEquals(3, databases.size());
    assertTrue(databases.contains("db1"));
    assertTrue(databases.contains("db2"));
    assertTrue(databases.contains("db3"));

    ArgumentCaptor<ListNamespacesRequest> captor =
        ArgumentCaptor.forClass(ListNamespacesRequest.class);
    verify(mockNamespace).listNamespaces(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG), captor.getValue().getId());
  }

  @Test
  public void testListDatabasesEmpty() {
    ListNamespacesResponse response = mock(ListNamespacesResponse.class);
    when(response.getNamespaces()).thenReturn(new HashSet<>());
    when(response.getPageToken()).thenReturn(null);
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(response);

    List<String> databases = catalog.listDatabases();

    assertNotNull(databases);
    assertTrue(databases.isEmpty());
  }

  // ==================== listTables Tests ====================

  @Test
  public void testListTables() {
    ListTablesResponse response = mock(ListTablesResponse.class);
    when(response.getTables())
        .thenReturn(new HashSet<>(Arrays.asList("table1", "table2", "table3")));
    when(response.getPageToken()).thenReturn(null);
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(response);

    List<String> tables = catalog.listTables(TEST_DATABASE);

    assertNotNull(tables);
    assertEquals(3, tables.size());
    assertTrue(tables.contains("table1"));

    ArgumentCaptor<ListTablesRequest> captor = ArgumentCaptor.forClass(ListTablesRequest.class);
    verify(mockNamespace).listTables(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE), captor.getValue().getId());
  }

  @Test
  public void testListTablesEmpty() {
    ListTablesResponse response = mock(ListTablesResponse.class);
    when(response.getTables()).thenReturn(new HashSet<>());
    when(response.getPageToken()).thenReturn(null);
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(response);

    List<String> tables = catalog.listTables(TEST_DATABASE);

    assertNotNull(tables);
    assertTrue(tables.isEmpty());
  }

  @Test
  public void testListTablesWithPagination() {
    ListTablesResponse page1 = mock(ListTablesResponse.class);
    when(page1.getTables()).thenReturn(new HashSet<>(Arrays.asList("table1", "table2")));
    when(page1.getPageToken()).thenReturn("page2-token");
    ListTablesResponse page2 = mock(ListTablesResponse.class);
    when(page2.getTables()).thenReturn(new HashSet<>(Arrays.asList("table3", "table4")));
    when(page2.getPageToken()).thenReturn(null);

    when(mockNamespace.listTables(any(ListTablesRequest.class)))
        .thenReturn(page1)
        .thenReturn(page2);

    List<String> tables = catalog.listTables(TEST_DATABASE);

    assertNotNull(tables);
    assertEquals(4, tables.size());
    assertTrue(tables.containsAll(Arrays.asList("table1", "table2", "table3", "table4")));

    verify(mockNamespace, times(2)).listTables(any(ListTablesRequest.class));

    ArgumentCaptor<ListTablesRequest> captor = ArgumentCaptor.forClass(ListTablesRequest.class);
    verify(mockNamespace, times(2)).listTables(captor.capture());
    List<ListTablesRequest> requests = captor.getAllValues();
    assertNull(requests.get(0).getPageToken());
    assertEquals("page2-token", requests.get(1).getPageToken());
  }

  // ==================== databaseExists Tests ====================

  @Test
  public void testDatabaseExistsTrue() {
    // namespaceExists returns normally (no exception) when the database exists.
    boolean exists = catalog.databaseExists(TEST_DATABASE);

    assertTrue(exists);

    ArgumentCaptor<NamespaceExistsRequest> captor =
        ArgumentCaptor.forClass(NamespaceExistsRequest.class);
    verify(mockNamespace).namespaceExists(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE), captor.getValue().getId());
  }

  @Test
  public void testDatabaseExistsFalse() {
    doThrow(new NamespaceNotFoundException("not found"))
        .when(mockNamespace)
        .namespaceExists(any(NamespaceExistsRequest.class));

    boolean exists = catalog.databaseExists("non_existent_db");

    assertFalse(exists);
  }

  // ==================== tableExists Tests ====================

  @Test
  public void testTableExistsTrue() {
    // tableExists returns normally (no exception) when the table exists.
    boolean exists = catalog.tableExists(TEST_DATABASE, TEST_TABLE);

    assertTrue(exists);

    ArgumentCaptor<TableExistsRequest> captor = ArgumentCaptor.forClass(TableExistsRequest.class);
    verify(mockNamespace).tableExists(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE, TEST_TABLE), captor.getValue().getId());
  }

  @Test
  public void testTableExistsFalse() {
    doThrow(new TableNotFoundException("table not found"))
        .when(mockNamespace)
        .tableExists(any(TableExistsRequest.class));

    boolean exists = catalog.tableExists(TEST_DATABASE, "non_existent_table");

    assertFalse(exists);
  }

  // ==================== createDatabase Tests ====================

  @Test
  public void testCreateDatabase() {
    catalog.createDatabase(TEST_DATABASE);

    ArgumentCaptor<CreateNamespaceRequest> captor =
        ArgumentCaptor.forClass(CreateNamespaceRequest.class);
    verify(mockNamespace).createNamespace(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE), captor.getValue().getId());
  }

  @Test
  public void testCreateDatabaseAlreadyExists() {
    doThrow(new NamespaceAlreadyExistsException("already exists"))
        .when(mockNamespace)
        .createNamespace(any(CreateNamespaceRequest.class));

    assertThrows(AlreadyExistsException.class, () -> catalog.createDatabase(TEST_DATABASE));
  }

  // ==================== dropDatabase Tests ====================

  @Test
  public void testDropDatabase() {
    // listTables returns empty (database is empty, allow drop)
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.getTables()).thenReturn(new HashSet<>());
    when(listTablesResponse.getPageToken()).thenReturn(null);
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);

    catalog.dropDatabase(TEST_DATABASE);

    ArgumentCaptor<DropNamespaceRequest> captor =
        ArgumentCaptor.forClass(DropNamespaceRequest.class);
    verify(mockNamespace).dropNamespace(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE), captor.getValue().getId());
  }

  @Test
  public void testDropDatabaseNotExists() {
    // listTables throws NamespaceNotFoundException → dropDatabase catches and re-throws as
    // NoSuchDatabaseException (the listTables call inside dropDatabase triggers the error because
    // the database doesn't exist).
    when(mockNamespace.listTables(any(ListTablesRequest.class)))
        .thenThrow(new NamespaceNotFoundException("not found"));

    assertThrows(NoSuchDatabaseException.class, () -> catalog.dropDatabase("non_existent_db"));
  }

  // ==================== dropTable Tests ====================

  @Test
  public void testDropTableWithoutPurge() {
    // purge=false calls deregisterTable directly (no pre-existence check).
    DeregisterTableResponse deregResponse = mock(DeregisterTableResponse.class);
    when(mockNamespace.deregisterTable(any(DeregisterTableRequest.class)))
        .thenReturn(deregResponse);

    boolean result = catalog.dropTable(TEST_DATABASE, TEST_TABLE, false);

    assertTrue(result);

    ArgumentCaptor<DeregisterTableRequest> captor =
        ArgumentCaptor.forClass(DeregisterTableRequest.class);
    verify(mockNamespace).deregisterTable(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE, TEST_TABLE), captor.getValue().getId());
    verify(mockNamespace, never()).dropTable(any(DropTableRequest.class));
  }

  @Test
  public void testDropTableWithPurge() {
    // purge=true calls dropTable directly (no pre-existence check).
    boolean result = catalog.dropTable(TEST_DATABASE, TEST_TABLE, true);

    assertTrue(result);

    ArgumentCaptor<DropTableRequest> captor = ArgumentCaptor.forClass(DropTableRequest.class);
    verify(mockNamespace).dropTable(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE, TEST_TABLE), captor.getValue().getId());
    verify(mockNamespace, never()).deregisterTable(any(DeregisterTableRequest.class));
  }

  @Test
  public void testDropTableNotExists() {
    // dropTable calls namespace.dropTable directly; TableNotFoundException is caught and re-thrown
    // as NoSuchTableException.
    doThrow(new TableNotFoundException("table not found"))
        .when(mockNamespace)
        .dropTable(any(DropTableRequest.class));

    assertThrows(
        NoSuchTableException.class,
        () -> catalog.dropTable(TEST_DATABASE, "non_existent_table", true));
  }

  @Test
  public void testDropTableNotExistsWithoutPurge() {
    doThrow(new TableNotFoundException("table not found"))
        .when(mockNamespace)
        .deregisterTable(any(DeregisterTableRequest.class));

    assertThrows(
        NoSuchTableException.class,
        () -> catalog.dropTable(TEST_DATABASE, "non_existent_table", false));
  }

  // ==================== loadTable Tests ====================

  @Test
  public void testLoadTableNotExists() {
    // loadTable calls describeTable directly; TableNotFoundException is caught and re-thrown as
    // NoSuchTableException.
    doThrow(new TableNotFoundException("table not found"))
        .when(mockNamespace)
        .describeTable(any(DescribeTableRequest.class));

    assertThrows(
        NoSuchTableException.class, () -> catalog.loadTable(TEST_DATABASE, "non_existent_table"));
  }

  @Test
  public void testLoadTableWithNullLocation() {
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
    DescribeTableResponse response = mock(DescribeTableResponse.class);
    when(response.getLocation()).thenReturn("/path/to/table");
    when(mockNamespace.describeTable(any(DescribeTableRequest.class))).thenReturn(response);

    // The actual Dataset.open() will fail without JNI library,
    // but we can verify the request parameters before that.
    try {
      catalog.loadTable(TEST_DATABASE, TEST_TABLE);
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
    DescribeTableResponse response = mock(DescribeTableResponse.class);
    when(response.getLocation()).thenReturn("cosn://bucket-123/path/to/table.lance");
    when(mockNamespace.describeTable(any(DescribeTableRequest.class))).thenReturn(response);

    String location = catalog.getTableLocation(TEST_DATABASE, TEST_TABLE);

    assertNotNull(location);
    assertTrue(
        "returned location should use the cos:// scheme, actual: " + location,
        location.startsWith("cos://") || location.startsWith("cosn://"));

    ArgumentCaptor<DescribeTableRequest> captor =
        ArgumentCaptor.forClass(DescribeTableRequest.class);
    verify(mockNamespace).describeTable(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE, TEST_TABLE), captor.getValue().getId());
  }

  @Test
  public void testGetTableLocationNotExists() {
    doThrow(new TableNotFoundException("table not found"))
        .when(mockNamespace)
        .describeTable(any(DescribeTableRequest.class));

    assertThrows(
        NoSuchTableException.class,
        () -> catalog.getTableLocation(TEST_DATABASE, "non_existent_table"));
  }

  // ==================== deregisterTable Tests ====================

  @Test
  public void testDeregisterTableNotExists() {
    doThrow(new TableNotFoundException("table not found"))
        .when(mockNamespace)
        .deregisterTable(any(DeregisterTableRequest.class));

    assertThrows(
        NoSuchTableException.class,
        () -> catalog.deregisterTable(TEST_DATABASE, "non_existent_table"));
  }

  // ==================== Constructor Catalog Creation Fallback Tests ====================

  @Test
  public void testConstructorCreatesCatalogForNonHiveName() {
    LanceNamespace nonHiveMockNamespace = mock(LanceNamespace.class);

    LanceHiveCatalog nonHiveCatalog =
        new LanceHiveCatalog("test_catalog", nonHiveMockNamespace, properties, allocator);

    assertNotNull(nonHiveCatalog);
    verify(nonHiveMockNamespace).createNamespace(any(CreateNamespaceRequest.class));
  }

  @Test
  public void testConstructorCatalogAlreadyExistsIsIgnored() {
    LanceNamespace nonHiveMockNamespace = mock(LanceNamespace.class);
    doThrow(new NamespaceAlreadyExistsException("already exists"))
        .when(nonHiveMockNamespace)
        .createNamespace(any(CreateNamespaceRequest.class));

    LanceHiveCatalog nonHiveCatalog =
        new LanceHiveCatalog("test_catalog", nonHiveMockNamespace, properties, allocator);

    assertNotNull(nonHiveCatalog);
    verify(nonHiveMockNamespace).createNamespace(any(CreateNamespaceRequest.class));
  }

  @Test
  public void testConstructorCatalogAlreadyExistsByMessageIsIgnored() {
    LanceNamespace nonHiveMockNamespace = mock(LanceNamespace.class);
    doThrow(new RuntimeException("namespace test_catalog already exists"))
        .when(nonHiveMockNamespace)
        .createNamespace(any(CreateNamespaceRequest.class));

    LanceHiveCatalog nonHiveCatalog =
        new LanceHiveCatalog("test_catalog", nonHiveMockNamespace, properties, allocator);

    assertNotNull(nonHiveCatalog);
    verify(nonHiveMockNamespace).createNamespace(any(CreateNamespaceRequest.class));
  }

  @Test
  public void testConstructorCatalogCreationErrorIsPropagated() {
    LanceNamespace nonHiveMockNamespace = mock(LanceNamespace.class);
    doThrow(new RuntimeException("connection refused"))
        .when(nonHiveMockNamespace)
        .createNamespace(any(CreateNamespaceRequest.class));

    assertThrows(
        RuntimeException.class,
        () -> new LanceHiveCatalog("test_catalog", nonHiveMockNamespace, properties, allocator));
  }

  // ==================== Edge Cases ====================

  @Test
  public void testListTablesWithNullDatabase() {
    ListTablesResponse response = mock(ListTablesResponse.class);
    when(response.getTables()).thenReturn(new HashSet<>());
    when(response.getPageToken()).thenReturn(null);
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(response);

    List<String> tables = catalog.listTables(null);

    assertNotNull(tables);

    ArgumentCaptor<ListTablesRequest> captor = ArgumentCaptor.forClass(ListTablesRequest.class);
    verify(mockNamespace).listTables(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, null), captor.getValue().getId());
  }

  @Test
  public void testCatalogPropertyIsolation() {
    Map<String, String> props1 = new HashMap<>();
    LanceHiveCatalog catalog1 =
        new LanceHiveCatalog(TEST_CATALOG, mockNamespace, props1, allocator);

    Map<String, String> props2 = new HashMap<>();
    LanceHiveCatalog catalog2 =
        new LanceHiveCatalog(TEST_CATALOG, mockNamespace, props2, allocator);

    ListTablesResponse response = mock(ListTablesResponse.class);
    when(response.getTables()).thenReturn(new HashSet<>());
    when(response.getPageToken()).thenReturn(null);
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(response);

    catalog1.listTables("db");
    catalog2.listTables("db");

    ArgumentCaptor<ListTablesRequest> captor = ArgumentCaptor.forClass(ListTablesRequest.class);
    verify(mockNamespace, times(2)).listTables(captor.capture());

    List<ListTablesRequest> requests = captor.getAllValues();
    assertEquals(Arrays.asList(TEST_CATALOG, "db"), requests.get(0).getId());
    assertEquals(Arrays.asList(TEST_CATALOG, "db"), requests.get(1).getId());
  }
}
