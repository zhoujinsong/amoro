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
import org.apache.amoro.DatabaseNotEmptyException;
import org.apache.amoro.NoSuchDatabaseException;
import org.apache.amoro.NoSuchTableException;
import org.junit.Before;
import org.junit.Test;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.NamespaceAlreadyExistsException;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/** Unit tests for {@link LanceRestCatalog} public methods using Mockito mocks. */
public class TestLanceRestCatalogMock {

  private static final String TEST_CATALOG = "test_catalog";
  private static final String TEST_DATABASE = "test_db";
  private static final String TEST_TABLE = "test_table";

  private LanceNamespace mockNamespace;
  private LanceRestCatalog catalog;
  private Map<String, String> properties;

  @Before
  public void setUp() {
    mockNamespace = mock(LanceNamespace.class);
    properties = new HashMap<>();
    properties.put(LanceRestCatalog.REST_CATALOG_NAME, TEST_CATALOG);
    catalog = new LanceRestCatalog(mockNamespace, properties);
  }

  // ==================== constructor / catalog name Tests ====================

  @Test
  public void testMissingCatalogNameRejected() {
    assertThrows(
        IllegalArgumentException.class, () -> new LanceRestCatalog(mockNamespace, new HashMap<>()));
  }

  @Test
  public void testBlankCatalogNameRejected() {
    Map<String, String> blank = new HashMap<>();
    blank.put(LanceRestCatalog.REST_CATALOG_NAME, "  ");
    assertThrows(IllegalArgumentException.class, () -> new LanceRestCatalog(mockNamespace, blank));
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

    ArgumentCaptor<ListNamespacesRequest> captor =
        ArgumentCaptor.forClass(ListNamespacesRequest.class);
    verify(mockNamespace).listNamespaces(captor.capture());
    assertEquals(Collections.singletonList(TEST_CATALOG), captor.getValue().getId());
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

  // ==================== databaseExists Tests ====================

  @Test
  public void testDatabaseExistsTrue() {
    // namespaceExists returns normally (no exception) when the database exists.
    assertTrue(catalog.databaseExists(TEST_DATABASE));

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

    assertFalse(catalog.databaseExists("non_existent_db"));
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

    assertTrue(catalog.listTables(TEST_DATABASE).isEmpty());
  }

  @Test
  public void testListTablesDatabaseNotExists() {
    when(mockNamespace.listTables(any(ListTablesRequest.class)))
        .thenThrow(new NamespaceNotFoundException("not found"));

    assertThrows(NoSuchDatabaseException.class, () -> catalog.listTables("non_existent_db"));
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

    assertEquals(4, tables.size());
    assertTrue(tables.containsAll(Arrays.asList("table1", "table2", "table3", "table4")));

    verify(mockNamespace, times(2)).listTables(any(ListTablesRequest.class));

    ArgumentCaptor<ListTablesRequest> captor = ArgumentCaptor.forClass(ListTablesRequest.class);
    verify(mockNamespace, times(2)).listTables(captor.capture());
    List<ListTablesRequest> requests = captor.getAllValues();
    assertNull(requests.get(0).getPageToken());
    assertEquals("page2-token", requests.get(1).getPageToken());
  }

  // ==================== tableExists Tests ====================

  @Test
  public void testTableExistsTrue() {
    // tableExists returns normally (no exception) when the table exists.
    assertTrue(catalog.tableExists(TEST_DATABASE, TEST_TABLE));

    ArgumentCaptor<TableExistsRequest> captor = ArgumentCaptor.forClass(TableExistsRequest.class);
    verify(mockNamespace).tableExists(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE, TEST_TABLE), captor.getValue().getId());
  }

  @Test
  public void testTableExistsFalse() {
    doThrow(new TableNotFoundException("table not found"))
        .when(mockNamespace)
        .tableExists(any(TableExistsRequest.class));

    assertFalse(catalog.tableExists(TEST_DATABASE, "non_existent_table"));
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
  public void testCreateDatabaseWithMetadata() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("owner", "amoro");

    catalog.createDatabase(TEST_DATABASE, metadata);

    ArgumentCaptor<CreateNamespaceRequest> captor =
        ArgumentCaptor.forClass(CreateNamespaceRequest.class);
    verify(mockNamespace).createNamespace(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE), captor.getValue().getId());
    assertEquals(metadata, captor.getValue().getProperties());
  }

  @Test
  public void testCreateDatabaseAlreadyExists() {
    doThrow(new NamespaceAlreadyExistsException("already exists"))
        .when(mockNamespace)
        .createNamespace(any(CreateNamespaceRequest.class));

    assertThrows(AlreadyExistsException.class, () -> catalog.createDatabase(TEST_DATABASE));
  }

  // ==================== loadDatabaseMetadata Tests ====================

  @Test
  public void testLoadDatabaseMetadata() {
    Map<String, String> props = new HashMap<>();
    props.put("owner", "amoro");
    DescribeNamespaceResponse response = mock(DescribeNamespaceResponse.class);
    when(response.getProperties()).thenReturn(props);
    when(mockNamespace.describeNamespace(any(DescribeNamespaceRequest.class))).thenReturn(response);

    Map<String, String> result = catalog.loadDatabaseMetadata(TEST_DATABASE);

    assertEquals(props, result);

    ArgumentCaptor<DescribeNamespaceRequest> captor =
        ArgumentCaptor.forClass(DescribeNamespaceRequest.class);
    verify(mockNamespace).describeNamespace(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE), captor.getValue().getId());
  }

  @Test
  public void testLoadDatabaseMetadataNotExists() {
    when(mockNamespace.describeNamespace(any(DescribeNamespaceRequest.class)))
        .thenThrow(new NamespaceNotFoundException("not found"));

    assertThrows(
        NoSuchDatabaseException.class, () -> catalog.loadDatabaseMetadata("non_existent_db"));
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
    // NoSuchDatabaseException.
    when(mockNamespace.listTables(any(ListTablesRequest.class)))
        .thenThrow(new NamespaceNotFoundException("not found"));

    assertThrows(NoSuchDatabaseException.class, () -> catalog.dropDatabase(TEST_DATABASE));
  }

  @Test
  public void testDropDatabaseNotEmpty() {
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.getTables())
        .thenReturn(new HashSet<>(Collections.singletonList(TEST_TABLE)));
    when(listTablesResponse.getPageToken()).thenReturn(null);
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);

    assertThrows(DatabaseNotEmptyException.class, () -> catalog.dropDatabase(TEST_DATABASE));
  }

  // ==================== dropTable Tests ====================

  @Test
  public void testDropTable() {
    boolean dropped = catalog.dropTable(TEST_DATABASE, TEST_TABLE, true);

    assertTrue(dropped);

    ArgumentCaptor<DropTableRequest> captor = ArgumentCaptor.forClass(DropTableRequest.class);
    verify(mockNamespace).dropTable(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE, TEST_TABLE), captor.getValue().getId());
  }

  @Test
  public void testDropTableNotExists() {
    doThrow(new TableNotFoundException("table not found"))
        .when(mockNamespace)
        .dropTable(any(DropTableRequest.class));

    assertThrows(
        NoSuchTableException.class, () -> catalog.dropTable(TEST_DATABASE, TEST_TABLE, true));
  }

  // ==================== set/removeDatabaseProperties Tests ====================

  @Test
  public void testSetDatabasePropertiesNoOp() {
    catalog.setDatabaseProperties(TEST_DATABASE, Collections.singletonMap("k", "v"));
    verify(mockNamespace, never()).createNamespace(any(CreateNamespaceRequest.class));
  }

  @Test
  public void testRemoveDatabasePropertiesNoOp() {
    catalog.removeDatabaseProperties(TEST_DATABASE, Collections.singleton("k"));
    verify(mockNamespace, never()).createNamespace(any(CreateNamespaceRequest.class));
  }
}
