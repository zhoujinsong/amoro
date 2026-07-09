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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.AlreadyExistsException;
import org.apache.amoro.DatabaseNotEmptyException;
import org.apache.amoro.NoSuchDatabaseException;
import org.apache.amoro.NoSuchTableException;
import org.junit.Before;
import org.junit.Test;
import org.lance.namespace.LanceNamespace;
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
import org.lance.namespace.model.TableExistsRequest;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link LanceRestCatalog} public methods.
 *
 * <p>This test class follows the Amoro project testing style, using Mockito to mock the {@link
 * LanceNamespace} dependency. Tests cover the metadata operations defined in the {@code
 * FormatCatalog} interface that only touch the namespace (no JNI native library).
 *
 * <p>The REST catalog uses a three-level namespace: the remote catalog name (from the {@code
 * catalog} property) is the top level, so databases are addressed as {@code id = [catalog,
 * database]} and tables as {@code id = [catalog, database, table]}; listing databases uses {@code
 * id = [catalog]}.
 *
 * <p>Note: {@code loadTable} relies on {@code Dataset.open()} which requires the Lance JNI native
 * library, so it is not covered here.
 */
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
    // No 'catalog' property -> 3-level addressing impossible -> reject at construction.
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
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(response);

    List<String> databases = catalog.listDatabases();

    assertNotNull(databases);
    assertEquals(3, databases.size());
    assertTrue(databases.contains("db1"));
    assertTrue(databases.contains("db2"));
    assertTrue(databases.contains("db3"));

    // Databases are listed under the catalog namespace: id = [catalog].
    ArgumentCaptor<ListNamespacesRequest> captor =
        ArgumentCaptor.forClass(ListNamespacesRequest.class);
    verify(mockNamespace).listNamespaces(captor.capture());
    assertEquals(Collections.singletonList(TEST_CATALOG), captor.getValue().getId());
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

  // ==================== databaseExists Tests ====================

  @Test
  public void testDatabaseExistsTrue() {
    ListNamespacesResponse response = mock(ListNamespacesResponse.class);
    when(response.getNamespaces())
        .thenReturn(new HashSet<>(Arrays.asList("db1", TEST_DATABASE, "db2")));
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(response);

    assertTrue(catalog.databaseExists(TEST_DATABASE));
  }

  @Test
  public void testDatabaseExistsFalse() {
    ListNamespacesResponse response = mock(ListNamespacesResponse.class);
    when(response.getNamespaces()).thenReturn(new HashSet<>(Arrays.asList("db1", "db2")));
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(response);

    assertFalse(catalog.databaseExists("non_existent_db"));
  }

  // ==================== listTables Tests ====================

  @Test
  public void testListTables() {
    ListTablesResponse response = mock(ListTablesResponse.class);
    when(response.getTables())
        .thenReturn(new HashSet<>(Arrays.asList("table1", "table2", "table3")));
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(response);

    List<String> tables = catalog.listTables(TEST_DATABASE);

    assertNotNull(tables);
    assertEquals(3, tables.size());
    assertTrue(tables.contains("table1"));

    // REST table listing uses a three-level id prefix: [catalog, database].
    ArgumentCaptor<ListTablesRequest> captor = ArgumentCaptor.forClass(ListTablesRequest.class);
    verify(mockNamespace).listTables(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE), captor.getValue().getId());
  }

  @Test
  public void testListTablesEmpty() {
    ListTablesResponse response = mock(ListTablesResponse.class);
    when(response.getTables()).thenReturn(new HashSet<>());
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(response);

    assertTrue(catalog.listTables(TEST_DATABASE).isEmpty());
  }

  @Test
  public void testListTablesDatabaseNotExists() {
    when(mockNamespace.listTables(any(ListTablesRequest.class)))
        .thenThrow(new NamespaceNotFoundException("not found"));

    assertThrows(NoSuchDatabaseException.class, () -> catalog.listTables("non_existent_db"));
  }

  // ==================== tableExists Tests ====================

  @Test
  public void testTableExistsTrue() {
    ListTablesResponse response = mock(ListTablesResponse.class);
    when(response.getTables())
        .thenReturn(new HashSet<>(Arrays.asList("table1", TEST_TABLE, "table2")));
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(response);

    assertTrue(catalog.tableExists(TEST_DATABASE, TEST_TABLE));
  }

  @Test
  public void testTableExistsFalse() {
    doThrow(new TableNotFoundException("table not found"))
        .when(mockNamespace)
        .tableExists(any(TableExistsRequest.class));

    assertFalse(catalog.tableExists(TEST_DATABASE, "non_existent_table"));
  }

  @Test
  public void testTableExistsWhenDatabaseNotExists() {
    doThrow(new TableNotFoundException("table not found"))
        .when(mockNamespace)
        .tableExists(any(TableExistsRequest.class));

    // tableExists should swallow the not-found case and return false.
    assertFalse(catalog.tableExists("non_existent_db", TEST_TABLE));
  }

  // ==================== createDatabase Tests ====================

  @Test
  public void testCreateDatabase() {
    ListNamespacesResponse listResponse = mock(ListNamespacesResponse.class);
    when(listResponse.getNamespaces()).thenReturn(new HashSet<>());
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(listResponse);

    catalog.createDatabase(TEST_DATABASE);

    ArgumentCaptor<CreateNamespaceRequest> captor =
        ArgumentCaptor.forClass(CreateNamespaceRequest.class);
    verify(mockNamespace).createNamespace(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE), captor.getValue().getId());
  }

  @Test
  public void testCreateDatabaseWithMetadata() {
    ListNamespacesResponse listResponse = mock(ListNamespacesResponse.class);
    when(listResponse.getNamespaces()).thenReturn(new HashSet<>());
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(listResponse);

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
    ListNamespacesResponse listResponse = mock(ListNamespacesResponse.class);
    when(listResponse.getNamespaces())
        .thenReturn(new HashSet<>(Collections.singletonList(TEST_DATABASE)));
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(listResponse);

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
    ListNamespacesResponse listResponse = mock(ListNamespacesResponse.class);
    when(listResponse.getNamespaces())
        .thenReturn(new HashSet<>(Collections.singletonList(TEST_DATABASE)));
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(listResponse);

    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.getTables()).thenReturn(new HashSet<>());
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);

    catalog.dropDatabase(TEST_DATABASE);

    ArgumentCaptor<DropNamespaceRequest> captor =
        ArgumentCaptor.forClass(DropNamespaceRequest.class);
    verify(mockNamespace).dropNamespace(captor.capture());
    assertEquals(Arrays.asList(TEST_CATALOG, TEST_DATABASE), captor.getValue().getId());
  }

  @Test
  public void testDropDatabaseNotExists() {
    ListNamespacesResponse listResponse = mock(ListNamespacesResponse.class);
    when(listResponse.getNamespaces()).thenReturn(new HashSet<>());
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(listResponse);

    assertThrows(NoSuchDatabaseException.class, () -> catalog.dropDatabase(TEST_DATABASE));
  }

  @Test
  public void testDropDatabaseNotEmpty() {
    ListNamespacesResponse listResponse = mock(ListNamespacesResponse.class);
    when(listResponse.getNamespaces())
        .thenReturn(new HashSet<>(Collections.singletonList(TEST_DATABASE)));
    when(mockNamespace.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(listResponse);

    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.getTables())
        .thenReturn(new HashSet<>(Collections.singletonList(TEST_TABLE)));
    when(mockNamespace.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);

    assertThrows(DatabaseNotEmptyException.class, () -> catalog.dropDatabase(TEST_DATABASE));
  }

  // ==================== dropTable Tests ====================

  @Test
  public void testDropTable() {
    // dropTable calls the namespace directly without a pre-existence check.
    boolean dropped = catalog.dropTable(TEST_DATABASE, TEST_TABLE, true);

    assertTrue(dropped);

    // REST table operations use a three-level id: [catalog, database, table].
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
    // Currently a no-op (logs a warning); should not interact with the namespace.
    catalog.setDatabaseProperties(TEST_DATABASE, Collections.singletonMap("k", "v"));
  }

  @Test
  public void testRemoveDatabasePropertiesNoOp() {
    catalog.removeDatabaseProperties(TEST_DATABASE, Collections.singleton("k"));
  }
}
