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

package org.apache.amoro.server.catalog.gravitino;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.NoSuchTableException;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableIDWithFormat;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.TableMetaStore;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TestGravitinoUnifiedCatalog {

  private static final String CATALOG_NAME = "test_gravitino_catalog";
  private static final TableMetaStore META_STORE = TableMetaStore.builder().build();

  private GravitinoClient client;
  private Catalog gravitinoCatalog;
  private SupportsSchemas schemas;
  private TableCatalog tableCatalog;
  private Map<String, String> remoteCatalogProperties;

  @Before
  public void setUp() {
    client = mock(GravitinoClient.class);
    gravitinoCatalog = mock(Catalog.class);
    schemas = mock(SupportsSchemas.class);
    tableCatalog = mock(TableCatalog.class);
    remoteCatalogProperties = Maps.newHashMap();

    lenient().when(client.loadCatalog(anyString())).thenReturn(gravitinoCatalog);
    lenient().when(gravitinoCatalog.asSchemas()).thenReturn(schemas);
    lenient().when(gravitinoCatalog.asTableCatalog()).thenReturn(tableCatalog);
    lenient().when(gravitinoCatalog.properties()).thenReturn(remoteCatalogProperties);
    lenient().when(schemas.listSchemas()).thenReturn(new String[0]);
    // Default: any table load reports "table missing" (empty format). Specific tables are stubbed
    // via stubTable(...). Build the mock into a local first to avoid nested stubbing.
    Table missingTable = tableWithFormat(null);
    lenient().when(tableCatalog.loadTable(any())).thenReturn(missingTable);
  }

  private Map<String, String> baseProperties() {
    Map<String, String> props = Maps.newHashMap();
    props.put(GravitinoUnifiedCatalogProperties.GRAVITINO_URI, "http://localhost:8090");
    // warehouse = "<metalake>.<catalog>"
    props.put(GravitinoUnifiedCatalogProperties.WAREHOUSE, "metalake1.lakehouse1");
    return props;
  }

  private GravitinoUnifiedCatalog newCatalog(Map<String, String> extraProperties) {
    Map<String, String> props = baseProperties();
    props.putAll(extraProperties);
    return new GravitinoUnifiedCatalog(CATALOG_NAME, props, META_STORE, client);
  }

  private static Table tableWithFormat(String format) {
    Table table = mock(Table.class);
    Map<String, String> props = Maps.newHashMap();
    if (format != null) {
      props.put("format", format);
    }
    lenient().when(table.properties()).thenReturn(props);
    return table;
  }

  /** Stubs a single table's format via {@code loadTable(schema.table)}. */
  private void stubTable(String schema, String table, String format) {
    // Build the mock into a local first to avoid nested stubbing.
    Table stubbed = tableWithFormat(format);
    when(tableCatalog.loadTable(NameIdentifier.of(Namespace.of(schema), table)))
        .thenReturn(stubbed);
  }

  /** Stubs the list of table names in a schema. */
  private void stubTableNames(String schema, String... tables) {
    NameIdentifier[] idents = new NameIdentifier[tables.length];
    for (int i = 0; i < tables.length; i++) {
      idents[i] = NameIdentifier.of(Namespace.of(schema), tables[i]);
    }
    when(tableCatalog.listTables(Namespace.of(schema))).thenReturn(idents);
  }

  // -----------------------------------------------------------------------------------------
  // Discovery layer: talks only to Gravitino
  // -----------------------------------------------------------------------------------------

  @Test
  public void testListDatabasesDelegatesToGravitinoSingleCall() {
    when(schemas.listSchemas()).thenReturn(new String[] {"db1", "db2"});
    GravitinoUnifiedCatalog catalog = newCatalog(Maps.newHashMap());

    List<String> databases = catalog.listDatabases();

    assertEquals(Lists.newArrayList("db1", "db2"), databases);
    verify(schemas, times(1)).listSchemas();
  }

  @Test
  public void testListTablesReturnsCrossFormatResultsInOneRoundTrip() {
    stubTableNames("db1", "t_iceberg", "t_lance");
    stubTable("db1", "t_iceberg", "iceberg");
    stubTable("db1", "t_lance", "lance");
    GravitinoUnifiedCatalog catalog = newCatalog(Maps.newHashMap());

    List<TableIDWithFormat> tables = catalog.listTables("db1");

    assertEquals(2, tables.size());
    Map<String, TableFormat> byName = Maps.newHashMap();
    tables.forEach(t -> byName.put(t.table(), t.getTableFormat()));
    assertEquals(TableFormat.ICEBERG, byName.get("t_iceberg"));
    assertEquals(TableFormat.LANCE, byName.get("t_lance"));

    // Exactly one "list names" round trip; formats resolved per table thereafter.
    verify(tableCatalog, times(1)).listTables(Namespace.of("db1"));
  }

  @Test
  public void testListTablesEmptySchemaReturnsEmpty() {
    stubTableNames("db-empty");
    GravitinoUnifiedCatalog catalog = newCatalog(Maps.newHashMap());

    List<TableIDWithFormat> tables = catalog.listTables("db-empty");

    assertTrue(tables.isEmpty());
    verify(tableCatalog, times(1)).listTables(Namespace.of("db-empty"));
  }

  @Test
  public void testListTableIdentifiersDoesNotResolveFormats() {
    stubTableNames("db1", "t_iceberg", "t_lance");
    // Intentionally do NOT stub formats: listTableIdentifiers must not load any table.
    GravitinoUnifiedCatalog catalog = newCatalog(Maps.newHashMap());

    List<TableIdentifier> identifiers = catalog.listTableIdentifiers("db1");

    assertEquals(2, identifiers.size());
    Map<String, TableIdentifier> byName = Maps.newHashMap();
    identifiers.forEach(id -> byName.put(id.getTableName(), id));
    assertTrue(byName.containsKey("t_iceberg"));
    assertTrue(byName.containsKey("t_lance"));
    assertEquals(CATALOG_NAME, byName.get("t_iceberg").getCatalog());
    assertEquals("db1", byName.get("t_iceberg").getDatabase());

    // Core performance guarantee: a single "list names" round trip, and NO per-table loadTable.
    verify(tableCatalog, times(1)).listTables(Namespace.of("db1"));
    verify(tableCatalog, never()).loadTable(any());
  }

  @Test
  public void testListTablesSkipsUnrecognizedFormatGracefully() {
    stubTableNames("db1", "t_ok", "t_unknown");
    stubTable("db1", "t_ok", "iceberg");
    stubTable("db1", "t_unknown", "delta"); // not yet supported
    GravitinoUnifiedCatalog catalog = newCatalog(Maps.newHashMap());

    List<TableIDWithFormat> tables = catalog.listTables("db1");

    assertEquals(1, tables.size());
    assertEquals("t_ok", tables.get(0).table());
  }

  @Test
  public void testLoadTableFormatThrowsNoSuchTableExceptionWhenMissing() {
    GravitinoUnifiedCatalog catalog = newCatalog(Maps.newHashMap());
    assertThrows(
        NoSuchTableException.class, () -> catalog.loadTableFormat("db1", "does_not_exist"));
  }

  @Test
  public void testTableExists() {
    stubTable("db1", "t1", "iceberg");
    GravitinoUnifiedCatalog catalog = newCatalog(Maps.newHashMap());

    assertTrue(catalog.tableExists("db1", "t1"));
    assertFalse(catalog.tableExists("db1", "t_missing"));
  }

  // -----------------------------------------------------------------------------------------
  // Data-plane layer: routes to the correct FormatCatalog once the format is known
  // -----------------------------------------------------------------------------------------

  @Test
  public void testLoadTableRoutesToIcebergEngine() {
    stubTable("db1", "t1", "iceberg");
    Map<String, String> extra = Maps.newHashMap();
    extra.put(
        GravitinoUnifiedCatalogProperties.ICEBERG_REST_SERVER_URL, "http://host:9001/iceberg/");
    GravitinoUnifiedCatalog catalog = newCatalog(extra);

    AmoroTable<?> table = catalog.loadTable("db1", "t1");

    assertEquals(TableFormat.ICEBERG, table.format());
  }

  @Test
  public void testLoadTableRoutesToLanceEngine() {
    stubTable("db1", "t1", "lance");
    Map<String, String> extra = Maps.newHashMap();
    extra.put(GravitinoUnifiedCatalogProperties.LANCE_REST_SERVER_URL, "http://host:9002/lance/");
    GravitinoUnifiedCatalog catalog = newCatalog(extra);

    AmoroTable<?> table = catalog.loadTable("db1", "t1");

    assertEquals(TableFormat.LANCE, table.format());
  }

  @Test
  public void testLanceRestUriAutoDerivedFromGravitinoUri() {
    stubTable("db1", "t1", "lance");
    // Neither lance_rest_server_url nor a remote catalog property is configured: the Lance engine
    // is still built by deriving the endpoint from gravitino.uri (same host, port 9002, /lance).
    GravitinoUnifiedCatalog catalog = newCatalog(Maps.newHashMap());

    AmoroTable<?> table = catalog.loadTable("db1", "t1");

    assertEquals(TableFormat.LANCE, table.format());
    assertEquals(
        "http://localhost:9002/lance",
        FakeLanceCatalogFactory.getLastReceivedProperties().get().get("lance.uri"));
  }

  @Test
  public void testLoadTableThrowsWhenEndpointUnresolvable() {
    stubTable("db1", "t1", "lance");
    // No lance_rest_server_url, no remote catalog property, and gravitino.uri has no host, so the
    // endpoint cannot be resolved and no data-plane engine gets built for LANCE.
    Map<String, String> extra = Maps.newHashMap();
    extra.put(GravitinoUnifiedCatalogProperties.GRAVITINO_URI, "invalid-uri-without-host");
    GravitinoUnifiedCatalog catalog = newCatalog(extra);

    assertThrows(IllegalStateException.class, () -> catalog.loadTable("db1", "t1"));
  }

  @Test
  public void testDropTableRoutesByFormat() {
    stubTable("db1", "t1", "iceberg");
    Map<String, String> extra = Maps.newHashMap();
    extra.put(
        GravitinoUnifiedCatalogProperties.ICEBERG_REST_SERVER_URL, "http://host:9001/iceberg/");
    GravitinoUnifiedCatalog catalog = newCatalog(extra);

    assertTrue(catalog.dropTable("db1", "t1", false));
  }

  @Test
  public void testDropTableReturnsFalseWhenTableMissing() {
    GravitinoUnifiedCatalog catalog = newCatalog(Maps.newHashMap());
    assertFalse(catalog.dropTable("db1", "does_not_exist", false));
  }

  // -----------------------------------------------------------------------------------------
  // Endpoint auto-derivation and property translation
  // -----------------------------------------------------------------------------------------

  @Test
  public void testIcebergRestUriAutoDerivedFromRemoteCatalogProperties() {
    remoteCatalogProperties.put("iceberg-rest-uri", "http://auto-derived:9001/iceberg/");
    stubTable("db1", "t1", "iceberg");
    GravitinoUnifiedCatalog catalog = newCatalog(Maps.newHashMap());

    // No explicit iceberg_rest_server_url configured: the engine still gets built because the
    // Gravitino loadCatalog() response carried it. Trailing '/' is stripped to prevent
    // double-slash in REST URL joins.
    AmoroTable<?> table = catalog.loadTable("db1", "t1");
    assertEquals(TableFormat.ICEBERG, table.format());
    assertEquals(
        "http://auto-derived:9001/iceberg",
        FakeIcebergCatalogFactory.getLastReceivedProperties().get().get("uri"));
  }

  @Test
  public void testExplicitIcebergRestUriOverridesRemote() {
    remoteCatalogProperties.put("iceberg-rest-uri", "http://remote:9001/iceberg/");
    Map<String, String> extra = Maps.newHashMap();
    extra.put(
        GravitinoUnifiedCatalogProperties.ICEBERG_REST_SERVER_URL, "http://explicit:9001/iceberg/");
    newCatalog(extra);

    assertEquals(
        "http://explicit:9001/iceberg",
        FakeIcebergCatalogFactory.getLastReceivedProperties().get().get("uri"));
  }

  @Test
  public void testDeriveDataPlanePropertiesForIceberg() {
    GravitinoUnifiedCatalog catalog = newCatalog(Maps.newHashMap());

    Optional<Map<String, String>> derived =
        catalog.deriveDataPlaneProperties(TableFormat.ICEBERG, "http://host:9001/iceberg/", null);

    assertTrue(derived.isPresent());
    assertEquals("http://host:9001/iceberg", derived.get().get("uri"));
    assertEquals("org.apache.iceberg.rest.RESTCatalog", derived.get().get("catalog-impl"));
    assertEquals("metalake1.lakehouse1", derived.get().get("warehouse"));
  }

  @Test
  public void testDeriveDataPlanePropertiesForLance() {
    GravitinoUnifiedCatalog catalog = newCatalog(Maps.newHashMap());

    Optional<Map<String, String>> derived =
        catalog.deriveDataPlaneProperties(TableFormat.LANCE, null, "http://host:9002/lance/");

    assertTrue(derived.isPresent());
    assertEquals("http://host:9002/lance/", derived.get().get("lance.uri"));
    assertEquals("metalake1.lakehouse1", derived.get().get("warehouse"));
  }

  @Test
  public void testDeriveDataPlanePropertiesReturnsEmptyWhenUriUnresolvable() {
    GravitinoUnifiedCatalog catalog = newCatalog(Maps.newHashMap());

    assertFalse(catalog.deriveDataPlaneProperties(TableFormat.ICEBERG, null, null).isPresent());
    assertFalse(catalog.deriveDataPlaneProperties(TableFormat.LANCE, null, null).isPresent());
  }

  @Test
  public void testDeriveDataPlanePropertiesPassesThroughHeaders() {
    Map<String, String> extra = Maps.newHashMap();
    extra.put("header.Authorization", "Basic xxx");
    extra.put("header.X-VPC-ID", "vpc-123");
    GravitinoUnifiedCatalog catalog = newCatalog(extra);

    Optional<Map<String, String>> derived =
        catalog.deriveDataPlaneProperties(TableFormat.ICEBERG, "http://host:9001/iceberg/", null);

    assertEquals("Basic xxx", derived.get().get("header.Authorization"));
    assertEquals("vpc-123", derived.get().get("header.X-VPC-ID"));
  }

  @Test
  public void testUnknownFormatHasNoDataPlaneMapping() {
    GravitinoUnifiedCatalog catalog = newCatalog(Maps.newHashMap());
    assertFalse(
        catalog.deriveDataPlaneProperties(TableFormat.PAIMON, "http://x", "http://y").isPresent());
  }

  @Test
  public void testMetastoreTypeAndName() {
    GravitinoUnifiedCatalog catalog = newCatalog(Maps.newHashMap());
    assertEquals(CATALOG_NAME, catalog.name());
    assertEquals("gravitino", catalog.metastoreType());
  }

  @Test
  public void testRequiredPropertiesValidated() {
    Map<String, String> missing = Maps.newHashMap();
    missing.put(GravitinoUnifiedCatalogProperties.GRAVITINO_URI, "http://localhost:8090");
    // warehouse missing
    assertThrows(
        IllegalArgumentException.class,
        () -> new GravitinoUnifiedCatalog(CATALOG_NAME, missing, META_STORE));
  }

  @Test
  public void testInvalidWarehouseFormatRejected() {
    Map<String, String> props = Maps.newHashMap();
    props.put(GravitinoUnifiedCatalogProperties.GRAVITINO_URI, "http://localhost:8090");
    // warehouse without a '.' separator is invalid (must be "<metalake>.<catalog>")
    props.put(GravitinoUnifiedCatalogProperties.WAREHOUSE, "no_dot_here");
    assertThrows(
        IllegalArgumentException.class,
        () -> new GravitinoUnifiedCatalog(CATALOG_NAME, props, META_STORE));
  }

  // -----------------------------------------------------------------------------------------
  // Inlined helpers: format mapping / format property extraction / header extraction
  // -----------------------------------------------------------------------------------------

  @Test
  public void testFromGravitinoFormat() {
    assertEquals(TableFormat.ICEBERG, GravitinoUnifiedCatalog.fromGravitinoFormat("iceberg"));
    assertEquals(TableFormat.ICEBERG, GravitinoUnifiedCatalog.fromGravitinoFormat(" ICEBERG "));
    assertEquals(
        TableFormat.MIXED_ICEBERG, GravitinoUnifiedCatalog.fromGravitinoFormat("mixed-iceberg"));
    assertEquals(
        TableFormat.MIXED_ICEBERG, GravitinoUnifiedCatalog.fromGravitinoFormat("mixed_iceberg"));
    assertEquals(TableFormat.LANCE, GravitinoUnifiedCatalog.fromGravitinoFormat("Lance"));
  }

  @Test
  public void testFromGravitinoFormatBlankOrUnknownThrows() {
    assertThrows(
        IllegalArgumentException.class, () -> GravitinoUnifiedCatalog.fromGravitinoFormat(""));
    assertThrows(
        IllegalArgumentException.class, () -> GravitinoUnifiedCatalog.fromGravitinoFormat(null));
    assertThrows(
        IllegalArgumentException.class, () -> GravitinoUnifiedCatalog.fromGravitinoFormat("delta"));
  }

  @Test
  public void testExtractFormatProperty() {
    Map<String, String> preferFormat = Maps.newHashMap();
    preferFormat.put("format", "iceberg");
    preferFormat.put("table-format", "lance");
    assertEquals("iceberg", GravitinoUnifiedCatalog.extractFormatProperty(preferFormat));

    Map<String, String> aliasOnly = Maps.newHashMap();
    aliasOnly.put("table-format", "lance");
    assertEquals("lance", GravitinoUnifiedCatalog.extractFormatProperty(aliasOnly));

    assertNull(GravitinoUnifiedCatalog.extractFormatProperty(Maps.newHashMap()));
    assertNull(GravitinoUnifiedCatalog.extractFormatProperty(null));
  }

  @Test
  public void testExtractHeadersStripsPrefixAndIgnoresNonHeaderProps() {
    Map<String, String> props = Maps.newHashMap();
    props.put("header.Authorization", "Basic xxx");
    props.put("header.X-VPC-ID", "vpc-123");
    props.put("uri", "http://host:8090");
    props.put("warehouse", "metalake1.lakehouse1");
    Map<String, String> headers = GravitinoUnifiedCatalogProperties.extractHeaders(props);
    assertEquals(2, headers.size());
    assertEquals("Basic xxx", headers.get("Authorization"));
    assertEquals("vpc-123", headers.get("X-VPC-ID"));
  }

  @Test
  public void testExtractHeadersEmptyOrNullSafe() {
    Map<String, String> props = Maps.newHashMap();
    props.put("uri", "http://host:8090");
    assertTrue(GravitinoUnifiedCatalogProperties.extractHeaders(props).isEmpty());
    assertTrue(GravitinoUnifiedCatalogProperties.extractHeaders(null).isEmpty());
  }
}
