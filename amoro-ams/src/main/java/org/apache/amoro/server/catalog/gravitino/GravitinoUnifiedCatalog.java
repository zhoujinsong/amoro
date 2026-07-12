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

import static org.apache.amoro.server.catalog.gravitino.GravitinoUnifiedCatalogProperties.GRAVITINO_URI;
import static org.apache.amoro.server.catalog.gravitino.GravitinoUnifiedCatalogProperties.ICEBERG_REST_URI;
import static org.apache.amoro.server.catalog.gravitino.GravitinoUnifiedCatalogProperties.LANCE_REST_URI;
import static org.apache.amoro.server.catalog.gravitino.GravitinoUnifiedCatalogProperties.WAREHOUSE;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.FormatCatalog;
import org.apache.amoro.FormatCatalogFactory;
import org.apache.amoro.NoSuchDatabaseException;
import org.apache.amoro.NoSuchTableException;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableIDWithFormat;
import org.apache.amoro.UnifiedCatalog;
import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.properties.CatalogMetaProperties;
import org.apache.amoro.shade.guava32.com.google.common.annotations.VisibleForTesting;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.TableMetaStore;
import org.apache.amoro.utils.CatalogUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.GravitinoClientBase;
import org.apache.gravitino.rel.Table;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class GravitinoUnifiedCatalog implements UnifiedCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoUnifiedCatalog.class);

  /** Table property key carrying the table format (mirrors Gravitino's PROPERTY_TABLE_FORMAT). */
  private static final String PROPERTY_FORMAT = "format";

  /** Legacy/alias table property key for the table format. */
  private static final String PROPERTY_TABLE_FORMAT = "table-format";

  private final String catalogName;
  private final Supplier<CatalogMeta> metaSupplier;
  private final Map<String, String> clientProperties;

  private Map<String, String> catalogProperties;
  private TableMetaStore tableMetaStore;
  /** Official Gravitino Java client, used directly for all metadata discovery. */
  private GravitinoClient client;

  private String warehouse;
  private String metalake;
  private String gravitinoCatalogName;
  private Set<TableFormat> allowedFormats;

  /** format -&gt; data-plane {@link FormatCatalog}, lazily (re)built by {@link #refresh()}. */
  private volatile Map<TableFormat, FormatCatalog> dataPlaneEngines = Maps.newHashMap();

  /** Constructor used when the catalog is refreshable from a live {@link CatalogMeta} source. */
  public GravitinoUnifiedCatalog(
      Supplier<CatalogMeta> catalogMetaSupplier, Map<String, String> clientProperties) {
    this.metaSupplier = catalogMetaSupplier;
    this.clientProperties = clientProperties == null ? Maps.newHashMap() : clientProperties;
    CatalogMeta meta = catalogMetaSupplier.get();
    CatalogUtil.mergeCatalogProperties(meta, this.clientProperties);
    this.catalogName = meta.getCatalogName();
    this.catalogProperties = meta.getCatalogProperties();
    this.tableMetaStore = CatalogUtil.buildMetaStore(meta);
    init();
  }

  /** Constructor used when properties/metastore are already resolved (e.g. client-side loading). */
  public GravitinoUnifiedCatalog(
      String catalogName, Map<String, String> properties, TableMetaStore tableMetaStore) {
    this.metaSupplier = null;
    this.clientProperties = Maps.newHashMap();
    this.catalogName = catalogName;
    this.catalogProperties = properties;
    this.tableMetaStore = tableMetaStore;
    init();
  }

  @VisibleForTesting
  GravitinoUnifiedCatalog(
      String catalogName,
      Map<String, String> properties,
      TableMetaStore tableMetaStore,
      GravitinoClient testClient) {
    this.metaSupplier = null;
    this.clientProperties = Maps.newHashMap();
    this.catalogName = catalogName;
    this.catalogProperties = properties;
    this.tableMetaStore = tableMetaStore;
    this.client = testClient;
    this.warehouse = properties.get(WAREHOUSE);
    Pair<String, String> mc = GravitinoUnifiedCatalogProperties.splitWarehouse(warehouse);
    this.metalake = mc.first();
    this.gravitinoCatalogName = mc.second();
    this.allowedFormats =
        CatalogUtil.tableFormats(CatalogMetaProperties.CATALOG_TYPE_GRAVITINO, properties);
    initializeDataPlaneEngines();
  }

  private void init() {
    this.warehouse = requireProperty(catalogProperties, WAREHOUSE);
    String uri = requireProperty(catalogProperties, GRAVITINO_URI);
    Pair<String, String> mc = GravitinoUnifiedCatalogProperties.splitWarehouse(warehouse);
    this.metalake = mc.first();
    this.gravitinoCatalogName = mc.second();
    this.client = buildClient(uri, metalake, catalogProperties);
    this.allowedFormats =
        CatalogUtil.tableFormats(CatalogMetaProperties.CATALOG_TYPE_GRAVITINO, catalogProperties);
    initializeDataPlaneEngines();
  }

  private static GravitinoClient buildClient(
      String uri, String metalake, Map<String, String> catalogProperties) {
    GravitinoClientBase.Builder<GravitinoClient> builder =
        GravitinoClient.builder(uri)
            .withMetalake(metalake)
            .withHeaders(GravitinoUnifiedCatalogProperties.extractHeaders(catalogProperties));
    if (versionCheckDisabled(catalogProperties)) {
      return builder.withVersionCheckDisabled().build();
    }
    return builder.build();
  }

  private static boolean versionCheckDisabled(Map<String, String> catalogProperties) {
    return catalogProperties != null
        && "false"
            .equalsIgnoreCase(
                catalogProperties.get(GravitinoUnifiedCatalogProperties.VERSION_CHECK_ENABLED));
  }

  private void initializeDataPlaneEngines() {
    Map<String, String> remoteCatalogProps;
    try {
      remoteCatalogProps = loadCatalogProperties();
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to load Gravitino catalog properties for {}.{}, iceberg/lance rest url "
              + "auto-derivation will be skipped; explicit iceberg-rest-uri / "
              + "lance-rest-uri must be configured.",
          metalake,
          gravitinoCatalogName,
          e);
      remoteCatalogProps = Maps.newHashMap();
    }

    String icebergRestUri =
        resolveRestUri(
            "iceberg",
            ICEBERG_REST_URI,
            remoteCatalogProps,
            GravitinoUnifiedCatalogProperties::deriveIcebergRestUri);
    String lanceRestUri =
        resolveRestUri(
            "lance",
            LANCE_REST_URI,
            remoteCatalogProps,
            GravitinoUnifiedCatalogProperties::deriveLanceRestUri);
    LOG.info(
        "Initializing data-plane engines for Gravitino catalog {}.{}: iceberg-rest-uri=[{}], "
            + "lance-rest-uri=[{}], allowedFormats={}",
        metalake,
        gravitinoCatalogName,
        icebergRestUri,
        lanceRestUri,
        allowedFormats);

    Map<TableFormat, FormatCatalog> engines = Maps.newHashMap();
    ServiceLoader<FormatCatalogFactory> loader = ServiceLoader.load(FormatCatalogFactory.class);
    for (FormatCatalogFactory factory : loader) {
      TableFormat format = factory.format();
      if (!allowedFormats.contains(format)) {
        continue;
      }
      // First registered factory per format wins: ServiceLoader iterates in classpath order, so a
      // single factory is expected per format in production. Skipping already-resolved formats also
      // avoids eagerly invoking a second factory's create() (e.g. a real factory shadowing a test
      // fake), which may open network connections.
      if (engines.containsKey(format)) {
        continue;
      }
      Optional<Map<String, String>> derivedProperties =
          deriveDataPlaneProperties(format, icebergRestUri, lanceRestUri);
      if (derivedProperties.isEmpty()) {
        LOG.warn(
            "Skip initializing data-plane engine for format {} in Gravitino catalog {}.{}: "
                + "no REST endpoint resolved. iceberg-rest-uri=[{}], "
                + "lance-rest-uri=[{}]. Configure iceberg-rest-uri / "
                + "lance-rest-uri explicitly, or ensure the Gravitino catalog advertises "
                + "them, or set gravitino.uri so it can be derived "
                + "(host:9001/iceberg, host:9002/lance).",
            format,
            metalake,
            gravitinoCatalogName,
            icebergRestUri,
            lanceRestUri);
        continue;
      }
      try {
        Map<String, String> formatProps =
            factory.convertCatalogProperties(
                catalogName, CatalogMetaProperties.CATALOG_TYPE_CUSTOM, derivedProperties.get());
        FormatCatalog catalog =
            factory.create(
                catalogName,
                CatalogMetaProperties.CATALOG_TYPE_CUSTOM,
                formatProps,
                tableMetaStore);
        engines.put(format, catalog);
      } catch (Exception e) {
        LOG.warn(
            "Failed to initialize data-plane engine for format {} in Gravitino catalog {}.{} "
                + "with properties {}",
            format,
            metalake,
            gravitinoCatalogName,
            derivedProperties.orElse(Maps.newHashMap()),
            e);
      }
    }
    this.dataPlaneEngines = engines;
  }

  @VisibleForTesting
  Optional<Map<String, String>> deriveDataPlaneProperties(
      TableFormat format, String icebergRestUri, String lanceRestUri) {
    Map<String, String> props = Maps.newHashMap(catalogProperties);
    props.remove(GRAVITINO_URI);

    if (format.in(TableFormat.ICEBERG, TableFormat.MIXED_ICEBERG)) {
      if (StringUtils.isBlank(icebergRestUri)) {
        return Optional.empty();
      }
      props.put("uri", stripTrailingSlash(icebergRestUri));
      props.putIfAbsent("catalog-impl", "org.apache.iceberg.rest.RESTCatalog");
      // Iceberg gets the warehouse verbatim ("<metalake>.<catalog>").
      props.putIfAbsent("warehouse", warehouse);
      return Optional.of(props);
    } else if (format.equals(TableFormat.LANCE)) {
      if (StringUtils.isBlank(lanceRestUri)) {
        return Optional.empty();
      }
      // Lance's factory reads `lance.uri` first (explicit override) and splits the raw warehouse
      // ("<metalake>.<catalog>") into the X-Gravitino-Metalake header + REST catalog name.
      props.put("lance.uri", lanceRestUri);
      props.putIfAbsent("warehouse", warehouse);
      return Optional.of(props);
    } else {
      return Optional.empty();
    }
  }

  @Override
  public List<String> listDatabases() {
    try {
      String[] schemas = gravitinoCatalog().asSchemas().listSchemas();
      return schemas == null ? Collections.emptyList() : Arrays.asList(schemas);
    } catch (RuntimeException e) {
      throw new IllegalStateException(
          "Failed to list schemas of catalog " + gravitinoCatalogName, e);
    }
  }

  @Override
  public boolean databaseExists(String database) {
    return listDatabases().contains(database);
  }

  @Override
  public boolean tableExists(String database, String table) {
    return describeTableFormat(database, table).isPresent();
  }

  @Override
  public List<TableIDWithFormat> listTables(String database) {
    List<String> names = listTableNames(database);
    if (names.isEmpty()) {
      return Collections.emptyList();
    }
    return names.stream()
        .map(
            name ->
                describeTableFormat(database, name)
                    .map(rawFormat -> toTableIDWithFormat(database, name, rawFormat))
                    .orElse(null))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private TableIDWithFormat toTableIDWithFormat(String database, String table, String rawFormat) {
    try {
      TableFormat format = fromGravitinoFormat(rawFormat);
      return TableIDWithFormat.of(TableIdentifier.of(catalogName, database, table), format);
    } catch (IllegalArgumentException e) {
      LOG.warn("Skip table {}.{}.{}: {}", catalogName, database, table, e.getMessage());
      return null;
    }
  }

  @Override
  public TableFormat loadTableFormat(String database, String table) {
    String rawFormat =
        describeTableFormat(database, table)
            .orElseThrow(() -> new NoSuchTableException("Table: " + table + " does not exist."));
    return fromGravitinoFormat(rawFormat);
  }

  @Override
  public List<TableIdentifier> listTableIdentifiers(String database) {
    return listTableNames(database).stream()
        .map(name -> TableIdentifier.of(catalogName, database, name))
        .collect(Collectors.toList());
  }

  private Catalog gravitinoCatalog() {
    return client.loadCatalog(gravitinoCatalogName);
  }

  private List<String> listTableNames(String schema) {
    try {
      NameIdentifier[] idents =
          gravitinoCatalog().asTableCatalog().listTables(Namespace.of(schema));
      if (idents == null) {
        return Collections.emptyList();
      }
      return Arrays.stream(idents).map(NameIdentifier::name).collect(Collectors.toList());
    } catch (RuntimeException e) {
      throw new IllegalStateException(
          "Failed to list tables of schema " + gravitinoCatalogName + "." + schema, e);
    }
  }

  private Optional<String> describeTableFormat(String schema, String table) {
    try {
      Table loaded =
          gravitinoCatalog()
              .asTableCatalog()
              .loadTable(NameIdentifier.of(Namespace.of(schema), table));
      String format = extractFormatProperty(loaded.properties());
      return StringUtils.isNotBlank(format) ? Optional.of(format) : Optional.empty();
    } catch (org.apache.gravitino.exceptions.NoSuchTableException e) {
      return Optional.empty();
    } catch (RuntimeException e) {
      throw new IllegalStateException(
          "Failed to load table " + gravitinoCatalogName + "." + schema + "." + table, e);
    }
  }

  /**
   * Loads the target catalog's properties, used to auto-derive {@code iceberg-rest-uri} / {@code
   * lance-rest-uri} when not explicitly configured on the Amoro side.
   */
  private Map<String, String> loadCatalogProperties() {
    try {
      Map<String, String> props = gravitinoCatalog().properties();
      return props == null ? Maps.newHashMap() : Maps.newHashMap(props);
    } catch (RuntimeException e) {
      throw new IllegalStateException(
          "Failed to load properties of catalog " + gravitinoCatalogName, e);
    }
  }

  /** Reads the {@code format} property, falling back to the {@code table-format} alias. */
  @VisibleForTesting
  static String extractFormatProperty(Map<String, String> properties) {
    if (properties == null) {
      return null;
    }
    String format = properties.get(PROPERTY_FORMAT);
    if (StringUtils.isNotBlank(format)) {
      return format;
    }
    return properties.get(PROPERTY_TABLE_FORMAT);
  }

  @VisibleForTesting
  static TableFormat fromGravitinoFormat(String gravitinoFormat) {
    if (StringUtils.isBlank(gravitinoFormat)) {
      throw new IllegalArgumentException("Gravitino table format property must not be blank");
    }
    switch (gravitinoFormat.trim().toLowerCase(Locale.ROOT)) {
      case "iceberg":
        return TableFormat.ICEBERG;
      case "mixed-iceberg":
      case "mixed_iceberg":
        return TableFormat.MIXED_ICEBERG;
      case "lance":
        return TableFormat.LANCE;
      default:
        throw new IllegalArgumentException(
            "Unrecognized Gravitino table format: "
                + gravitinoFormat
                + ". If this is a newly supported format on the Gravitino side, add a mapping "
                + "here and ensure a corresponding Amoro FormatCatalogFactory exists.");
    }
  }

  @Override
  public AmoroTable<?> loadTable(String database, String table) {
    TableFormat format = loadTableFormat(database, table);
    FormatCatalog engine = dataPlaneEngines.get(format);
    if (engine == null) {
      throw new IllegalStateException(
          "No data-plane engine configured for format "
              + format
              + " in Gravitino catalog "
              + metalake
              + "."
              + gravitinoCatalogName
              + ". Check `table-formats`, or iceberg-rest-uri / "
              + "lance-rest-uri.");
    }
    return engine.loadTable(database, table);
  }

  @Override
  public boolean dropTable(String database, String table, boolean purge) {
    try {
      TableFormat format = loadTableFormat(database, table);
      FormatCatalog engine = dataPlaneEngines.get(format);
      return engine != null && engine.dropTable(database, table, purge);
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  @Override
  public void createDatabase(String database) {
    anyDataPlaneEngine().createDatabase(database);
  }

  @Override
  public void createDatabase(String database, Map<String, String> metadata) {
    anyDataPlaneEngine().createDatabase(database, metadata);
  }

  @Override
  public Map<String, String> loadDatabaseMetadata(String database) {
    return anyDataPlaneEngine().loadDatabaseMetadata(database);
  }

  @Override
  public void dropDatabase(String database) {
    anyDataPlaneEngine().dropDatabase(database);
  }

  @Override
  public void setDatabaseProperties(String database, Map<String, String> properties)
      throws NoSuchDatabaseException {
    anyDataPlaneEngine().setDatabaseProperties(database, properties);
  }

  @Override
  public void removeDatabaseProperties(String database, Set<String> properties)
      throws NoSuchDatabaseException {
    anyDataPlaneEngine().removeDatabaseProperties(database, properties);
  }

  /**
   * Picks a deterministic data-plane engine to perform format-agnostic schema operations (schemas
   * are a single, format-independent namespace in Gravitino). Prefers ICEBERG for stability across
   * calls.
   */
  private FormatCatalog anyDataPlaneEngine() {
    return dataPlaneEngines.getOrDefault(
        TableFormat.ICEBERG,
        dataPlaneEngines.values().stream()
            .min(Comparator.comparing(Object::toString))
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "No data-plane engine available for Gravitino catalog "
                            + metalake
                            + "."
                            + gravitinoCatalogName)));
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public String metastoreType() {
    return CatalogMetaProperties.CATALOG_TYPE_GRAVITINO;
  }

  @Override
  public TableMetaStore authenticationContext() {
    return tableMetaStore;
  }

  @Override
  public Map<String, String> properties() {
    return catalogProperties;
  }

  @Override
  public synchronized void refresh() {
    if (metaSupplier == null) {
      return;
    }
    CatalogMeta newMeta = metaSupplier.get();
    CatalogUtil.mergeCatalogProperties(newMeta, clientProperties);
    this.catalogProperties = newMeta.getCatalogProperties();
    this.tableMetaStore = CatalogUtil.buildMetaStore(newMeta);
    init();
  }

  private static String requireProperty(Map<String, String> properties, String key) {
    String value = properties.get(key);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(value), "Gravitino unified catalog property '%s' is required", key);
    return value;
  }

  private static String firstNonBlank(String a, String b) {
    return StringUtils.isNotBlank(a) ? a : b;
  }

  private String resolveRestUri(
      String formatLabel,
      String restUriKey,
      Map<String, String> remoteCatalogProps,
      Function<String, String> deriveFromGravitinoUri) {
    String uri =
        firstNonBlank(catalogProperties.get(restUriKey), remoteCatalogProps.get(restUriKey));
    // Fallback: derive from gravitino.uri when neither explicit config nor the remote catalog
    // property provided the endpoint.
    if (StringUtils.isBlank(uri)) {
      uri = deriveFromGravitinoUri.apply(catalogProperties.get(GRAVITINO_URI));
      if (StringUtils.isNotBlank(uri)) {
        LOG.info(
            "{}_rest_server_url not found in config or remote catalog; derived from "
                + "gravitino.uri: {}",
            formatLabel,
            uri);
      }
    }
    return uri;
  }

  private static String stripTrailingSlash(String uri) {
    if (uri == null) {
      return null;
    }
    while (uri.endsWith("/")) {
      uri = uri.substring(0, uri.length() - 1);
    }
    return uri;
  }
}
