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

import org.apache.amoro.FormatCatalog;
import org.apache.amoro.FormatCatalogFactory;
import org.apache.amoro.TableFormat;
import org.apache.amoro.properties.CatalogMetaProperties;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.table.TableMetaStore;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.lang3.StringUtils;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.hive3.Hive3Namespace;
import org.lance.namespace.hive3.Hive3NamespaceConfig;

import java.util.Map;

public class LanceCatalogFactory implements FormatCatalogFactory {

  /** Metastore type that maps to a REST-based Lance namespace. */
  private static final String CATALOG_TYPE_REST = "rest";

  /** Prefix for Lance-only catalog properties; stripped before reaching the Lance namespace. */
  private static final String LANCE_PROPERTY_PREFIX = "lance.";

  /** HTTP header prefix (Iceberg REST convention) reused for the Lance REST client. */
  private static final String HEADER_PROPERTY_PREFIX = "header.";

  /** Property key for the Lance REST endpoint (after the {@code lance.} prefix is stripped). */
  private static final String URI_KEY = "uri";

  /** Gravitino metalake header. Carries the metalake parsed from {@code warehouse}. */
  private static final String GRAVITINO_METALAKE_HEADER =
      HEADER_PROPERTY_PREFIX + "X-Gravitino-Metalake";

  /** Default Iceberg→Lance endpoint token rewrite used when {@code lance.uri} is absent. */
  private static final String ICEBERG_ENDPOINT_TOKEN = "9001/iceberg";

  private static final String LANCE_ENDPOINT_TOKEN = "9002/lance";

  private static final String ICEBERG_PATH_SUFFIX = "/iceberg";
  private static final String LANCE_PATH_SUFFIX = "/lance";

  static {
    LanceNamespace.registerNamespaceImpl("hive3", "org.lance.namespace.hive3.Hive3Namespace");
  }

  @Override
  public FormatCatalog create(
      String catalogName,
      String metastoreType,
      Map<String, String> properties,
      TableMetaStore metaStore) {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    // A REST-based Lance namespace is used for "rest" / "custom" catalogs (the Gravitino/TCLake
    // shared-catalog scenario), so Iceberg-REST + Lance-REST can live in one catalog without
    // touching the Iceberg side. hadoop/hive keep their directory/HMS namespaces.
    if (isRestMetastore(metastoreType)) {
      Map<String, String> lanceRestProperties = buildLanceRestProperties(properties);
      LanceNamespace namespace = LanceNamespace.connect("rest", lanceRestProperties, allocator);
      return new LanceRestCatalog(namespace, lanceRestProperties);
    }

    Map<String, String> lanceCatalogProperties = Maps.newHashMap(properties);
    String warehouse = lanceCatalogProperties.get(CatalogMetaProperties.KEY_WAREHOUSE);
    if (warehouse != null && !warehouse.isEmpty()) {
      lanceCatalogProperties.putIfAbsent(
          Hive3NamespaceConfig.ROOT, LanceProperties.normalizeRootUrl(warehouse));
    }

    if (CatalogMetaProperties.CATALOG_TYPE_HADOOP.equalsIgnoreCase(metastoreType)) {
      LanceNamespace namespace = LanceNamespace.connect("dir", lanceCatalogProperties, allocator);
      return new LanceDirectoryCatalog(namespace, lanceCatalogProperties, allocator);
    } else if (CatalogMetaProperties.CATALOG_TYPE_HIVE.equalsIgnoreCase(metastoreType)) {
      Hive3Namespace namespace = new Hive3Namespace();
      namespace.setHadoopConf(metaStore.getConfiguration());
      namespace.initialize(lanceCatalogProperties, allocator);
      return new LanceHiveCatalog(
          catalogName, namespace, lanceCatalogProperties, allocator, metaStore.getConfiguration());
    }
    throw new IllegalArgumentException(
        "Un-supported metastore type:" + metastoreType + " for Lance");
  }

  /**
   * REST-style Lance namespaces are selected by the {@code rest} or {@code custom} catalog type.
   */
  private static boolean isRestMetastore(String metastoreType) {
    return CATALOG_TYPE_REST.equalsIgnoreCase(metastoreType)
        || CatalogMetaProperties.CATALOG_TYPE_CUSTOM.equalsIgnoreCase(metastoreType);
  }

  /**
   * Build the property map handed to the Lance REST namespace, fully isolated from Iceberg:
   *
   * <ol>
   *   <li>every {@code lance.<key>} entry is stripped of the {@code lance.} prefix (so {@code
   *       lance.uri} -> {@code uri}); these always win over derived values;
   *   <li>catalog-level {@code header.*} (e.g. {@code header.Authorization}) is inherited as the
   *       Lance REST client headers (auth reuse);
   *   <li>{@code warehouse = <metalake>.<catalog>} is reused: the metalake becomes the {@code
   *       header.X-Gravitino-Metalake} header and the catalog becomes the top-level namespace name
   *       ({@link LanceRestCatalog#REST_CATALOG_NAME});
   *   <li>if no {@code uri} was provided via {@code lance.uri}, it is derived from the shared
   *       {@code uri} by rewriting the endpoint token (default {@code 9001/iceberg} -> {@code
   *       9002/lance}).
   * </ol>
   */
  static Map<String, String> buildLanceRestProperties(Map<String, String> properties) {
    Map<String, String> out = Maps.newHashMap();
    if (properties == null) {
      return out;
    }
    // 1. lance.* -> strip prefix (explicit overrides)
    properties.forEach(
        (k, v) -> {
          if (k != null && k.startsWith(LANCE_PROPERTY_PREFIX)) {
            out.put(k.substring(LANCE_PROPERTY_PREFIX.length()), v);
          }
        });
    // 2. reuse catalog-level header.* (e.g. header.Authorization) for REST auth
    properties.forEach(
        (k, v) -> {
          if (k != null && k.startsWith(HEADER_PROPERTY_PREFIX)) {
            out.putIfAbsent(k, v);
          }
        });
    // 3. reuse warehouse = <metalake>.<catalog>
    String warehouse = properties.get(CatalogMetaProperties.KEY_WAREHOUSE);
    if (StringUtils.isNotBlank(warehouse)) {
      int idx = warehouse.indexOf('.');
      if (idx > 0) {
        String metalake = warehouse.substring(0, idx);
        String catalog = warehouse.substring(idx + 1);
        if (StringUtils.isNotBlank(metalake)) {
          out.putIfAbsent(GRAVITINO_METALAKE_HEADER, metalake);
        }
        if (StringUtils.isNotBlank(catalog)) {
          out.putIfAbsent(LanceRestCatalog.REST_CATALOG_NAME, catalog);
        }
      } else {
        out.putIfAbsent(LanceRestCatalog.REST_CATALOG_NAME, warehouse);
      }
    }
    // 4. uri: lance.uri wins; otherwise derive from the shared (iceberg) uri.
    if (StringUtils.isBlank(out.get(URI_KEY))) {
      String sharedUri = properties.get(URI_KEY);
      if (StringUtils.isNotBlank(sharedUri)) {
        out.put(URI_KEY, stripTrailingSlash(deriveLanceUri(sharedUri)));
      }
    } else {
      out.put(URI_KEY, stripTrailingSlash(out.get(URI_KEY)));
    }
    return out;
  }

  /**
   * Removes trailing {@code /} characters so the Rust SDK's {@code uri + "/v1/..."} join is clean.
   */
  private static String stripTrailingSlash(String uri) {
    if (uri == null) {
      return null;
    }
    while (uri.endsWith("/")) {
      uri = uri.substring(0, uri.length() - 1);
    }
    return uri;
  }

  /**
   * Derive the Lance REST endpoint from the Iceberg REST endpoint. Primary rule rewrites the
   * documented Gravitino/TCLake token {@code 9001/iceberg} -> {@code 9002/lance}; otherwise the
   * trailing {@code /iceberg} path segment is rewritten to {@code /lance}. When neither matches the
   * input is returned unchanged (configure {@code lance.uri} explicitly in that case).
   */
  static String deriveLanceUri(String sharedUri) {
    if (StringUtils.isBlank(sharedUri)) {
      return sharedUri;
    }
    String rewritten = sharedUri.replace(ICEBERG_ENDPOINT_TOKEN, LANCE_ENDPOINT_TOKEN);
    if (!rewritten.equals(sharedUri)) {
      return rewritten;
    }
    if (sharedUri.endsWith(ICEBERG_PATH_SUFFIX)) {
      return sharedUri.substring(0, sharedUri.length() - ICEBERG_PATH_SUFFIX.length())
          + LANCE_PATH_SUFFIX;
    }
    return sharedUri;
  }

  @Override
  public TableFormat format() {
    return TableFormat.LANCE;
  }

  @Override
  public Map<String, String> convertCatalogProperties(
      String catalogName, String metastoreType, Map<String, String> unifiedCatalogProperties) {
    return unifiedCatalogProperties;
  }
}
