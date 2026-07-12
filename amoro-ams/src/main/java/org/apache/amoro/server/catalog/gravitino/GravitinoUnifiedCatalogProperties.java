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

import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.util.Pair;

import java.util.Map;

/**
 * Catalog-property key constants consumed by {@link GravitinoUnifiedCatalog}.
 *
 * <p>Design reference: {@code docs/wizzhou/gravitino-unified-catalog-design.md}.
 */
public class GravitinoUnifiedCatalogProperties {

  private GravitinoUnifiedCatalogProperties() {}

  /** Base URI of the Gravitino server, e.g. {@code http://gravitino-host:8090}. Required. */
  public static final String GRAVITINO_URI = "uri";

  /** The target catalog, expressed as {@code <metalake>.<catalog>} */
  public static final String WAREHOUSE = "warehouse";

  public static final String ICEBERG_REST_URI = "iceberg-rest-uri";

  public static final String LANCE_REST_URI = "lance-rest-uri";

  public static final String HEADER_PROPERTY_PREFIX = "header.";

  /** Default Iceberg REST service port when not explicitly configured. */
  public static final int DEFAULT_ICEBERG_REST_PORT = 9001;

  /** Default Iceberg REST service path. */
  public static final String DEFAULT_ICEBERG_REST_PATH = "/iceberg";

  /** Default Lance REST service port when not explicitly configured. */
  public static final int DEFAULT_LANCE_REST_PORT = 9002;

  /** Default Lance REST service path. */
  public static final String DEFAULT_LANCE_REST_PATH = "/lance";

  /**
   * Property key to disable the Gravitino client-side version check. Gravitino does not support a
   * client whose version is higher than the server's; set {@code
   * gravitino.version-check.enabled=false} to skip the check when the server is older.
   */
  public static final String VERSION_CHECK_ENABLED = "gravitino.version-check.enabled";

  public static Map<String, String> extractHeaders(Map<String, String> catalogProperties) {
    Map<String, String> result = Maps.newHashMap();
    if (catalogProperties == null) {
      return result;
    }
    catalogProperties.forEach(
        (k, v) -> {
          if (k != null && k.startsWith(HEADER_PROPERTY_PREFIX) && v != null) {
            result.put(k.substring(HEADER_PROPERTY_PREFIX.length()), v);
          }
        });
    return result;
  }

  public static Pair<String, String> splitWarehouse(String warehouse) {
    if (StringUtils.isBlank(warehouse)) {
      throw new IllegalArgumentException(
          "Gravitino unified catalog property 'warehouse' is required, expected "
              + "'<metalake>.<catalog>'");
    }
    int idx = warehouse.indexOf('.');
    if (idx <= 0 || idx >= warehouse.length() - 1) {
      throw new IllegalArgumentException(
          "Invalid 'warehouse' value '" + warehouse + "', expected '<metalake>.<catalog>'");
    }
    return Pair.of(warehouse.substring(0, idx), warehouse.substring(idx + 1));
  }

  public static String deriveIcebergRestUri(String gravitinoUri) {
    return deriveRestUri(gravitinoUri, DEFAULT_ICEBERG_REST_PORT, DEFAULT_ICEBERG_REST_PATH);
  }

  public static String deriveLanceRestUri(String gravitinoUri) {
    return deriveRestUri(gravitinoUri, DEFAULT_LANCE_REST_PORT, DEFAULT_LANCE_REST_PATH);
  }

  private static String deriveRestUri(String gravitinoUri, int port, String path) {
    if (gravitinoUri == null || gravitinoUri.trim().isEmpty()) {
      return null;
    }
    try {
      java.net.URI base = java.net.URI.create(gravitinoUri);
      String scheme = base.getScheme() != null ? base.getScheme() : "http";
      String host = base.getHost();
      if (host == null || host.isEmpty()) {
        return null;
      }
      return scheme + "://" + host + ":" + port + path;
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
