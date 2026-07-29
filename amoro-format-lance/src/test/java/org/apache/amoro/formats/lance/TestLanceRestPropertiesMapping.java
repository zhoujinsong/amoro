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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link LanceCatalogFactory#buildLanceRestProperties(Map)} and {@link
 * LanceCatalogFactory#deriveLanceUri(String)}.
 *
 * <p>Verifies the shared Iceberg-REST + Lance-REST property bag is correctly translated for Lance:
 *
 * <ul>
 *   <li>{@code warehouse = <metalake>.<catalog>} -> metalake becomes {@code
 *       header.X-Gravitino-Metalake}, catalog becomes the top-level namespace name;
 *   <li>auth header {@code header.Authorization} is reused;
 *   <li>{@code lance.uri} wins; otherwise the Lance uri is derived from the shared (iceberg) uri;
 *   <li>Iceberg-scoped keys ({@code warehouse} / {@code catalog-impl} / ...) never leak.
 * </ul>
 */
public class TestLanceRestPropertiesMapping {

  private static final String META_HEADER = "header.X-Gravitino-Metalake";

  /** Mimics the user's real Iceberg-REST catalog properties. */
  private Map<String, String> sharedCatalogProperties() {
    Map<String, String> p = new HashMap<>();
    p.put("uri", "http://127.0.0.1:9001/iceberg");
    p.put("warehouse", "260076792.lakehouse_01");
    p.put("catalog-impl", "org.apache.iceberg.rest.RESTCatalog");
    p.put("io-impl", "org.apache.iceberg.hadoop.HadoopFileIO");
    p.put("table-formats", "ICEBERG,MIXED_ICEBERG,LANCE");
    p.put("header.Authorization", "Basic dWluLzcwMDAwMjIzNzM1OTpkdW1teQ==");
    return p;
  }

  @Test
  public void testWarehouseSplitsIntoMetalakeHeaderAndCatalog() {
    Map<String, String> lance =
        LanceCatalogFactory.buildLanceRestProperties(sharedCatalogProperties());

    // metalake -> X-Gravitino-Metalake header
    assertEquals("260076792", lance.get(META_HEADER));
    // catalog -> top-level namespace name consumed by LanceRestCatalog
    assertEquals("lakehouse_01", lance.get(LanceRestCatalog.REST_CATALOG_NAME));
  }

  @Test
  public void testWarehouseWithoutDotIsTreatedAsCatalogOnly() {
    Map<String, String> p = sharedCatalogProperties();
    p.put("warehouse", "single_catalog");

    Map<String, String> lance = LanceCatalogFactory.buildLanceRestProperties(p);

    assertEquals("single_catalog", lance.get(LanceRestCatalog.REST_CATALOG_NAME));
    assertNull(lance.get(META_HEADER));
  }

  @Test
  public void testAuthHeaderIsReused() {
    Map<String, String> lance =
        LanceCatalogFactory.buildLanceRestProperties(sharedCatalogProperties());

    assertEquals("Basic dWluLzcwMDAwMjIzNzM1OTpkdW1teQ==", lance.get("header.Authorization"));
  }

  @Test
  public void testLanceUriFallbackRewritesEndpoint() {
    // No lance.uri -> derive from the shared iceberg uri: 9001/iceberg -> 9002/lance
    Map<String, String> lance =
        LanceCatalogFactory.buildLanceRestProperties(sharedCatalogProperties());

    assertEquals("http://127.0.0.1:9002/lance", lance.get("uri"));
  }

  @Test
  public void testExplicitLanceUriWins() {
    Map<String, String> p = sharedCatalogProperties();
    p.put("lance.uri", "http://lance-rest.example:9999/custom");

    Map<String, String> lance = LanceCatalogFactory.buildLanceRestProperties(p);

    assertEquals("http://lance-rest.example:9999/custom", lance.get("uri"));
  }

  @Test
  public void testIcebergScopedKeysDoNotLeakIntoLance() {
    Map<String, String> lance =
        LanceCatalogFactory.buildLanceRestProperties(sharedCatalogProperties());

    assertNull(lance.get("warehouse"));
    assertNull(lance.get("catalog-impl"));
    assertNull(lance.get("io-impl"));
    assertNull(lance.get("table-formats"));
  }

  @Test
  public void testExplicitLanceCatalogOverridesWarehouse() {
    Map<String, String> p = sharedCatalogProperties();
    p.put("lance.catalog", "override_catalog");

    Map<String, String> lance = LanceCatalogFactory.buildLanceRestProperties(p);

    // explicit lance.catalog wins over the warehouse-derived catalog
    assertEquals("override_catalog", lance.get(LanceRestCatalog.REST_CATALOG_NAME));
    // metalake header is still derived from warehouse
    assertEquals("260076792", lance.get(META_HEADER));
  }

  @Test
  public void testDeriveLanceUriTrailingPathRewrite() {
    // when the 9001/iceberg token is absent, the trailing /iceberg is rewritten to /lance
    assertEquals(
        "http://host:8080/lance", LanceCatalogFactory.deriveLanceUri("http://host:8080/iceberg"));
  }

  @Test
  public void testEmptyAndNullInputs() {
    assertTrue(LanceCatalogFactory.buildLanceRestProperties(null).isEmpty());
    assertTrue(LanceCatalogFactory.buildLanceRestProperties(new HashMap<>()).isEmpty());
  }
}
