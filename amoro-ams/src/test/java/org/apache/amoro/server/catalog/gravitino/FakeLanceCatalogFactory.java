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

import org.apache.amoro.FormatCatalog;
import org.apache.amoro.FormatCatalogFactory;
import org.apache.amoro.TableFormat;
import org.apache.amoro.table.TableMetaStore;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Fake {@code FormatCatalogFactory} for {@link TableFormat#LANCE}. See {@link
 * FakeIcebergCatalogFactory} for rationale.
 */
public class FakeLanceCatalogFactory implements FormatCatalogFactory {

  /** Captures the properties this factory last received, for test assertions. */
  private static final AtomicReference<Map<String, String>> lastReceivedProperties =
      new AtomicReference<>();

  /** Returns the properties this factory last received, for test assertions. */
  static AtomicReference<Map<String, String>> getLastReceivedProperties() {
    return lastReceivedProperties;
  }

  @Override
  public FormatCatalog create(
      String catalogName,
      String metastoreType,
      Map<String, String> properties,
      TableMetaStore metaStore) {
    lastReceivedProperties.set(properties);
    return new FakeFormatCatalog(TableFormat.LANCE, catalogName);
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
