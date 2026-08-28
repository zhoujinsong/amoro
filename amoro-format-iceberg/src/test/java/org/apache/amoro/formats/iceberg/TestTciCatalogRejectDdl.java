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

package org.apache.amoro.formats.iceberg;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.amoro.table.TableMetaStore;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Amoro TciCatalog is recognition-only; drop must not mutate via Amoro. */
public class TestTciCatalogRejectDdl {

  @Test
  public void dropTable_rejectsWithGravitinoGuidance() {
    Catalog iceberg =
        new Catalog() {
          @Override
          public String name() {
            return "test";
          }

          @Override
          public List<TableIdentifier> listTables(Namespace namespace) {
            return Collections.emptyList();
          }

          @Override
          public org.apache.iceberg.Table createTable(
              TableIdentifier identifier,
              Schema schema,
              PartitionSpec spec,
              String location,
              Map<String, String> properties) {
            throw new UnsupportedOperationException();
          }

          @Override
          public Transaction newCreateTableTransaction(
              TableIdentifier identifier,
              Schema schema,
              PartitionSpec spec,
              String location,
              Map<String, String> properties) {
            throw new UnsupportedOperationException();
          }

          @Override
          public Transaction newReplaceTableTransaction(
              TableIdentifier identifier,
              Schema schema,
              PartitionSpec spec,
              String location,
              Map<String, String> properties,
              boolean orCreate) {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean dropTable(TableIdentifier identifier, boolean purge) {
            throw new UnsupportedOperationException("should not be called");
          }

          @Override
          public void renameTable(TableIdentifier from, TableIdentifier to) {
            throw new UnsupportedOperationException();
          }

          @Override
          public org.apache.iceberg.Table loadTable(TableIdentifier identifier) {
            throw new UnsupportedOperationException();
          }
        };

    TciCatalog catalog = new TciCatalog(iceberg, Collections.emptyMap(), TableMetaStore.EMPTY);
    try {
      catalog.dropTable("db", "t", false);
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      assertTrue(e.getMessage().contains("Gravitino"));
      assertTrue(e.getMessage().contains("recognition-only"));
    }
  }
}
