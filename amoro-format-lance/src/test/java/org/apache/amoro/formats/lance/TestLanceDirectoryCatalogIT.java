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
import static org.junit.Assert.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.hive3.Hive3NamespaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration tests for {@link LanceDirectoryCatalog} using a real dir namespace + local
 * filesystem.
 *
 * <p>Verifies the listTables pagination fix end-to-end under a real directory namespace.
 * Complements {@link TestLanceHiveCatalogIT} (HMS environment) by using the local filesystem,
 * requiring no HMS.
 */
public class TestLanceDirectoryCatalogIT {

  private static final Logger LOG = LoggerFactory.getLogger(TestLanceDirectoryCatalogIT.class);

  private static final Schema ARROW_SCHEMA =
      new Schema(
          Arrays.asList(
              Field.nullable("id", new ArrowType.Int(64, true)),
              Field.nullable("name", ArrowType.Utf8.INSTANCE)));

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private LanceDirectoryCatalog catalog;
  private BufferAllocator allocator;
  private String rootLocation;

  @Before
  public void setUp() throws IOException {
    allocator = new RootAllocator(Long.MAX_VALUE);
    rootLocation = tempFolder.newFolder("lance_dir_root").getAbsolutePath();

    Map<String, String> properties = new HashMap<>();
    properties.put(Hive3NamespaceConfig.ROOT, rootLocation);
    LanceNamespace namespace = LanceNamespace.connect("dir", properties, allocator);
    catalog = new LanceDirectoryCatalog(namespace, properties, allocator);

    LOG.info("Directory catalog setup complete. root={}", rootLocation);
  }

  @After
  public void tearDown() {
    if (allocator != null) {
      allocator.close();
    }
  }

  /** Create a lance dataset under the root directory (for listTables verification). */
  private void createDataset(String tableName) {
    String location = rootLocation + "/" + tableName + ".lance";
    Dataset.write()
        .allocator(allocator)
        .uri(location)
        .schema(ARROW_SCHEMA)
        .mode(WriteParams.WriteMode.CREATE)
        .execute();
  }

  // ==================== listTables Tests ====================

  @Test
  public void testListTablesEmpty() {
    List<String> tables = catalog.listTables("default");
    assertNotNull(tables);
    assertTrue("empty directory should return an empty table list", tables.isEmpty());
  }

  @Test
  public void testListTablesAfterCreate() {
    createDataset("dir_table_1");
    createDataset("dir_table_2");

    List<String> tables = catalog.listTables("default");
    assertEquals("should list 2 tables", 2, tables.size());
  }

  @Test
  public void testListTablesMoreThan100() {
    int tableCount = 110;
    for (int i = 0; i < tableCount; i++) {
      createDataset("dir_table_" + i);
    }

    List<String> tables = catalog.listTables("default");
    assertEquals(
        "pagination fix should list all " + tableCount + " tables", tableCount, tables.size());
  }

  // ==================== listDatabases / databaseExists Tests ====================

  @Test
  public void testListDatabases() {
    List<String> dbs = catalog.listDatabases();
    assertEquals("directory catalog only supports the default database", 1, dbs.size());
    assertEquals("default", dbs.get(0));
  }

  @Test
  public void testDatabaseExists() {
    assertTrue("default database should exist", catalog.databaseExists("default"));
    assertFalse("non-default database should not exist", catalog.databaseExists("other_db"));
  }
}
