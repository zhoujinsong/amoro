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

import org.apache.amoro.AmoroTable;
import org.apache.amoro.FormatCatalog;
import org.apache.amoro.NoSuchDatabaseException;
import org.apache.amoro.NoSuchTableException;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.table.TableIdentifier;
import org.apache.arrow.memory.BufferAllocator;
import org.lance.Dataset;
import org.lance.ReadOptions;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.TableExistsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lance format catalog implementation backed by a directory-based namespace.
 *
 * <p>This catalog uses a directory-based {@link LanceNamespace} to manage tables. It only supports
 * a single "default" database, and all tables are stored under the root directory.
 *
 * <p>Database-level operations (create, drop, set/remove properties) are not supported.
 */
public class LanceDirectoryCatalog implements FormatCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(LanceDirectoryCatalog.class);

  private final LanceNamespace directoryNamespace;
  //  private final TableMetaStore metaStore;
  private final Map<String, String> properties;
  private final BufferAllocator allocator;

  /**
   * Create a new LanceDirectoryCatalog instance.
   *
   * @param directoryNamespace the directory-based namespace client
   * @param properties catalog-level properties including storage configurations
   * @param allocator Arrow memory allocator for Lance dataset operations
   */
  public LanceDirectoryCatalog(
      LanceNamespace directoryNamespace,
      Map<String, String> properties,
      BufferAllocator allocator) {
    this.directoryNamespace = directoryNamespace;
    this.properties = properties;
    this.allocator = allocator;
  }

  /** {@inheritDoc} */
  @Override
  public List<String> listTables(String database) {
    // Directory namespace only supports "default" database
    if (!"default".equals(database)) {
      LOG.warn("Directory namespace only supports 'default' database, requested: {}", database);
      return Collections.emptyList();
    }

    try {
      // Directory namespace returns all tables in a single call (no pagination needed).
      ListTablesRequest request = new ListTablesRequest();
      request.setId(Collections.emptyList());
      ListTablesResponse response = directoryNamespace.listTables(request);
      return new ArrayList<>(response.getTables());
    } catch (Exception e) {
      LOG.error("Failed to list tables in database: {}", database, e);
      throw new RuntimeException("Failed to list tables in database: " + database, e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public List<String> listDatabases() {
    return Lists.newArrayList("default");
  }

  /** {@inheritDoc} */
  @Override
  public boolean databaseExists(String database) {
    return "default".equals(database);
  }

  /** {@inheritDoc} */
  @Override
  public boolean tableExists(String database, String table) {
    // Directory namespace only supports "default" database
    if (!"default".equals(database)) {
      return false;
    }

    try {
      TableExistsRequest request = new TableExistsRequest();
      // For directory namespace, table ID is just the table name (empty namespace + table)
      request.setId(Collections.singletonList(table));
      directoryNamespace.tableExists(request);
      return true;
    } catch (Exception e) {
      LOG.debug("Table does not exist: {}.{}", database, table, e);
      return false;
    }
  }

  /**
   * Not supported. Directory namespace does not support creating databases.
   *
   * @throws UnsupportedOperationException always
   */
  @Override
  public void createDatabase(String database) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. Directory namespace does not support creating databases.
   *
   * @throws UnsupportedOperationException always
   */
  @Override
  public void createDatabase(String database, Map<String, String> metadata) {
    throw new UnsupportedOperationException();
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> loadDatabaseMetadata(String database) {
    return Map.of();
  }

  /**
   * Not supported. Directory namespace does not support dropping databases.
   *
   * @throws UnsupportedOperationException always
   */
  @Override
  public void dropDatabase(String database) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. Directory namespace does not support setting database properties.
   *
   * @throws UnsupportedOperationException always
   */
  @Override
  public void setDatabaseProperties(String database, Map<String, String> properties)
      throws NoSuchDatabaseException {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. Directory namespace does not support removing database properties.
   *
   * @throws UnsupportedOperationException always
   */
  @Override
  public void removeDatabaseProperties(String database, Set<String> properties)
      throws NoSuchDatabaseException {
    throw new UnsupportedOperationException();
  }

  /**
   * Load a table from the directory namespace.
   *
   * <p>Only the "default" database is supported. The table location is resolved via the directory
   * namespace, and the Lance dataset is opened with catalog-level storage options.
   *
   * @param database database name, must be "default"
   * @param table table name
   * @return loaded table instance with merged metadata from the dataset
   * @throws IllegalArgumentException if database is not "default" or table location is invalid
   * @throws RuntimeException if the table cannot be loaded
   */
  @Override
  public AmoroTable<?> loadTable(String database, String table) {
    // Directory namespace only supports "default" database
    if (!"default".equals(database)) {
      throw new IllegalArgumentException(
          "Directory namespace only supports 'default' database, requested: " + database);
    }

    // Use namespace to describe table and get location
    DescribeTableRequest request = new DescribeTableRequest();
    // For directory namespace, table ID is just the table name
    request.setId(Collections.singletonList(table));
    DescribeTableResponse response;
    try {
      response = directoryNamespace.describeTable(request);
    } catch (TableNotFoundException e) {
      throw new NoSuchTableException("Table does not exist: " + database + "." + table);
    }

    String location = response.getLocation();
    if (location == null || location.isEmpty()) {
      throw new IllegalArgumentException(
          "Failed to get table location for " + database + "." + table);
    }

    // Extract storage options and open the Lance dataset
    Map<String, String> storageOptions = LanceProperties.extractCatalogStorageOptions(properties);
    ReadOptions.Builder optionsBuilder = new ReadOptions.Builder();
    optionsBuilder.setStorageOptions(storageOptions);

    Dataset dataset = Dataset.open().uri(location).readOptions(optionsBuilder.build()).build();

    // Merge the three metadata sources into unified tableProperties
    Map<String, String> mergedProperties = LanceProperties.mergeDatasetMetadata(dataset);

    // Create TableIdentifier with "default" as catalog for directory namespace
    TableIdentifier tableIdentifier = TableIdentifier.of("default", database, table);

    // Create and return LanceTable
    return new LanceTable(tableIdentifier, dataset, mergedProperties);
  }

  /**
   * Drop a table from the directory namespace.
   *
   * <p>Only the "default" database is supported. The table and its data files are removed.
   *
   * @param database database name, must be "default"
   * @param table table name
   * @param purge not used in directory namespace (data is always removed)
   * @return true if the table was dropped, false if the database is not "default" or an error
   *     occurred
   */
  @Override
  public boolean dropTable(String database, String table, boolean purge) {
    // Directory namespace only supports "default" database
    if (!"default".equals(database)) {
      LOG.warn("Directory namespace only supports 'default' database, requested: {}", database);
      return false;
    }

    DropTableRequest request = new DropTableRequest();
    // For directory namespace, table ID is just the table name
    request.setId(Collections.singletonList(table));
    try {
      directoryNamespace.dropTable(request);
      return true;
    } catch (TableNotFoundException e) {
      throw new NoSuchTableException("Table does not exist: " + database + "." + table);
    } catch (Exception e) {
      LOG.error("Failed to drop table: {}.{}", database, table, e);
      return false;
    }
  }
}
