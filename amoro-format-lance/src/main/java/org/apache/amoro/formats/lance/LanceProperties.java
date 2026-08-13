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
import org.apache.amoro.properties.CatalogMetaProperties;
import org.lance.Dataset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for Lance parameters that centralizes constant definitions and parameter parsing.
 *
 * <p>Main responsibilities:
 *
 * <ul>
 *   <li>Define constant KEYs related to Lance storage
 *   <li>Extract COSN storage configuration from catalog properties
 *   <li>Extract storage parameters with the {@code lance.storage.} prefix from tableProperties
 *   <li>Merge the three kinds of metadata in a Dataset (tableMetadata, config, schemaMetadata)
 *   <li>Filter sensitive storage configuration (for frontend display)
 * </ul>
 */
public class LanceProperties {

  private LanceProperties() {}

  // ======================== Constant KEYs ========================

  /** Create mode */
  public static final String CREATE_MODE_EXIST_OK = "exist_ok";

  public static final String CREATE_MODE_OVERWRITE = "overwrite";
  public static final String CREATE_MODE_CREATE = "create";

  /** Default page size for lance REST listTables requests. */
  public static final int DEFAULT_PAGE_SIZE = 1000;

  /**
   * Empty-table flag key. Value {@code "true"} indicates that the table is only registered in HMS
   * and the underlying data has not been created yet.
   */
  public static final String TABLE_EMPTY = "lance.table.empty";

  public static final String TABLE_LOCATION = "location";

  /** File format version key */
  public static final String DATA_STORAGE_VERSION = "lance.data_storage_version";

  public static final String DATA_STORAGE_VERSION_STABLE = "stable";

  /** {@code lance.storage.} prefix, used to extract storage configuration from tableProperties */
  public static final String LANCE_STORAGE_PREFIX = "lance.storage.";

  /** COS storage configuration - region */
  public static final String COS_REGION = CatalogMetaProperties.COS_STORAGE_CONFIGS_REGION;

  /** COS storage configuration - secret id */
  public static final String COS_SECRET_ID = CatalogMetaProperties.COS_STORAGE_CONFIGS_SECRET_ID;

  /** COS storage configuration - secret key */
  public static final String COS_SECRET_KEY = CatalogMetaProperties.COS_STORAGE_CONFIGS_SECRET_KEY;

  /** COS storage configuration - security token */
  public static final String COS_SECURITY_TOKEN =
      CatalogMetaProperties.COS_STORAGE_CONFIGS_SECURITY_TOKEN;

  /** COS storage configuration - expires at millis */
  public static final String EXPIRES_AT_MILLIS =
      CatalogMetaProperties.COS_STORAGE_CONFIGS_EXPIRES_AT_MILLIS;

  /** List of COS storage keys, used to batch extract from catalog properties */
  public static final String[] COS_STORAGE_KEYS = {
    COS_REGION, COS_SECRET_ID, COS_SECRET_KEY, COS_SECURITY_TOKEN, EXPIRES_AT_MILLIS
  };

  /** COS scheme prefix */
  public static final String COS_SCHEME = "cos://";

  /** COSN scheme prefix (Tencent Cloud Hadoop-compatible scheme) */
  public static final String COSN_SCHEME = "cosn://";

  /** {@code file:} scheme prefix, matches {@code file:/} or {@code file://} or {@code file:///} */
  private static final String FILE_SCHEME_PREFIX = "file:";

  /** Common filesystem scheme regex, used for matching and replacement */
  private static final java.util.regex.Pattern SCHEME_PATTERN =
      java.util.regex.Pattern.compile("^[a-zA-Z][a-zA-Z0-9+\\-\\.]*://");

  // ======================== Scheme Conversion ========================

  /**
   * Normalize a root URL: replace its scheme prefix with {@code cos://}.
   *
   * <p>Special handling:
   *
   * <ul>
   *   <li>{@code file:} scheme paths stay unchanged (local filesystem paths)
   *   <li>{@code hdfs://namenode:8020/warehouse} → {@code cosn://namenode:8020/warehouse}
   *   <li>{@code chdfs://xxx/path} → {@code cosn://xxx/path}
   *   <li>{@code cosn://bucket/path} → {@code cosn://bucket/path}
   *   <li>{@code /local/path} (no scheme) → {@code cosn:///local/path}
   * </ul>
   *
   * @param path original path
   * @return normalized path; returned as-is if input is null or empty
   */
  public static String normalizeRootUrl(String path) {
    if (path == null || path.isEmpty()) {
      return path;
    }
    // file scheme stays unchanged
    if (path.toLowerCase().startsWith(FILE_SCHEME_PREFIX)) {
      return path;
    }
    java.util.regex.Matcher matcher = SCHEME_PATTERN.matcher(path);
    if (matcher.find()) {
      return matcher.replaceFirst(COSN_SCHEME);
    }
    // No scheme: prepend cosn://
    return COSN_SCHEME + path;
  }

  /**
   * Convert {@code cosn://} scheme to {@code cos://} scheme, used for the location passed directly
   * to the Lance SDK.
   *
   * <p>Only handles {@code cosn://} → {@code cos://}; other schemes (including {@code file:},
   * {@code hdfs://}, etc.) stay unchanged.
   *
   * @param path original path
   * @return converted path; returned as-is if input is null/empty or not a {@code cosn://} URL
   */
  public static String toCosScheme(String path) {
    if (path == null || path.isEmpty()) {
      return path;
    }
    if (path.startsWith(COSN_SCHEME)) {
      return COS_SCHEME + path.substring(COSN_SCHEME.length());
    }
    return path;
  }

  /**
   * Convert {@code cos://} scheme to {@code cosn://} scheme, used for the location stored in Hive
   * Metastore.
   *
   * <p>Only handles {@code cos://} → {@code cosn://}; other schemes (including {@code file:},
   * {@code hdfs://}, etc.) stay unchanged.
   *
   * @param path original path
   * @return converted path; returned as-is if input is null/empty or not a {@code cos://} URL
   */
  public static String toCosnScheme(String path) {
    if (path == null || path.isEmpty()) {
      return path;
    }
    if (path.startsWith(COS_SCHEME)) {
      return COSN_SCHEME + path.substring(COS_SCHEME.length());
    }
    return path;
  }

  // ======================== Storage Parameter Extraction ========================

  /**
   * Extract COSN storage related configuration (region, secret_id, secret_key) from catalog
   * properties.
   *
   * @param properties catalog level configuration
   * @return non-null storage configuration map
   */
  public static Map<String, String> extractCatalogStorageOptions(Map<String, String> properties) {
    Map<String, String> storageOptions = new HashMap<>();
    for (String key : COS_STORAGE_KEYS) {
      String value = properties.get(key);
      if (value != null && !value.isEmpty()) {
        storageOptions.put(key, value);
      }
    }
    return storageOptions;
  }

  /**
   * Extract storage configuration with the {@code lance.storage.} prefix from tableProperties,
   * strip the prefix and return the values as storageOptions, while removing those storage keys
   * from the original tableProperties.
   *
   * <p>For example, {@code lance.storage.region=ap-guangzhou} is extracted as {@code
   * region=ap-guangzhou} and the {@code lance.storage.region} key is removed from tableProperties.
   *
   * @param tableProperties table properties (will be modified, storage-related keys removed)
   * @return storage configuration map with prefix stripped
   */
  public static Map<String, String> extractStorageOptionsByPrefix(
      Map<String, String> tableProperties) {
    Map<String, String> storageOptions = new HashMap<>();
    List<String> keysToRemove = new ArrayList<>();
    for (Map.Entry<String, String> entry : tableProperties.entrySet()) {
      if (entry.getKey() != null && entry.getKey().startsWith(LANCE_STORAGE_PREFIX)) {
        String strippedKey = entry.getKey().substring(LANCE_STORAGE_PREFIX.length());
        if (!strippedKey.isEmpty() && entry.getValue() != null) {
          storageOptions.put(strippedKey, entry.getValue());
        }
        keysToRemove.add(entry.getKey());
      }
    }
    keysToRemove.forEach(tableProperties::remove);
    return storageOptions;
  }

  /**
   * Merge storage configurations: first extract catalog-level COSN configuration, then merge
   * table-level storage configuration on top (table-level has higher priority).
   *
   * <p>Note: this method modifies tableProperties by removing keys with the {@code lance.storage.}
   * prefix.
   *
   * @param catalogProperties catalog level configuration
   * @param tableProperties table properties (will be modified, storage-related keys removed)
   * @return merged storage configuration map
   */
  public static Map<String, String> mergeStorageOptions(
      Map<String, String> catalogProperties, Map<String, String> tableProperties) {
    Map<String, String> storageOptions = extractCatalogStorageOptions(catalogProperties);
    Map<String, String> tableStorageOptions = extractStorageOptionsByPrefix(tableProperties);
    // Table-level configuration has higher priority and overrides the catalog level
    storageOptions.putAll(tableStorageOptions);
    return storageOptions;
  }

  // ======================== Metadata Merging ========================

  /**
   * Merge three kinds of metadata from a Dataset into a unified tableProperties map.
   *
   * <p>Merge order (later overrides earlier):
   *
   * <ol>
   *   <li>tableMetadata - table level metadata
   *   <li>config - configuration
   *   <li>schemaMetadata - schema custom metadata
   * </ol>
   *
   * @param dataset Lance dataset
   * @return merged properties map
   */
  public static Map<String, String> mergeDatasetMetadata(Dataset dataset) {
    Map<String, String> mergedProperties = new HashMap<>();

    Map<String, String> tableMetadata = dataset.getTableMetadata();
    if (tableMetadata != null) {
      mergedProperties.putAll(tableMetadata);
    }

    Map<String, String> config = dataset.getConfig();
    if (config != null) {
      mergedProperties.putAll(config);
    }

    Map<String, String> schemaMetadata = dataset.getSchema().getCustomMetadata();
    if (schemaMetadata != null) {
      mergedProperties.putAll(schemaMetadata);
    }

    return mergedProperties;
  }

  // ======================== Parameter Filtering ========================

  /**
   * Remove sensitive COSN storage configurations (region, secret_id, secret_key) from properties,
   * used for frontend display and similar scenarios.
   *
   * @param properties properties map (will be modified)
   */
  public static void removeSensitiveStorageConfigs(Map<String, String> properties) {
    for (String key : COS_STORAGE_KEYS) {
      properties.remove(key);
    }
  }

  // ======================== Empty Table Detection ========================

  /**
   * Check whether the given AmoroTable is an "empty Lance table".
   *
   * <p>When a Lance table is only registered in HMS but the underlying dataset cannot be opened yet
   * (typical cases: just created without any data, underlying directory wiped, path or permission
   * errors), {@link LanceHiveCatalog#loadTable} falls back to returning a {@link LanceTable} whose
   * {@code originalTable()} is null and tags the properties with {@link #TABLE_EMPTY}=true.
   *
   * <p>All downstream logic that accesses the Lance Dataset (table details, snapshot refresh, file
   * listing, etc.) should call this method first to skip dataset access and avoid NPE when invoking
   * reflective or Dataset APIs.
   *
   * @param amoroTable the AmoroTable to check, may be null
   * @return true if it is an empty table (dataset access should be skipped)
   */
  public static boolean isEmptyLanceTable(AmoroTable<?> amoroTable) {
    if (amoroTable == null) {
      return true;
    }
    if (amoroTable.originalTable() == null) {
      return true;
    }
    Map<String, String> properties = amoroTable.properties();
    if (properties == null) {
      return false;
    }
    return Boolean.parseBoolean(properties.getOrDefault(TABLE_EMPTY, "false"));
  }
}
