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

import org.apache.amoro.properties.CatalogMetaProperties;
import org.lance.Dataset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Lance 参数工具类，集中管理参数常量定义和参数解析逻辑。
 *
 * <p>主要职责：
 *
 * <ul>
 *   <li>定义 Lance 存储相关的参数常量 KEY
 *   <li>从 catalog properties 中提取 COSN 存储配置
 *   <li>从 tableProperties 中提取 lance.storage. 前缀的存储参数
 *   <li>合并 Dataset 中的三类元数据（tableMetadata、config、schemaMetadata）
 *   <li>过滤敏感存储配置（用于前端展示）
 * </ul>
 */
public class LanceProperties {

  private LanceProperties() {}

  // ======================== 参数常量 KEY ========================

  /** 创建模式 */
  public static final String CREATE_MODE_EXIST_OK = "exist_ok";

  public static final String CREATE_MODE_OVERWRITE = "overwrite";
  public static final String CREATE_MODE_CREATE = "create";

  /** 空表标志 key，值为 "true" 表示该表仅在 HMS 中注册但尚未创建实际数据 */
  public static final String TABLE_EMPTY = "lance.table.empty";

  public static final String TABLE_LOCATION = "location";

  /** File Format 版本Key */
  public static final String DATA_STORAGE_VERSION = "lance.data_storage_version";

  public static final String DATA_STORAGE_VERSION_STABLE = "stable";

  /** lance.storage. 前缀，用于从 tableProperties 中提取存储配置 */
  public static final String LANCE_STORAGE_PREFIX = "lance.storage.";

  /** COS 存储配置 - 区域 */
  public static final String COS_REGION = CatalogMetaProperties.COS_STORAGE_CONFIGS_REGION;

  /** COS 存储配置 - 密钥 ID */
  public static final String COS_SECRET_ID = CatalogMetaProperties.COS_STORAGE_CONFIGS_SECRET_ID;

  /** COS 存储配置 - 密钥 Key */
  public static final String COS_SECRET_KEY = CatalogMetaProperties.COS_STORAGE_CONFIGS_SECRET_KEY;

  /** COS 存储配置 - 秘钥 Token */
  public static final String COS_SECURITY_TOKEN =
      CatalogMetaProperties.COS_STORAGE_CONFIGS_SECURITY_TOKEN;

  /** COS 存储配置的 key 列表，用于从 catalog properties 中批量提取 */
  public static final String[] COS_STORAGE_KEYS = {
    COS_REGION, COS_SECRET_ID, COS_SECRET_KEY, COS_SECURITY_TOKEN
  };

  /** COS 协议头 */
  public static final String COS_SCHEME = "cos://";

  /** COSN 协议头（腾讯云 Hadoop 兼容协议） */
  public static final String COSN_SCHEME = "cosn://";

  /** file 协议头前缀，匹配 file:/ 或 file:// 或 file:/// */
  private static final String FILE_SCHEME_PREFIX = "file:";

  /** 常见的文件系统协议头正则，用于匹配并替换 */
  private static final java.util.regex.Pattern SCHEME_PATTERN =
      java.util.regex.Pattern.compile("^[a-zA-Z][a-zA-Z0-9+\\-\\.]*://");

  // ======================== 协议转换 ========================

  /**
   * 将 root URL 统一化：将协议头替换为 cos://。
   *
   * <p>特殊处理：
   *
   * <ul>
   *   <li>{@code file:} 协议头的路径保持不变（本地文件系统路径）
   *   <li>{@code hdfs://namenode:8020/warehouse} → {@code cosn://namenode:8020/warehouse}
   *   <li>{@code chdfs://xxx/path} → {@code cosn://xxx/path}
   *   <li>{@code cosn://bucket/path} → {@code cosn://bucket/path}
   *   <li>{@code /local/path}（无协议头）→ {@code cosn:///local/path}
   * </ul>
   *
   * @param path 原始路径
   * @return 统一化后的路径，如果输入为 null 或空则原样返回
   */
  public static String normalizeRootUrl(String path) {
    if (path == null || path.isEmpty()) {
      return path;
    }
    // file 协议头保持不变
    if (path.toLowerCase().startsWith(FILE_SCHEME_PREFIX)) {
      return path;
    }
    java.util.regex.Matcher matcher = SCHEME_PATTERN.matcher(path);
    if (matcher.find()) {
      return matcher.replaceFirst(COSN_SCHEME);
    }
    // 无协议头，直接加上 cosn://
    return COSN_SCHEME + path;
  }

  /**
   * 将 cosn:// 协议头转换为 cos:// 协议头，用于 Lance SDK 直接使用的 location。
   *
   * <p>仅处理 cosn:// → cos:// 的转换，其他协议头（包括 file:、hdfs:// 等）保持不变。
   *
   * @param path 原始路径
   * @return 转换后的路径，如果输入为 null 或空或非 cosn:// 协议则原样返回
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
   * 将 cos:// 协议头转换为 cosn:// 协议头，用于 Hive Metastore 存储的 location。
   *
   * <p>仅处理 cos:// → cosn:// 的转换，其他协议头（包括 file:、hdfs:// 等）保持不变。
   *
   * @param path 原始路径
   * @return 转换后的路径，如果输入为 null 或空或非 cos:// 协议则原样返回
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

  // ======================== 存储参数提取 ========================

  /**
   * 从 catalog properties 中提取 COSN 存储相关配置（region、secret_id、secret_key）。
   *
   * @param properties catalog 级别的配置
   * @return 非空的存储配置 Map
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
   * 从 tableProperties 中提取 lance.storage. 前缀的存储配置，去除前缀后作为 storageOptions 返回， 同时从原 tableProperties
   * 中移除这些 storage 参数。
   *
   * <p>例如：{@code lance.storage.region=ap-guangzhou} 会被提取为 {@code region=ap-guangzhou}， 并从
   * tableProperties 中删除 {@code lance.storage.region} 这个 key。
   *
   * @param tableProperties 表属性（会被修改，移除 storage 相关的 key）
   * @return 去除前缀后的存储配置 Map
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
   * 合并存储配置：先提取 catalog 级别的 COSN 配置，再合并 table 级别的 storage 配置（table 级别优先级更高）。
   *
   * <p>注意：此方法会修改 tableProperties，移除其中的 lance.storage. 前缀参数。
   *
   * @param catalogProperties catalog 级别的配置
   * @param tableProperties 表属性（会被修改，移除 storage 相关的 key）
   * @return 合并后的存储配置 Map
   */
  public static Map<String, String> mergeStorageOptions(
      Map<String, String> catalogProperties, Map<String, String> tableProperties) {
    Map<String, String> storageOptions = extractCatalogStorageOptions(catalogProperties);
    Map<String, String> tableStorageOptions = extractStorageOptionsByPrefix(tableProperties);
    // table 级别的配置优先级更高，覆盖 catalog 级别
    storageOptions.putAll(tableStorageOptions);
    return storageOptions;
  }

  // ======================== 元数据合并 ========================

  /**
   * 从 Dataset 中合并三类元数据到统一的 tableProperties。
   *
   * <p>合并顺序（后者覆盖前者）：
   *
   * <ol>
   *   <li>tableMetadata - 表级别元数据
   *   <li>config - 配置信息
   *   <li>schemaMetadata - Schema 自定义元数据
   * </ol>
   *
   * @param dataset Lance 数据集
   * @return 合并后的属性 Map
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

  // ======================== 参数过滤 ========================

  /**
   * 从 properties 中移除敏感的 COSN 存储配置（region、secret_id、secret_key）， 用于前端展示等场景。
   *
   * @param properties 属性 Map（会被修改）
   */
  public static void removeSensitiveStorageConfigs(Map<String, String> properties) {
    for (String key : COS_STORAGE_KEYS) {
      properties.remove(key);
    }
  }
}
