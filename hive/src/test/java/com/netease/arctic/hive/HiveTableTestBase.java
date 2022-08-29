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

package com.netease.arctic.hive;

import com.netease.arctic.CatalogMetaTestUtil;
import com.netease.arctic.TableTestBase;
import com.netease.arctic.ams.api.CatalogMeta;
import com.netease.arctic.ams.api.MockArcticMetastoreServer;
import com.netease.arctic.ams.api.properties.CatalogMetaProperties;
import com.netease.arctic.catalog.CatalogLoader;
import com.netease.arctic.hive.catalog.ArcticHiveCatalog;
import com.netease.arctic.hive.table.KeyedHiveTable;
import com.netease.arctic.hive.table.UnkeyedHiveTable;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.TableIdentifier;
import com.netease.arctic.table.TableProperties;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.netease.arctic.table.TableProperties;
import com.netease.arctic.table.UnkeyedTable;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.thrift.TException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.HashMap;
import java.util.Map;
import org.junit.rules.TemporaryFolder;

import static com.netease.arctic.ams.api.properties.CatalogMetaProperties.CATALOG_TYPE_HIVE;

public class HiveTableTestBase extends TableTestBase {
  protected static final String HIVE_DB_NAME = "hivedb";
  protected static final String HIVE_CATALOG_NAME = "hive_catalog";
  protected static final AtomicInteger testCount = new AtomicInteger(0);

  protected static final TemporaryFolder tempFolder = new TemporaryFolder();

  protected static HMSMockServer hms  ;

  protected static final TableIdentifier HIVE_TABLE_ID =
      TableIdentifier.of(HIVE_CATALOG_NAME, HIVE_DB_NAME, "test_hive_table");
  protected static final TableIdentifier HIVE_PK_TABLE_ID =
      TableIdentifier.of(HIVE_CATALOG_NAME, HIVE_DB_NAME, "test_pk_hive_table");

  protected static final TableIdentifier UN_PARTITION_HIVE_TABLE_ID =
      TableIdentifier.of(HIVE_CATALOG_NAME, HIVE_DB_NAME, "un_partition_test_hive_table");
  protected static final TableIdentifier UN_PARTITION_HIVE_PK_TABLE_ID =
      TableIdentifier.of(HIVE_CATALOG_NAME, HIVE_DB_NAME, "un_partition_test_pk_hive_table");

  public static final Schema HIVE_TABLE_SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "name", Types.StringType.get()),
      Types.NestedField.required(3, "op_time", Types.TimestampType.withoutZone()),
      Types.NestedField.required(4, "op_time_with_zone", Types.TimestampType.withZone()),
      Types.NestedField.required(5, "d", Types.DecimalType.of(3, 0))
  );

  protected static final PartitionSpec HIVE_SPEC =
      PartitionSpec.builderFor(HIVE_TABLE_SCHEMA).identity("name").build();

  protected ArcticHiveCatalog hiveCatalog;
  protected UnkeyedHiveTable testHiveTable;
  protected KeyedHiveTable testKeyedHiveTable;

  protected UnkeyedHiveTable testUnPartitionHiveTable;
  protected KeyedHiveTable testUnPartitionKeyedHiveTable;

  @BeforeClass
  public static void startMetastore() throws Exception {
    int ref = testCount.incrementAndGet();
    if (ref == 1){
      tempFolder.create();
      HiveTableTestBase.hms = new HMSMockServer(tempFolder.newFolder("hive"));
      hms.start();

      String dbPath = hms.getDatabasePath(HIVE_DB_NAME);
      Database db = new Database(HIVE_DB_NAME, "description", dbPath, new HashMap<>());
      hms.getClient().createDatabase(db);

      Map<String, String> storageConfig = new HashMap<>();
      storageConfig.put(
          CatalogMetaProperties.STORAGE_CONFIGS_KEY_TYPE,
          CatalogMetaProperties.STORAGE_CONFIGS_VALUE_TYPE_HDFS);
      storageConfig.put(CatalogMetaProperties.STORAGE_CONFIGS_KEY_CORE_SITE, MockArcticMetastoreServer.getHadoopSite());
      storageConfig.put(CatalogMetaProperties.STORAGE_CONFIGS_KEY_HDFS_SITE, MockArcticMetastoreServer.getHadoopSite());
      storageConfig.put(CatalogMetaProperties.STORAGE_CONFIGS_KEY_HIVE_SITE, CatalogMetaTestUtil.encodingSite(hms.hiveConf()));


      Map<String, String> authConfig = new HashMap<>();
      authConfig.put(CatalogMetaProperties.AUTH_CONFIGS_KEY_TYPE,
          CatalogMetaProperties.AUTH_CONFIGS_VALUE_TYPE_SIMPLE);
      authConfig.put(CatalogMetaProperties.AUTH_CONFIGS_KEY_HADOOP_USERNAME,
          System.getProperty("user.name"));

      Map<String, String> catalogProperties = new HashMap<>();

      CatalogMeta catalogMeta = new CatalogMeta(HIVE_CATALOG_NAME, CATALOG_TYPE_HIVE,
          storageConfig, authConfig, catalogProperties);
      AMS.createCatalogIfAbsent(catalogMeta);
    }
  }

  @AfterClass
  public static void stopMetastore() {
    int ref = testCount.decrementAndGet();
    if (ref == 0){
      hms.stop();
      HiveTableTestBase.hms = null;
      tempFolder.delete();
    }
  }

  @Before
  public void setupTables() throws Exception {
    hiveCatalog = (ArcticHiveCatalog) CatalogLoader.load(AMS.getUrl(HIVE_CATALOG_NAME));
    tableDir = temp.newFolder();

    testHiveTable = (UnkeyedHiveTable) hiveCatalog
        .newTableBuilder(HIVE_TABLE_ID, HIVE_TABLE_SCHEMA)
        .withPartitionSpec(HIVE_SPEC)
        .create().asUnkeyedTable();

    testUnPartitionHiveTable = (UnkeyedHiveTable) hiveCatalog
        .newTableBuilder(UN_PARTITION_HIVE_TABLE_ID, HIVE_TABLE_SCHEMA)
        .create().asUnkeyedTable();

    testKeyedHiveTable = (KeyedHiveTable) hiveCatalog
        .newTableBuilder(HIVE_PK_TABLE_ID, HIVE_TABLE_SCHEMA)
        .withPartitionSpec(HIVE_SPEC)
        .withPrimaryKeySpec(PRIMARY_KEY_SPEC)
        .create().asKeyedTable();

    testUnPartitionKeyedHiveTable = (KeyedHiveTable) hiveCatalog
        .newTableBuilder(UN_PARTITION_HIVE_PK_TABLE_ID, HIVE_TABLE_SCHEMA)
        .withPrimaryKeySpec(PRIMARY_KEY_SPEC)
        .create().asKeyedTable();
  }

  @After
  public void clearTable() {
    hiveCatalog.dropTable(HIVE_TABLE_ID, true);
    AMS.handler().getTableCommitMetas().remove(HIVE_TABLE_ID.buildTableIdentifier());

    hiveCatalog.dropTable(UN_PARTITION_HIVE_TABLE_ID, true);
    AMS.handler().getTableCommitMetas().remove(UN_PARTITION_HIVE_TABLE_ID.buildTableIdentifier());

    hiveCatalog.dropTable(HIVE_PK_TABLE_ID, true);
    AMS.handler().getTableCommitMetas().remove(HIVE_PK_TABLE_ID.buildTableIdentifier());

    hiveCatalog.dropTable(UN_PARTITION_HIVE_PK_TABLE_ID, true);
    AMS.handler().getTableCommitMetas().remove(UN_PARTITION_HIVE_PK_TABLE_ID.buildTableIdentifier());
  }


  public static String getPartitionPath(List<String> values, PartitionSpec spec) {
    List<String> nameValues = Lists.newArrayList();
    for (int i = 0; i < values.size(); i++) {
      String field = spec.fields().get(i).name();
      String value = values.get(i);
      nameValues.add(field + "=" + value);
    }
    return Joiner.on("/").join(nameValues);
  }

  public static StructLike getPartitionData(String partitionPath, PartitionSpec spec) {
    return DataFiles.data(spec, partitionPath);
  }

  /**
   * assert hive table partition location as expected
   */
  public static void assertHivePartitionLocations(Map<String, String> partitionLocations, ArcticTable table) throws TException {
    TableIdentifier identifier = table.id();
    final String database = identifier.getDatabase();
    final String tableName = identifier.getTableName();

    List<Partition> partitions = hms.getClient().listPartitions(
        database,
        tableName,
        (short) -1);

    System.out.println("> assert hive partition location as expected");
    System.out.printf("HiveTable[%s.%s] partition count: %d \n", database, tableName, partitions.size());
    for (Partition p: partitions){
      System.out.printf(
          "HiveTablePartition[%s.%s  %s] location:%s \n", database, tableName,
          Joiner.on("/").join(p.getValues()), p.getSd().getLocation());
    }

    Assert.assertEquals("expect " + partitionLocations.size() + " partition after first rewrite partition",
        partitionLocations.size(), partitions.size());

    UnkeyedTable unkeyedTable;
    if (table.isKeyedTable()) {
      unkeyedTable = table.asKeyedTable().baseTable();
    } else {
      unkeyedTable = table.asUnkeyedTable();
    }
    StructLikeMap<Map<String, String>> partitionProperties = unkeyedTable.partitionProperty();
    for (Partition p : partitions) {
      String valuePath = getPartitionPath(p.getValues(), table.spec());
      Assert.assertTrue("partition " + valuePath + " is not expected",
          partitionLocations.containsKey(valuePath));

      String locationExpect = partitionLocations.get(valuePath);
      String actualLocation = p.getSd().getLocation();
      Assert.assertTrue(
          "partition location is not expected, expect " + actualLocation + " end-with " + locationExpect,
          actualLocation.contains(locationExpect));
      Map<String, String> properties = partitionProperties.get(getPartitionData(valuePath, table.spec()));
      Assert.assertEquals("partition properties is not expected", actualLocation,
          properties.get(TableProperties.PARTITION_PROPERTIES_KEY_HIVE_LOCATION));
    }
  }
}