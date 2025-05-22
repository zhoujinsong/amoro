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

package org.apache.amoro.server.utils;

import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class SparkConfUtil {

  public static final String SPARK_PARAMETER_PREFIX = "spark-conf.";

  final Map<String, String> sparkConf;

  final Map<String, String> sparkOptions;

  public SparkConfUtil(Map<String, String> sparkConf, Map<String, String> sparkOptions) {
    this.sparkConf = sparkConf;
    this.sparkOptions = sparkOptions;
  }

  public String configValue(String key) {
    if (sparkOptions.containsKey(key)) {
      return sparkOptions.get(key);
    }
    return sparkConf.get(key);
  }

  public void putToOptions(String key, String value) {
    this.sparkOptions.put(key, value);
  }

  /**
   * The properties with prefix "spark-conf." will be merged with the following priority and
   * transformed into Spark options. 1. optimizing-group properties 2. optimizing-container
   * properties
   *
   * @return spark options, format is `--conf key1=value1 --conf key2=value2`
   */
  public String toConfOptions() {
    return sparkOptions.entrySet().stream()
        .map(entry -> "--conf " + entry.getKey() + "=" + entry.getValue())
        .collect(Collectors.joining(" "));
  }

  public static Builder buildFor(
      Map<String, String> sparkConf, Map<String, String> containerProperties) {
    return new Builder(sparkConf, containerProperties);
  }

  public Map<String, String> getSparkOptions() {
    return sparkOptions;
  }

  public Map<String, String> getSparkConf() {
    return sparkConf;
  }

  public static class Builder {
    final Map<String, String> sparkConf;
    Map<String, String> containerProperties;
    Map<String, String> groupProperties = Collections.emptyMap();

    public Builder(Map<String, String> sparkConf, Map<String, String> containerProperties) {
      this.sparkConf = Maps.newHashMap(sparkConf);
      this.containerProperties =
          containerProperties == null ? Collections.emptyMap() : containerProperties;
    }

    public Builder withGroupProperties(Map<String, String> groupProperties) {
      this.groupProperties = groupProperties;
      return this;
    }

    public SparkConfUtil build() {
      Map<String, String> options = Maps.newHashMap();
      this.containerProperties.entrySet().stream()
          .filter(entry -> entry.getKey().startsWith(SPARK_PARAMETER_PREFIX))
          .forEach(
              entry ->
                  options.put(
                      entry.getKey().substring(SPARK_PARAMETER_PREFIX.length()), entry.getValue()));

      this.groupProperties.entrySet().stream()
          .filter(entry -> entry.getKey().startsWith(SPARK_PARAMETER_PREFIX))
          .forEach(
              entry ->
                  options.put(
                      entry.getKey().substring(SPARK_PARAMETER_PREFIX.length()), entry.getValue()));

      return new SparkConfUtil(this.sparkConf, options);
    }
  }
}
