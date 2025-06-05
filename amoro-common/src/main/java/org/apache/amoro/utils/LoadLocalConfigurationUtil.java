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

package org.apache.amoro.utils;

import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadLocalConfigurationUtil {

  static Configuration localConfiguration;

  static volatile boolean initialized = false;

  private static final Logger LOG = LoggerFactory.getLogger(LoadLocalConfigurationUtil.class);

  static boolean enable =
      Boolean.parseBoolean(System.getProperty("amoro.load-local-hadoop-conf", "false"));

  public static void init(Configuration mergedConfigs) {
    if (!enable) {
      return;
    }
    if (initialized) {
      return;
    }
    if (localConfiguration == null) {
      synchronized (LoadLocalConfigurationUtil.class) {
        if (localConfiguration == null) {
          localConfiguration = mergedConfigs;
        }
        initialized = true;
      }
    }
    LOG.info("Initialized local Hadoop configuration: " + localConfiguration);
  }

  public static boolean checkInitialize() {
    return initialized;
  }

  public static boolean loadLocalConf() {
    return enable;
  }

  public static Configuration getLocalConfiguration() {
    Preconditions.checkNotNull(localConfiguration, "Local Hadoop configuration is not initialized");
    return localConfiguration;
  }
}
