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

package org.apache.amoro.server.manager;

import org.apache.amoro.config.Configurations;
import org.apache.amoro.metrics.MetricRegisterListener;
import org.apache.amoro.metrics.MetricReporter;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.amoro.server.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Metric plugins manager and registry */
public class MetricManager extends AbstractPluginManager<MetricReporter> {

  private static final Logger LOG = LoggerFactory.getLogger(MetricManager.class);

  public static final String PLUGIN_CATEGORY = "metric-reporters";

  /** Reporter name of the built-in Prometheus exporter. */
  static final String PROMETHEUS_REPORTER_NAME = "prometheus-exporter";

  /** Property key used by {@code PrometheusMetricsReporter} to toggle scrape-stage filtering. */
  static final String PROP_SKIP_ZERO_COUNTERS = "skip-zero-counters";

  /**
   * Property key used by {@code PrometheusMetricsReporter} to toggle scrape-stage filtering of
   * zero-valued Gauge samples.
   */
  static final String PROP_SKIP_ZERO_GAUGES = "skip-zero-gauges";

  private static volatile MetricManager INSTANCE;
  /**
   * Service-level configuration injected by {@link org.apache.amoro.server.AmoroServiceContainer}
   * via {@link #init(Configurations)} before the first {@link #getInstance()} call. May be {@code
   * null} in unit-test contexts where the container is not booted; in that case no global value is
   * propagated and reporter properties are used verbatim (default behaviour).
   */
  private static volatile Configurations serviceConfig;

  /**
   * Inject the service-level {@link Configurations} so that subsequent {@link #getInstance()} can
   * propagate global metric.* options into reporter plugin properties. Must be invoked before the
   * first {@code getInstance()} call. Idempotent: latest non-null value wins.
   */
  public static void init(Configurations config) {
    serviceConfig = config;
  }

  /** @return Get the singleton object. */
  public static MetricManager getInstance() {
    if (INSTANCE == null) {
      synchronized (MetricManager.class) {
        if (INSTANCE == null) {
          INSTANCE = new MetricManager();
          INSTANCE.initialize();
        }
      }
    }
    return INSTANCE;
  }

  /** Close the manager */
  public static void dispose() {
    synchronized (MetricManager.class) {
      if (INSTANCE != null) {
        INSTANCE.close();
      }
      INSTANCE = null;
    }
  }

  protected MetricManager() {
    super(PLUGIN_CATEGORY);
  }

  private final MetricRegistry globalRegistry = new MetricRegistry();

  public MetricRegistry getGlobalRegistry() {
    return this.globalRegistry;
  }

  /**
   * Override plugin configuration loading to merge the global {@code
   * metric.prometheus.skip-zero-counters} and {@code metric.prometheus.skip-zero-gauges} options
   * into the {@code prometheus-exporter} reporter's properties using {@code putIfAbsent} semantics
   * — i.e. an explicit per-reporter entry in {@code plugins/metric-reporters.yaml} always wins.
   * When {@link #serviceConfig} is {@code null} (e.g. unit-test contexts that did not call {@link
   * #init(Configurations)}), this method behaves identically to the parent implementation.
   */
  @Override
  protected List<PluginConfiguration> loadPluginConfigurations() {
    List<PluginConfiguration> configs = super.loadPluginConfigurations();
    if (serviceConfig == null) {
      return configs;
    }
    boolean globalSkipZeroCounters =
        serviceConfig.get(AmoroManagementConf.METRIC_PROMETHEUS_SKIP_ZERO_COUNTERS);
    boolean globalSkipZeroGauges =
        serviceConfig.get(AmoroManagementConf.METRIC_PROMETHEUS_SKIP_ZERO_GAUGES);
    configs = mergeGlobalSkipZeroCounters(configs, globalSkipZeroCounters);
    configs = mergeGlobalSkipZeroGauges(configs, globalSkipZeroGauges);
    return configs;
  }

  /**
   * Visible-for-testing pure transformation: returns a new list where every {@code
   * prometheus-exporter} entry has its {@code skip-zero-counters} property set to {@code
   * globalSkipZero} unless the entry already declared the key explicitly (in which case the local
   * value wins). Non-prometheus entries are returned untouched. Input list is not mutated.
   */
  static List<PluginConfiguration> mergeGlobalSkipZeroCounters(
      List<PluginConfiguration> configs, boolean globalSkipZero) {
    List<PluginConfiguration> result = new java.util.ArrayList<>(configs.size());
    for (PluginConfiguration cfg : configs) {
      if (!PROMETHEUS_REPORTER_NAME.equals(cfg.getName())) {
        result.add(cfg);
        continue;
      }
      Map<String, String> original = cfg.getProperties();
      if (original.containsKey(PROP_SKIP_ZERO_COUNTERS)) {
        // local override present -> keep verbatim
        result.add(cfg);
        continue;
      }
      Map<String, String> merged = new HashMap<>(original);
      merged.put(PROP_SKIP_ZERO_COUNTERS, Boolean.toString(globalSkipZero));
      result.add(new PluginConfiguration(cfg.getName(), cfg.isEnabled(), merged));
      LOG.info(
          "Reporter '{}' inherits global metric.prometheus.skip-zero-counters={}",
          cfg.getName(),
          globalSkipZero);
    }
    return result;
  }

  /**
   * Visible-for-testing pure transformation: returns a new list where every {@code
   * prometheus-exporter} entry has its {@code skip-zero-gauges} property set to {@code
   * globalSkipZero} unless the entry already declared the key explicitly (in which case the local
   * value wins). Non-prometheus entries are returned untouched. Input list is not mutated.
   */
  static List<PluginConfiguration> mergeGlobalSkipZeroGauges(
      List<PluginConfiguration> configs, boolean globalSkipZero) {
    List<PluginConfiguration> result = new java.util.ArrayList<>(configs.size());
    for (PluginConfiguration cfg : configs) {
      if (!PROMETHEUS_REPORTER_NAME.equals(cfg.getName())) {
        result.add(cfg);
        continue;
      }
      Map<String, String> original = cfg.getProperties();
      if (original.containsKey(PROP_SKIP_ZERO_GAUGES)) {
        // local override present -> keep verbatim
        result.add(cfg);
        continue;
      }
      Map<String, String> merged = new HashMap<>(original);
      merged.put(PROP_SKIP_ZERO_GAUGES, Boolean.toString(globalSkipZero));
      result.add(new PluginConfiguration(cfg.getName(), cfg.isEnabled(), merged));
      LOG.info(
          "Reporter '{}' inherits global metric.prometheus.skip-zero-gauges={}",
          cfg.getName(),
          globalSkipZero);
    }
    return result;
  }

  @Override
  public void initialize() {
    super.initialize();
    forEach(
        reporter -> {
          reporter.setGlobalMetricSet(globalRegistry);
          if (reporter instanceof MetricRegisterListener) {
            globalRegistry.addListener((MetricRegisterListener) reporter);
          }
        });
  }
}
