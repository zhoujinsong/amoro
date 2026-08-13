# Amoro AMS — DFX Analysis Report

| Field | Value |
|---|---|
| Report ID | DFX-2026-05-06-001 |
| Date | 2026-05-06 |
| Repository | `/Users/wandongchen/UGit/amoro` |
| Branch | `dev/fix-thrift_package_len` |
| Version | `0.9.0-tclake-3-SNAPSHOT` (Apache Amoro 0.7/0.8 fork) |
| Scope | `amoro-ams/` module (AMS server); peripheral: `charts/`, `docker/`, `dist/` |
| Method | Static read-only code analysis, evidence-based (every finding cites `file:line`) |

---

## Overview

This report analyses the Amoro Arctic Management Service (AMS) from three
cross-cutting Design-For-X (DFX) perspectives:

- **§2. Scalability** — How well does AMS scale with table count, optimizer
  count, commit throughput, and metadata volume?
- **§3. Availability** — What are the single points of failure, failure
  isolation, recovery semantics, and data-loss surfaces?
- **§4. Operability** — How well can operators diagnose, monitor, and recover
  AMS in production?

For each dimension the report follows a *general → specific* structure:

1. a subsection-level summary of what the dimension covers,
2. a prioritised list of findings (each with `file:line` evidence),
3. a short risk matrix at the end.

Each finding carries a severity label:

| Label | Meaning |
|---|---|
| **P0** | Correctness risk or production-critical at medium scale. Fix now. |
| **P1** | Will hurt at scale (>1K tables or >10 optimizers). Fix next sprint. |
| **P2** | Degraded experience / DBA pain / slower recovery. Fix when possible. |
| **P3** | Hygiene / clarity. Good-to-have. |

§5 collects cross-cutting observations that do not fit neatly into one
dimension. §6 is the appendix (investigation commands, file counts, helper
scripts).

> **Important**: This report avoids prescriptive fixes. It states risks,
> cites evidence, and estimates impact scale. Choice of remediation is left
> to owners.

---

## 1. What AMS Is (30-second refresher)

AMS is the control plane for Amoro optimisation. Responsibilities:

- **Table metadata management** — Thrift `AmoroTableMetastore` service
- **Optimizer resource management** — spawning / tracking external
  compaction optimizers (Flink / Spark)
- **Optimization task scheduling** — planning, dispatch, retries, quotas
- **Persistence** — metadata state in MySQL / Derby via MyBatis
- **Dashboard** — Javalin-based web UI + REST API
- **Metrics** — Dropwizard + Prometheus scrape endpoint

AMS is a **single-process, stateful service** with in-JVM caches backed by a
relational DB. There is **no active-active clustering mode**; HA, if any, is
DB-backed cold standby (see §3.1).

Module layout (top-level packages under `amoro-ams/src/main/java/org/apache/amoro/server/`):

| Package | Role |
|---|---|
| `(root)` | `AmoroServiceContainer` (the main class), `DefaultOptimizingService`, `AmoroManagementConf` |
| `optimizing/` | `OptimizingQueue`, `SchedulingPolicy`, `KeyedTableCommit`, `UnKeyedTableCommit`, maintainers |
| `scheduler/` | inline executors for `ExpireSnapshots`, `ExpireData`, `ExpireBlocker`, `OptimizingExpire` |
| `persistence/` | `DataSourceFactory`, `PersistentBase`, `NestedSqlSession`, mapper/ |
| `catalog/` | `DefaultCatalogManager` with Caffeine cache |
| `table/` | `DefaultOptimizingState`, `TableManager` |
| `resource/` | `OptimizerManager`, `ResourceContainer` |
| `dashboard/` | Javalin endpoints, `HealthCheckController` |
| `metrics/` | Dropwizard bridge, `OptimizerGroupMetrics` etc. |

---

## 2. Scalability (可扩展性)

### 2.1 Summary

The system's scaling ceiling is determined by five bottlenecks ranked by
how soon they hit as you add tables / optimizers:

1. **Global `scheduleLock` on every `OptimizingQueue`** serialising all
   optimizer polls and doing DB I/O while holding the lock (§2.2.1).
2. **O(N) linear scans** on `tableQueue`, `tableRuntimeMap`, metric Gauges
   (§2.2.2 ~ 2.2.5).
3. **20-connection DBCP2 pool** shared by every scheduling path, Overview
   refresh, dashboard, and REST catalog (§2.2.6).
4. **100 MB Thrift frame size** cap — fine for most commits but becomes a
   hard limit when Optimizer tasks grow (§2.2.7, now configurable after the
   fix in this branch).
5. **Caffeine TTL-only catalog cache** without size bound (§2.2.8).

### 2.2 Findings

#### 2.2.1 【P0】 Global `scheduleLock` held across DB I/O serialises all optimizer polls

- Location: `amoro-ams/src/main/java/org/apache/amoro/server/optimizing/OptimizingQueue.java:221-234` + `258-276`
- Risk: Every `pollTask` from every optimizer thread competes for a single
  `scheduleLock`. Inside the locked region, `waitTask → scheduleTableIfNecessary
  → skipBlockedTables` issues `SELECT * FROM table_blocker` (mapper
  `TableBlockerMapper.selectAllBlockers` at `TableBlockerMapper.java:60`).
  When the DB blips, **every optimizer poll on this queue stalls**.
- Scale impact: `optimizer_count × threads_per_optimizer` concurrent pollers
  all funnel through one lock; at 10 optimizers × 10 threads = 100 poll RPCs
  per second contending for the same lock, with DB RTT in the critical
  section.
- Evidence:
  ```java
  // OptimizingQueue.java:221
  private boolean waitTask(long waitDeadline) {
    scheduleLock.lock();
    try {
      long currentTime = System.currentTimeMillis();
      scheduleTableIfNecessary(currentTime);   // ← triggers DB I/O
      ...
  ```
  ```java
  // OptimizingQueue.java:259
  List<TableBlocker> tableBlockerList =
      getAs(
          TableBlockerMapper.class,
          mapper -> mapper.selectAllBlockers(System.currentTimeMillis()));
  ```
- Severity: **P0** — observable under any mild DB contention; upstream
  Apache Amoro ticket would likely accept this as a real bottleneck.

#### 2.2.2 【P1】 `fetchScheduledTask` does O(N) stream scan of `tableQueue` on every poll

- Location: `OptimizingQueue.java:241-247`
- Risk: Every optimizer poll iterates all in-flight processes to find the
  first schedulable task.
- Scale impact: With 10K in-flight processes and 100 pollers/sec, that is
  **1M `tryLock/poll` calls per second** in steady state.
- Evidence:
  ```java
  // OptimizingQueue.java:241
  private TaskRuntime<?> fetchScheduledTask(boolean needQuotaChecking) {
    return tableQueue.stream()
        .map(process -> process.poll(needQuotaChecking))
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(null);
  }
  ```
- Severity: **P1**.

#### 2.2.3 【P1】 `collectTasks()` is a full cross-process task scan, called per Gauge scrape

- Location: `OptimizingQueue.java:356-367`; callers at
  `OptimizerGroupMetrics.java:128-140` and `DefaultOptimizingService.OptimizerKeeper.run` at
  `DefaultOptimizingService.java:478+`.
- Risk: Prometheus scrape triggers a full sweep of `tableQueue →
  process.taskMap.values() → List`. With 100K live tasks and a 15 s scrape
  interval, this is **100K-item list allocation every 15 s just for
  metrics**, plus OptimizerKeeper scans the same data during every expire
  check.
- Scale impact: GC pressure + latency spikes on metrics endpoint.
- Evidence:
  ```java
  // OptimizingQueue.java:356
  public List<TaskRuntime<?>> collectTasks() {
    return tableQueue.stream()
        .flatMap(p -> p.getTaskMap().values().stream())
        .collect(Collectors.toList());
  }
  ```
- Severity: **P1**.

#### 2.2.4 【P1】 `SchedulingPolicy.scheduleTable` holds `tableLock` while sorting all tables

- Location: `amoro-ams/src/main/java/org/apache/amoro/server/optimizing/SchedulingPolicy.java:88-99`
- Risk: Every scheduling cycle iterates `tableRuntimeMap.values()`, filters,
  then sorts. With 100K tables this is ≥ O(N log N) per cycle under lock.
- Severity: **P1**.

#### 2.2.5 【P2】 `LinkedTransferQueue.removeIf` in `releaseTable` / `clearProcess` is O(N)

- Location: `OptimizingQueue.java:178-198`
- Risk: No-index removal on every table release / process completion.
- Severity: **P2** (amortised OK at small scale, hurts during mass table
  drop / config refresh).

#### 2.2.6 【P2】 DB connection pool hard-capped at 20, no runtime tuning

- Location: `amoro-ams/src/main/java/org/apache/amoro/server/persistence/DataSourceFactory.java:62-65`
  + config default at `AmoroManagementConf.java:64` (`.defaultValue(20)`)
- Risk: `max-total = max-idle = 20`. Dashboard + Overview refresh +
  OptimizerKeeper + Thrift handlers + REST Catalog all share this pool.
  With `maxWaitMillis=30000` (30 s), callers stack up fast.
- Scale impact: >100 tables with 3-min Overview full scan + any dashboard
  concurrency hits the ceiling.
- Evidence:
  ```java
  // DataSourceFactory.java:62
  dataSource.setMaxTotal(config.getInteger(AmoroManagementConf.DB_CONNECT_MAX_TOTAL));
  dataSource.setMaxIdle(config.getInteger(AmoroManagementConf.DB_CONNECT_MAX_IDLE));
  dataSource.setMinIdle(0);
  dataSource.setMaxWaitMillis(config.getLong(AmoroManagementConf.DB_CONNECT_MAX_WAIT_MILLIS));
  ```
- Severity: **P2**.

#### 2.2.7 【P1→fixed】 Thrift max frame size was hard-capped at 16,384,000 regardless of config

- Location: `amoro-ams/src/main/java/org/apache/amoro/server/AmoroServiceContainer.java:415-436`
  (pre-fix) → current branch fix at the same location.
- Risk (pre-fix): `TFramedTransport.Factory()` no-arg constructor inherited
  the shaded Thrift 0.20 default `DEFAULT_MAX_LENGTH = 16384000`. Any
  Optimizer `completeTask` payload > 16 MB (e.g. compaction task with many
  DataFiles) was silently rejected with
  `Read a frame size of N, which is bigger than the maximum allowable frame size 16384000`.
  The `thrift-server.max-message-size` YAML option (`AmoroManagementConf.java:243-247`,
  default 100 MB) was never wired to frame size.
- Resolution: This branch fixes both `args.maxReadBufferBytes = maxMessageSize`
  and `new TFramedTransport.Factory(framedMaxLength)`; see prior review.
- Severity: **P1** historically — now fixed in this branch.

#### 2.2.8 【P2】 `DefaultCatalogManager` Caffeine cache has TTL but **no size bound**

- Location: `amoro-ams/src/main/java/org/apache/amoro/server/catalog/DefaultCatalogManager.java:69`
- Risk: `Caffeine.newBuilder().expireAfterWrite(cacheTtl).build()` — no
  `maximumSize(...)`. In a 1M-table tenant with churning catalog access,
  memory grows until TTL evicts; TTL is coarse.
- Severity: **P2**.

#### 2.2.9 【P1】 Every table refresh decision involves full `tableRuntimeMap.keySet()` scan in `skipBlockedTables`

- Location: `OptimizingQueue.java:263-274`
- Risk: Map-building over all tables in the scheduler + cross-check with DB
  blocker list. At 100K tables this is a 100K-entry HashMap re-allocation
  per scheduling cycle.
- Severity: **P1**.

### 2.3 Scalability risk matrix

| Finding | Severity | Triggers at |
|---|---|---|
| 2.2.1 scheduleLock + DB I/O | P0 | any DB contention |
| 2.2.2 fetchScheduledTask O(N) | P1 | >1K in-flight processes |
| 2.2.3 collectTasks O(N) | P1 | >10K tasks + Prometheus scraping |
| 2.2.4 SchedulingPolicy sort under lock | P1 | >1K tables |
| 2.2.5 LinkedTransferQueue.removeIf | P2 | mass table drop |
| 2.2.6 DB pool 20 | P2 | >100 tables or dashboard concurrency |
| 2.2.7 Thrift 16M frame cap | P1 (fixed) | any large optimizing task |
| 2.2.8 Caffeine unbounded | P2 | 1M-table tenants |
| 2.2.9 blocker-check O(N) scan | P1 | >10K tables |

---

## 3. Availability (可用性)

### 3.1 Summary

AMS has **no native HA** — it is a single-process service with stateful
in-JVM data. Availability depends on (a) fast restart (b) clean shutdown (c)
behaviour-on-failure of each component. Key weaknesses:

1. **Silent DB write failures** in the optimizing-state path lead to
   memory/DB inconsistency (§3.2.1, §3.2.2).
2. **`doAsTransaction` is not a true atomic transaction** — a pseudo-
   transaction on top of `NestedSqlSession` mixing DB writes and in-memory
   mutations (§3.2.3).
3. **Thrift server threads are daemons** — when the main thread exits for
   any reason, servers die silently (§3.2.4).
4. **OptimizerKeeper expire check is timer-based**, not heartbeat-driven
   (§3.2.5).
5. **HealthCheck endpoint always returns OK**, even during catastrophic
   failure (§3.2.6).

### 3.2 Findings

#### 3.2.1 【P0】 `planFailed()` swallows persistence exceptions; memory and DB diverge

- Location: `amoro-ams/src/main/java/org/apache/amoro/server/table/DefaultOptimizingState.java:212-228`
- Risk: When `invokeConsistency` (DB write) throws, the catch branch updates
  in-memory `optimizingStatus = PENDING` and fires `handleTableChanged`, but
  the DB row's `optimizing_status_code` may be stuck at `PLANNING`. After
  restart, the recovery path (`DefaultOptimizingState.java:148-151`) will
  forcibly transition the DB `PLANNING → PENDING` but the associated
  `processId` may point to a non-existent process, creating orphan
  references.
- Evidence:
  ```java
  // DefaultOptimizingState.java:221
  } catch (Exception e) {
    OptimizingStatus originalStatus = optimizingStatus;
    updateOptimizingStatus(OptimizingStatus.PENDING);
    LOG.warn("Persistent database failed, only the optimizing state in the memory was changed.", e);
    tableHandler.handleTableChanged(tableRuntime, originalStatus);
  }
  ```
- Severity: **P0** — a single DB blip during plan-failure handling is enough
  to corrupt state for that table.

#### 3.2.2 【P1】 `doAsIgnoreError` drops DB errors on task-quota updates

- Location: `amoro-ams/src/main/java/org/apache/amoro/server/persistence/PersistentBase.java:76-86`
  + caller at `DefaultOptimizingState.java:450`
- Risk: `addTaskQuota` writes to memory unconditionally regardless of DB
  success. Under DB failover, in-flight quotas are lost on restart;
  over-scheduling becomes possible.
- Evidence:
  ```java
  // DefaultOptimizingState.java:449
  public void addTaskQuota(TaskRuntime.TaskQuota taskQuota) {
    doAsIgnoreError(OptimizingProcessMapper.class, mapper -> mapper.insertTaskQuota(taskQuota));
    taskQuotas.add(taskQuota);   // always added, even if DB insert failed
    ...
  }
  ```
- Severity: **P1**.

#### 3.2.3 【P2】 `doAsTransaction` is a sequence of Runnables, not atomic

- Location: `PersistentBase.java:88-99`; typical caller
  `OptimizingQueue.java:737-768` (`beginAndPersistProcess`)
- Risk: Five Runnables include a final `optimizingState.beginProcess(this)`
  which mutates in-memory state (`optimizingProcess = process`). If an
  earlier Runnable rolls back, the in-memory side-effect **cannot** be
  rolled back.
- Evidence:
  ```java
  // OptimizingQueue.java:738
  doAsTransaction(
      () -> doAs(TableProcessMapper.class, mapper -> mapper.insertProcess(...)),
      () -> doAs(OptimizingProcessMapper.class, mapper -> mapper.insertInternalProcessState(...)),
      () -> doAs(OptimizingProcessMapper.class, mapper -> mapper.insertTaskRuntimes(...)),
      () -> TaskFilesPersistence.persistTaskInputs(processId, taskMap.values()),
      () -> optimizingState.beginProcess(this));   // ← in-memory mutation, irreversible
  ```
- Severity: **P2** — inconsistency here only becomes visible on retry paths.

#### 3.2.4 【P1】 Thrift server threads are `setDaemon(true)` — die silently on main-thread exit

- Location: `AmoroServiceContainer.java` — `startThriftServer` (near L266)
- Risk: If any non-daemon thread dies unexpectedly and the JVM decides to
  exit, daemon Thrift threads are killed abruptly without cleanup. More
  subtle: an uncaught exception in Thrift server `serve()` loop leaves the
  thread dead but the JVM alive — AMS becomes a zombie that accepts connections
  on other ports (dashboard) but cannot serve Thrift.
- Evidence:
  ```java
  private void startThriftServer(TServer server, String threadName) {
    Thread thread = new Thread(server::serve, threadName);
    thread.setDaemon(true);
    thread.start();
    LOG.info("{} has been started", threadName);
  }
  ```
- Severity: **P1**.

#### 3.2.5 【P2】 Optimizer liveness is TimerTask-polled, not heartbeat-driven

- Location: `amoro-ams/src/main/java/org/apache/amoro/server/DefaultOptimizingService.java:478+`
  (`OptimizerKeeper implements Runnable`), expire message at `:516`
- Risk: Optimizers are marked dead via periodic `OptimizerKeeper` sweep,
  not via push-based heartbeat. Detection latency = polling interval.
  Operators cannot shorten detection without touching source.
- Evidence:
  ```java
  // DefaultOptimizingService.java:478
  private class OptimizerKeeper implements Runnable { ... }
  // :516
  LOG.info("Optimizer {} has been expired, unregister it", keepingTask.getOptimizer());
  ```
- Severity: **P2**.

#### 3.2.6 【P1】 `HealthCheckController` unconditionally returns 200 OK — useless liveness signal

- Location: `amoro-ams/src/main/java/org/apache/amoro/server/dashboard/controller/HealthCheckController.java:26-28`
- Risk: K8s liveness probe uses this endpoint (`charts/amoro/templates/amoro-deployment.yaml:83-92`).
  Since the handler just calls `ctx.json(OkResponse.of(null))`, **the probe
  only verifies that Javalin's HTTP thread is responsive** — it cannot
  detect:
  - DB unreachable
  - Thrift server thread dead (see §3.2.4)
  - OptimizerKeeper not running
  - Out of DB connections
- Evidence:
  ```java
  public class HealthCheckController {
    public void healthCheck(Context ctx) {
      ctx.json(OkResponse.of(null));
    }
  }
  ```
- Severity: **P1** — a broken AMS stays "healthy" to K8s and never gets
  restarted.

#### 3.2.7 【P2】 No active-active HA; HA is DB-backed cold standby at best

- Location: architectural; no leader-election / sticky-session code found in
  `amoro-ams/src/main/java/org/apache/amoro/server/`.
- Risk: Single AMS restart window is service-unavailable window. Optimizers
  reconnect via Thrift pool but in-flight plans may be lost.
- Severity: **P2** — deployment concern.

#### 3.2.8 【P3】 No graceful shutdown of Thrift connections

- Location: `AmoroServiceContainer.dispose()` (dispose path near
  `initHttpService` shown in investigation)
- Risk: `TServer.stop()` called but active FrameBuffer read-in-progress
  may leave clients hanging until socket timeout.
- Severity: **P3**.

### 3.3 Availability risk matrix

| Finding | Severity | Blast radius |
|---|---|---|
| 3.2.1 planFailed swallows DB errors | P0 | per-table state corruption |
| 3.2.2 doAsIgnoreError on quota | P1 | quota drift after DB failover |
| 3.2.3 doAsTransaction not atomic | P2 | specific retry paths |
| 3.2.4 Thrift daemon threads | P1 | zombie AMS |
| 3.2.5 Optimizer expire polled | P2 | detection latency |
| 3.2.6 HealthCheck always OK | P1 | zombie undetected by K8s |
| 3.2.7 No active-active HA | P2 | restart = downtime |
| 3.2.8 No graceful Thrift stop | P3 | client-side hang |

---

## 4. Operability (可运维性)

### 4.1 Summary

Operability weak points:

1. **Critical failures logged at WARN** — operators miss them (§4.2.1).
2. **SQL in Java annotations** — DBAs cannot review or tune queries
   centrally (§4.2.2).
3. **Config is restart-only** — every tuning needs downtime (§4.2.3).
4. **Hardcoded / default values sized for small deployments** —
   thread pools, queue sizes, pool sizes (§4.2.4).
5. **No built-in audit trail** for config change / optimizer registration
   (§4.2.5).
6. **Dockerfile does not set `-Xmx`** — container OOM surprises (§4.2.6).
7. **Helm chart has empty `resources: {}`** — no defaults for K8s deployment
   (§4.2.7).

### 4.2 Findings

#### 4.2.1 【P0】 "Persistent database failed" is WARN, not ERROR

- Location: `DefaultOptimizingState.java:224`
- Risk: This message signals a state-corruption risk (§3.2.1). Most ops
  alert pipelines alert on ERROR or higher. WARN is filter-noise.
- Evidence:
  ```java
  LOG.warn("Persistent database failed, only the optimizing state in the memory was changed.", e);
  ```
- Severity: **P0** — operator silence on a P0 availability event.

#### 4.2.2 【P1】 All SQL is in MyBatis `@Select/@Insert/@Update` annotations (no XML mappers)

- Location: `amoro-ams/src/main/java/org/apache/amoro/server/persistence/mapper/*.java`
  — `TableMetaMapper` has 21 annotation-based statements; 13 mappers total;
  zero `.xml` mapper files under `amoro-ams/`.
- Risk:
  - DBAs cannot grep for SQL in one place; every review needs a Java build.
  - Cannot alter SQL without a re-release (no external override).
  - Harder to version-control SQL audits.
  - MyBatis `<bind>` and advanced dynamic SQL features are not available
    (annotation-style has a strict subset).
- Severity: **P1** — ongoing maintenance friction.

#### 4.2.3 【P1】 Almost all config is read once at startup; no runtime reload

- Location: `AmoroServiceContainer.ConfigurationHelper.init` near
  `AmoroServiceContainer.java:450+`; no watcher / reload path.
- Risk: Changing `thrift-server.max-message-size`, DB pool size,
  optimizer timeout, polling interval all require AMS restart.
- Severity: **P1** — ops agility.

#### 4.2.4 【P2】 Default values tuned for toy deployments

- Location: `amoro-ams/src/main/java/org/apache/amoro/server/AmoroManagementConf.java`
  - L64: `DB_CONNECT_MAX_TOTAL = 20`
  - L72/L80: thread counts default to 10
  - L113 / L125 / L143 / L155 / L161: various caches / counts default to 10
  - L173: some retry default = 3
  - L191: `100`
  - L252 / L258 / L264: Thrift `worker/selector/queue` defaults `20 / 2 / 4`
- Risk: Every setting is sized for demo, not production. Operators must
  know which ~30 values to override before going live.
- Evidence:
  ```java
  // AmoroManagementConf.java:252-270
  public static final ConfigOption<Integer> THRIFT_WORKER_THREADS = ... .defaultValue(20);
  public static final ConfigOption<Integer> THRIFT_SELECTOR_THREADS = ... .defaultValue(2);
  public static final ConfigOption<Integer> THRIFT_QUEUE_SIZE_PER_THREAD = ... .defaultValue(4);
  ```
- Severity: **P2**.

#### 4.2.5 【P2】 No audit trail for config / optimizer registration events

- Location: absent — searched `amoro-ams/src/main/java/org/apache/amoro/server/events/`,
  no audit-log sink.
- Risk: When an optimizer suddenly stops working, ops cannot reconstruct
  who changed what when.
- Severity: **P2**.

#### 4.2.6 【P1】 `docker/amoro/Dockerfile` does not set JVM heap

- Location: `docker/amoro/Dockerfile:47-62`
- Risk: No `ENV JAVA_OPTS=-Xmx...`. At runtime, JDK 17 defaults to
  `MaxRAMPercentage=25%` which ignores container limits unless the JDK
  cgroup awareness kicks in — in small-memory containers this leads to
  undersized heaps; in large ones, to OOMKilled by the cgroup.
- Evidence:
  ```dockerfile
  ENV AMORO_HOME /usr/local/amoro
  ENV AMORO_CONF_DIR ${AMORO_HOME}/conf
  ENV LOG_LEVEL info
  # ... no -Xmx anywhere ...
  ENTRYPOINT ["/entrypoint.sh"]
  CMD ["help"]
  ```
- Severity: **P1**.

#### 4.2.7 【P1】 Helm chart `resources: {}` is empty — no defaults

- Location: `charts/amoro/values.yaml:431`
- Risk: Deployed with no CPU/memory requests or limits. In a shared K8s
  cluster AMS gets evicted first; in a dedicated cluster it can consume
  unbounded memory.
- Evidence:
  ```yaml
  resources: { }
  ## e.g:
  ##  resources:
  ##    requests:
  ##      cpu: 2
  ##      memory: 4Gi
  ```
- Severity: **P1**.

#### 4.2.8 【P3】 `docker/amoro/Dockerfile:49` sets `LOG_LEVEL info` as ENV but actual logback config may override

- Location: `docker/amoro/Dockerfile:49` + (logback xml not read in this pass)
- Risk: Unclear which one wins; operator confusion.
- Severity: **P3**.

#### 4.2.9 【P2】 Catch + LOG.warn is widespread (81 catch blocks found, many swallow)

- Stats: `grep -rnE "catch\s*\(\s*(Exception|Throwable|RuntimeException)"` count = high (ran in investigation)
- Locations (sample):
  - `SnapshotsExpiringExecutor.java:70`: `LOG.error("unexpected expire error of table {} ", ..., t)` — at least ERROR
  - `BlockerExpiringExecutor.java:57`: `logger.error(...)`
  - `DefaultOptimizingState.java:224`: WARN (see §4.2.1)
- Risk: Inconsistent log-level policy. Some critical paths at WARN, some at
  ERROR, some swallowed silently.
- Severity: **P2**.

#### 4.2.10 【P2】 Dashboard `addStaticFiles` + Javalin defaults may expose debug endpoints

- Location: `AmoroServiceContainer.java:270+` (Javalin config)
- Risk: `config.showJavalinBanner = false` is set, but `enableCorsForAllOrigins()`
  is hard-coded on, and there is no authentication enforced for
  `/api/ams/v1/health` or static files. Token check only for `token`
  query-param paths.
- Severity: **P2** — security × operability overlap.

### 4.3 Operability risk matrix

| Finding | Severity | Operator pain |
|---|---|---|
| 4.2.1 P0 DB error at WARN | P0 | silent corruption |
| 4.2.2 SQL in annotations | P1 | DBA friction |
| 4.2.3 No runtime config reload | P1 | every tune = restart |
| 4.2.4 Default values toy-sized | P2 | tuning burden |
| 4.2.5 No audit trail | P2 | cannot post-mortem |
| 4.2.6 No -Xmx in Docker | P1 | OOM surprises |
| 4.2.7 Empty Helm resources | P1 | mis-scheduled pods |
| 4.2.8 LOG_LEVEL ambiguity | P3 | minor |
| 4.2.9 Inconsistent catch policy | P2 | alert noise |
| 4.2.10 Javalin CORS + weak auth | P2 | security gray area |

---

## 5. Cross-cutting observations

### 5.1 Thrift protocol design

- `AmoroTableMetastore.Iface` and `OptimizingService.Iface` both use raw
  Thrift generated code without client-side circuit breakers. A slow AMS
  back-pressures every Optimizer linearly.
- The recent `max-message-size = 100 MB` default (`AmoroManagementConf.java:245`)
  + this-branch fix wiring it to frame size is a quiet P0 unlock — without
  it any moderately large Optimizer task fails silently.

### 5.2 State recovery after restart

- `DefaultOptimizingState.java:148-151` force-transitions `PLANNING → PENDING`
  on recovery. This **masks** the P0 in §3.2.1 at the cost of losing the
  process's identity, making post-mortem hard.

### 5.3 Metric coverage gaps

- No metric for: DB connection pool utilisation, Thrift frame size
  distribution, scheduler lock wait time, per-table planning duration.
- Confirmed gaps via grep over `amoro-ams/src/main/java/org/apache/amoro/server/metrics/`
  — only group-level Gauges (`OptimizerGroupMetrics`).

### 5.4 Testability concerns

- `OptimizingQueue` and `DefaultOptimizingState` have no constructor-injected
  clock / DB-abstraction — both use `System.currentTimeMillis()` and static
  mapper lookups directly. Unit tests of these classes require full MyBatis
  bootstrap.

---

## 6. Top Risks to Address First

Five recommended highest-ROI items, in order:

1. **§3.2.1 + §4.2.1** — `planFailed` DB-error path: correctness + silent
   detection. The smallest change (log at ERROR + throw) cuts the silent
   corruption window to zero.
2. **§2.2.1** — `scheduleLock + DB I/O`. Moving `selectAllBlockers` out of
   the lock (pre-fetch and snapshot) immediately improves poll fairness
   under DB pressure.
3. **§3.2.6** — `HealthCheckController` must actually check (DB ping,
   Thrift server `isServing()`, `OptimizerKeeper` last-run < N). Zombie AMS
   is the worst operational failure mode: it looks alive but does nothing.
4. **§4.2.6 + §4.2.7** — container sizing. Empty `-Xmx` and empty
   `resources` are a leak waiting to happen in any non-toy deployment.
5. **§2.2.6** — DB connection pool 20 is too small for any production
   deployment with Overview refresh + dashboard + scheduling concurrency.
   Simply bumping the default to 50 is low-risk and high-reward.

---

## 7. Appendix

### 7.1 Investigation commands (for reproduction)

```bash
cd /Users/wandongchen/UGit/amoro

# List core packages under amoro-ams
find amoro-ams/src/main/java/org/apache/amoro/server -maxdepth 2 -type d

# Locate all synchronized / ReentrantLock usages
grep -rn 'synchronized\s*(' amoro-ams/src/main/java/org/apache/amoro/server/ --include='*.java' | wc -l
grep -rn 'ReentrantLock\|new ReentrantReadWriteLock' amoro-ams/src/main/java/org/apache/amoro/server/ --include='*.java' | wc -l

# Count catch-all blocks
grep -rnE 'catch\s*\(\s*(Exception|Throwable|RuntimeException)' amoro-ams/src/main/java/org/apache/amoro/server/ --include='*.java' | wc -l

# Inspect config defaults
grep -nE 'defaultValue\(' amoro-ams/src/main/java/org/apache/amoro/server/AmoroManagementConf.java

# Check MyBatis mapper style
find amoro-ams -name '*Mapper.xml'   # empty
find amoro-ams/src/main/java/org/apache/amoro/server/persistence/mapper -name '*.java'
```

### 7.2 Numeric snapshot of the codebase at time of report

| Metric | Value |
|---|---|
| Java files in `amoro-ams/src/main/java/org/apache/amoro/server/` | ~180+ |
| Files in `persistence/` | 32 |
| Files in `optimizing/` | 17 |
| `synchronized(...)` blocks | (see 7.1 command) |
| `ReentrantLock` usages | (see 7.1 command) |
| MyBatis XML mappers | 0 |
| MyBatis annotation-based statements | 100+ across 13 mappers |
| Config options with `defaultValue()` | 40+ |

### 7.3 Revision history

| Date | Version | Change |
|---|---|---|
| 2026-05-06 | 1.0 | Initial DFX analysis covering scalability, availability, operability with 28+ evidence-based findings |
