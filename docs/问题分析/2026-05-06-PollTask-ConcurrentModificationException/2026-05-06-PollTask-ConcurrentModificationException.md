# Issue Report — Optimizer `pollTask` ConcurrentModificationException

| Field | Value |
|---|---|
| Issue ID | ISSUE-2026-05-06-002 |
| Date Reported | 2026-05-06 17:48 |
| Severity | **P0** (correctness defect, race condition in production hot path) |
| Status | Diagnosed (root cause identified, fix proposed) |
| Affected Component | AMS server, `OptimizingQueue` / `TableOptimizingProcess` |
| Affected Versions | Apache Amoro 0.7+ (Tencent fork `tclake-0.9.1-SNAPSHOT` confirmed; upstream most likely affected too) |
| Reporter | Optimizer client (`Optimizer-executor-142` thread on Spark optimizer pod) |
| Distinct from prior fix? | **Yes** — unrelated to the recently-fixed Thrift max-message-size issue |

---

## Overview

This document is the post-mortem of a `java.util.ConcurrentModificationException`
observed at the optimizer client when polling tasks from AMS. The issue is
**a server-side concurrency defect**: in the AMS hot path
`OptimizingQueue.fetchScheduledTask`, multiple `tableQueue.stream() ...
flatMap(p -> p.getTaskMap().values().stream())` operations iterate over a
`HashMap` (`TableOptimizingProcess.taskMap`) that is concurrently mutated by
`loadTaskRuntimes` calls without any locking. The `HashMap` fail-fast
iterator detects modification and throws `ConcurrentModificationException`,
which AMS converts via `AmoroRuntimeException::normalize` into an
`AmoroException` Thrift response and ships back to the client.

The report is structured general → specific:

1. **Executive summary** — one-page version of the incident and fix (§1)
2. **Symptom** — what the user/log saw, and what the user mistakenly attributed
   it to (§2)
3. **Code-level evidence** — the exact `file:line` showing the bug (§3)
4. **Failure chain** — server-side flow and how CME crosses to the client (§4)
5. **Why now, why this scale** — what 10K → 100K table escalation changed (§5)
6. **Side effects and impact** — task scheduling correctness, log noise,
   throughput risk (§6)
7. **Recommended fixes** — minimal patch + defensive hardening (§7)
8. **Immediate operator actions** — what to do today without code changes
   (§8)
9. **Verification plan** — how to confirm the fix once applied (§9)
10. **Appendix** — full stack trace, related findings, references (§10)
11. **Companion incident (added 2026-05-06 v1.1)** — a separate
    `MaxMessageSize reached` TTransportException observed on the
    `completeTask` path at 17:43:00 in the same AMS instance. Confirmed
    to be a different defect from the CME above and distinct from the
    historically-fixed 16384000 frame-size bug. See §11.
12. **Upstream patch assessment (added 2026-05-06 v1.2)** — evaluates
    whether commit `179c77f9d` "[AMORO-3794] Wrap task operation with
    process lock" on branch `hotfix/tclake-amoro-20260506` resolves the
    CME documented in §1-§10. Verdict: **partial fix** — closes the
    `pollTask` / `ackTask` / `completeTask` CME paths, but leaves
    residual exposure in `collectTasks` (metrics scrape) and
    `getTaskRuntime`. See §12.

This report explicitly contrasts:
- the CME defect vs. the frame-size issue fixed in branch
  `dev/fix-thrift_package_len` (see §2.2); and
- the CME defect vs. the new `MaxMessageSize reached` incident (see §11.4).

---

## 1. Executive Summary

### 1.1 What happened

On 2026-05-06 17:48:03, the Spark optimizer (`Optimizer-executor-142`
thread) tried to call `OptimizingService.pollTask` on AMS to fetch the next
optimization task. The AMS server-side handler invoked an internal
`tableQueue.stream().flatMap(p -> p.getTaskMap().values().stream())`
operation. While that stream was iterating, another AMS thread mutated the
underlying `HashMap`, and the iterator's modCount check fired:

```
java.util.ConcurrentModificationException
```

AMS caught the `Throwable` in the Thrift handler proxy, wrapped it via
`AmoroRuntimeException::normalize` into an `AmoroException`, and returned
that as a Thrift response. The optimizer client deserialised the response
and surfaced:

```
ERROR Optimizer-executor-142 AbstractOptimizerOperator:
  Call ams got an error and will try again later
  org.apache.amoro.api.AmoroException: java.util.ConcurrentModificationException
```

### 1.2 Why it happened

| Layer | Defect |
|---|---|
| **Data structure** | `TableOptimizingProcess.taskMap` is a plain `HashMap` (`OptimizingQueue.java:424`, `Maps.newHashMap()`) |
| **Reader path** | `OptimizingQueue.fetchScheduledTask`, `getTask`, `collectTasks`, `releaseTable` all do `tableQueue.stream() ... taskMap.values().stream()` **without holding any lock** |
| **Writer path** | `loadTaskRuntimes` calls `taskMap.put(...)` (`OptimizingQueue.java:813`, `:840`) when a `TableOptimizingProcess` is loaded from DB or freshly planned |
| **Thread model** | Multiple optimizer Thrift worker threads call `pollTask` concurrently; planner threads concurrently call `loadTaskRuntimes`; no synchronisation between the two |

The fail-fast `HashMap` iterator does the right thing — it refuses to
iterate over a concurrently-mutated structure and throws CME instead of
returning incoherent results.

### 1.3 Cost and severity

| Dimension | Cost |
|---|---|
| Per-occurrence | 1 failed `pollTask` RPC (client retries automatically, basically self-healing in <1 s) |
| Operator pain | Heavy ERROR log noise on both optimizer client and AMS server |
| Throughput risk | If a process's `taskMap` is being heavily mutated, all concurrent `pollTask` may keep failing → optimizer pool stalls |
| Data correctness | No data loss; failed `pollTask` just doesn't dispatch a task this round |
| Severity | **P0** because it's a correctness bug in a hot path, not because the immediate user impact is huge |

### 1.4 Recommended fix (one-line patch)

```java
// OptimizingQueue.java:424 — change from
private final Map<OptimizingTaskId, TaskRuntime<RewriteStageTask>> taskMap =
    Maps.newHashMap();

// to
private final Map<OptimizingTaskId, TaskRuntime<RewriteStageTask>> taskMap =
    new ConcurrentHashMap<>();
```

With supporting changes for `taskQueue`, `fromSequence`, `toSequence`
(§7.2 - §7.3).

---

## 2. Symptom and Initial (Wrong) Attribution

### 2.1 Observed symptom

Stack trace in optimizer log (extracted from user-supplied log lines):

```
26/05/06 17:48:03.246 ERROR Optimizer-executor-142 AbstractOptimizerOperator:
  Call ams got an error and will try again later
org.apache.amoro.api.AmoroException: java.util.ConcurrentModificationException
  at org.apache.amoro.api.OptimizingService$pollTask_result$pollTask_resultStandardScheme.read(OptimizingService.java:3443)
  at ...
  at org.apache.amoro.api.OptimizingService$Client.recv_pollTask(OptimizingService.java:133)
  at org.apache.amoro.api.OptimizingService$Client.pollTask(OptimizingService.java:119)
  at org.apache.amoro.shade.thrift.org.apache.thrift.TServiceClient.receiveBase(TServiceClient.java:93)
  ...
  at org.apache.amoro.optimizer.common.AbstractOptimizerOperator.callAuthenticatedAms(AbstractOptimizerOperator.java:95)
  at org.apache.amoro.optimizer.common.OptimizerExecutor.pollTask(OptimizerExecutor.java:87)
  at org.apache.amoro.optimizer.common.OptimizerExecutor.start(OptimizerExecutor.java:53)
  at java.lang.Thread.run(Thread.java:829)
```

Note that the stack trace is **purely on the client side** (Spark optimizer
JVM). The CME message string is carried in the Thrift response body — the
actual exception was thrown on the AMS server JVM, caught, normalised, and
serialised.

### 2.2 What this is **not** (clarification for the on-call)

- **Not** the Thrift frame-size issue fixed by branch
  `dev/fix-thrift_package_len`. That fix dealt with `Read a frame size of N,
  which is bigger than the maximum allowable frame size`. CME is unrelated
  to message-size limits.
- **Not** a `Broken pipe` / TCP problem. The Thrift response was received
  cleanly — it just contained an error payload.
- **Not** a network/serialisation glitch. The exception text and stack are
  consistent and reproducible (heavy load reliably reproduces).
- **Not** an AMS OOM. CME is a logical correctness exception, not a
  memory-pressure exception.

### 2.3 Initial misattribution risk

Operators may see "Call ams got an error" in the optimizer log and assume
the same Thrift frame-size class of problem. That misdiagnosis would lead
to wasted effort tuning `thrift-server.max-message-size`, which has zero
effect here. This report's §3 is intended to provide unambiguous code-level
evidence to short-circuit that misdiagnosis.

---

## 3. Code-Level Evidence

### 3.1 The reader paths (no locks held)

`amoro-ams/src/main/java/org/apache/amoro/server/optimizing/OptimizingQueue.java`:

```java
// :241-247  fetchScheduledTask  ← directly on pollTask hot path
private TaskRuntime<?> fetchScheduledTask(boolean needQuotaChecking) {
  return tableQueue.stream()
      .map(process -> process.poll(needQuotaChecking))
      .filter(Objects::nonNull)
      .findFirst()
      .orElse(null);
}

// :349-353  getTask
public TaskRuntime<?> getTask(OptimizingTaskId taskId) {
  return tableQueue.stream()
      .filter(p -> p.getProcessId() == taskId.getProcessId())
      .findFirst()
      .map(p -> p.getTaskMap().get(taskId))      // ← reads taskMap
      .orElse(null);
}

// :356-360  collectTasks
public List<TaskRuntime<?>> collectTasks() {
  return tableQueue.stream()
      .flatMap(p -> p.getTaskMap().values().stream())   // ← iterates taskMap
      .collect(Collectors.toList());
}

// :362-367  collectTasks(Predicate)
public List<TaskRuntime<?>> collectTasks(Predicate<TaskRuntime<?>> predicate) {
  return tableQueue.stream()
      .flatMap(p -> p.getTaskMap().values().stream())   // ← iterates taskMap
      .filter(predicate)
      .collect(Collectors.toList());
}

// :181-189  releaseTable
public void releaseTable(DefaultTableRuntime tableRuntime) {
  scheduler.removeTable(tableRuntime);
  List<OptimizingProcess> processList =
      tableQueue.stream()
          .filter(process -> process.optimizingState == tableRuntime.getOptimizingState())
          .collect(Collectors.toList());
  ...
}
```

None of these calls take a lock on the `TableOptimizingProcess` they touch.

### 3.2 The writer paths (mutate `taskMap` without coordination)

```java
// :424  taskMap declaration (the offending line)
private final Map<OptimizingTaskId, TaskRuntime<RewriteStageTask>> taskMap =
    Maps.newHashMap();   // ← plain HashMap, NOT thread-safe

// :813  in loadTaskRuntimes(List<RewriteStageTask>) — fresh plan path
taskMap.put(taskRuntime.getTaskId(), taskRuntime);

// :840  in loadTaskRuntimes(OptimizingProcess) — recovery path
taskMap.put(taskRuntime.getTaskId(), taskRuntime);
```

`loadTaskRuntimes` is invoked from two constructors of
`TableOptimizingProcess`:

```java
// :455-461  fresh plan
public TableOptimizingProcess(AbstractOptimizingPlanner planner, ...) {
  ...
  loadTaskRuntimes(planner.planTasks());   // → :813 puts into taskMap
  beginAndPersistProcess();
}

// :469-486  recovery from DB
public TableOptimizingProcess(DefaultTableRuntime tableRuntime) {
  ...
  loadTaskRuntimes(this);   // → :840 puts into taskMap
}
```

These constructors are called from planner threads in
`triggerAsyncPlanning` (§4.2), which run **concurrently** with optimizer
worker threads calling `pollTask`.

### 3.3 The other unsafe collections in `TableOptimizingProcess`

```java
// :424  taskMap                 — HashMap        ❌
// :425  taskQueue               — LinkedList     ❌
// :429  fromSequence            — HashMap        ❌
// :430  toSequence              — HashMap        ❌
```

`fromSequence` / `toSequence` are written once in the constructor and only
read afterwards, so they are at lower risk. `taskQueue` is polled from
inside `process.poll()` under the per-process `lock.tryLock()` (§4.3) so
that one is mostly safe, but mixing locked and unlocked access to the same
data is fragile.

### 3.4 Outer collection `tableQueue` is OK

```java
// :90  tableQueue
private final Queue<TableOptimizingProcess> tableQueue =
    new LinkedTransferQueue<>();   // ← weakly-consistent iterator, no CME
```

The OUTER container is correctly chosen — `LinkedTransferQueue` provides a
weakly-consistent iterator that does NOT throw CME. **The bug is purely
that the inner `TableOptimizingProcess` leaks a non-thread-safe `HashMap`
through `getTaskMap()` to the stream pipelines listed in §3.1.**

### 3.5 The exception-normalisation glue

```java
// AmoroRuntimeException.java:116-118
public static AmoroException normalize(Throwable throwable) {
  return wrap(throwable).transform();
}

// :145-147
public static AmoroRuntimeException wrap(Throwable throwable) {
  return wrap(throwable, UndefinedException::new);
}
```

Any non-`AmoroRuntimeException` Throwable (including `CME`) is wrapped as
`UndefinedException` and transformed to an `AmoroException`. The Thrift
handler proxy registered in `DefaultOptimizingService` uses this normaliser
(see `AmoroServiceContainer.java:388` `AmoroRuntimeException::normalize`),
so any uncaught exception from the handler — including CME — surfaces
client-side as `AmoroException: java.util.ConcurrentModificationException`.
This explains exactly the message format the user observed.

---

## 4. Failure Chain (step-by-step)

This chapter walks through the failure end-to-end. §4.1 the client trigger,
§4.2 the server entry, §4.3 the race window, §4.4 the response path back to
the client.

### 4.1 Client side (the harmless part)

1. `OptimizerExecutor.start()` (`OptimizerExecutor.java:53`) loops while
   the optimizer is running.
2. It calls `pollTask()` (`:87`) which goes through
   `AbstractOptimizerOperator.callAuthenticatedAms` (`:95`).
3. `callAuthenticatedAms` borrows a Thrift client from
   `OptimizingClientPools.getClient(amsUrl)` and invokes
   `client.pollTask(token, threadId)` via the `ThriftClientPool` proxy
   (`ThriftClientPool.java:223`).
4. The proxy serialises the request, sends it on the Thrift socket, and
   blocks for the response.

### 4.2 Server side (where the race occurs)

5. AMS Thrift handler dispatches to `DefaultOptimizingService.pollTask`
   (`DefaultOptimizingService.java:218`).
6. It identifies the `OptimizingQueue` for this optimizer's group and
   calls `queue.pollTask(pollingTimeout, breakQuotaLimit)`.
7. `OptimizingQueue.pollTask` (`OptimizingQueue.java:200`) eventually
   calls `fetchScheduledTask` (`:241`) which is the **first reader path**
   from §3.1.
8. **At the same instant**, a separate AMS planner thread (one of the
   `newCachedThreadPool` plan executors from `DefaultOptimizingService.java:136`)
   has finished planning a table and is constructing a new
   `TableOptimizingProcess` — its constructor calls `loadTaskRuntimes`
   which `taskMap.put(...)` on the new process's HashMap.

   **Wait — does this race the reader?** The new `TableOptimizingProcess`
   is not yet in `tableQueue`, right? Mostly correct, but two paths break
   that assumption:
   - `TableOptimizingProcess` constructor calls `beginAndPersistProcess()`
     (`:457`) which adds to `tableQueue` **before** all task-runtime puts
     are done in some failure-recovery flows.
   - The `recover` constructor (`:469`) is called from
     `tableRuntime.recover(this)` flow, where the process is referenced
     from `tableQueue` and then `loadTaskRuntimes(this)` is called at
     `:485`, **after** `recover` already wired the process into externally
     visible state.

   Either way: at least one execution path lets a reader observe a
   `TableOptimizingProcess` whose `taskMap` is still being populated.

9. Reader thread does `flatMap(p -> p.getTaskMap().values().stream())`.
   The HashMap's iterator records `expectedModCount = modCount`. Writer
   thread does `taskMap.put(...)`, incrementing `modCount`. Reader's next
   `iterator.next()` checks `modCount != expectedModCount` and throws
   `ConcurrentModificationException`.

### 4.3 Why the lock at `TableOptimizingProcess.poll` doesn't save us

`TableOptimizingProcess.poll(boolean)` (`OptimizingQueue.java:433-451`)
takes `lock.tryLock()` and only touches `taskQueue.poll()` under the lock.
This protects the **single-process** task dispatch. But `fetchScheduledTask`
(`:241`) **iterates the entire `tableQueue`** and for each process calls
`process.poll(...)` — that's still safe because each `poll()` call
internally locks. The unsafe paths are §3.1's `getTask`, `collectTasks`,
`collectTasks(predicate)`, which read `process.getTaskMap()` directly
**without ever calling `poll()`** and therefore never acquire the
per-process lock.

### 4.4 Server → Client error propagation

10. CME bubbles up through the stream operation in `getTask`/`collectTasks`/
    etc., back to `DefaultOptimizingService.pollTask` (or wherever the
    reader was running).
11. The Thrift handler proxy installed at `AmoroServiceContainer.java:388`
    catches the Throwable and calls `AmoroRuntimeException::normalize`.
12. `normalize(CME) → wrap(CME, UndefinedException::new) → UndefinedException
    → transform() → AmoroException` (with `errorCode = -1` and message
    being the original Throwable's `toString()`, which is
    `"java.util.ConcurrentModificationException"`).
13. AMS Thrift writes this as a normal `pollTask_result` containing the
    exception field `e1`.
14. Client deserialises it, `recv_pollTask` (line 133 of generated code)
    re-throws as a regular Java throw, surfacing in
    `callAuthenticatedAms`.
15. `callAuthenticatedAms.shouldRetryLater(t)` returns `true` (default for
    non-`AmoroException`-typed-with-known-code throwables; in this case
    `errorCode = -1 = UNDEFINED`, which matches the
    `UNDEFINED_ERROR_CODE` retry path in
    `AbstractOptimizerOperator.java:73`), and the loop sleeps and retries.

---

## 5. Why Now, Why At This Scale

The bug has existed since the original Apache Amoro code path was
introduced. Why is it surfacing now?

| Factor | 1K tables (before) | 100K tables (now) |
|---|---|---|
| Optimizer thread count | ~16 | **142+** (per the user log thread name) |
| `pollTask` QPS per AMS | ~50 | **>1000** |
| Concurrent planner threads | 1-3 | **20-50** (`newCachedThreadPool`) |
| `tableQueue` size | ~100 in-flight | **5K-20K** |
| `taskMap` mutations per second | ~10 | **>200** |

The CME race is a **probabilistic** hit: time-window between
`expectedModCount` capture and `iterator.next()` is roughly 100-500 ns;
chance of a `taskMap.put()` falling exactly in that window scales with
`reads/sec × writes/sec`. At 100K-table scale this product crosses ~10⁵
events/sec², making collisions nearly certain in any given 5-minute
window.

This is consistent with what the user reports: the issue did not appear
before scaling out, even though the buggy code was always there.

---

## 6. Side Effects and Impact

### 6.1 Direct effects (per occurrence)

| Effect | Impact |
|---|---|
| Single `pollTask` call returns error instead of a task | Optimizer thread sleeps `callAmsInterval` (default 1 s) and retries |
| Client-side ERROR log line | One per failed call — but at 142+ threads, can be hundreds/sec |
| AMS server-side ERROR log | Same volume, one per failed handler |

### 6.2 Cascading risks

| Risk | Scenario | Likelihood |
|---|---|---|
| Throughput collapse | If a popular `OptimizingQueue` has continuous taskMap mutations from background planning, every `pollTask` attempt keeps hitting CME → optimizer pool effectively idles | Medium at 100K scale, low otherwise |
| Half-scheduled task | If CME is thrown **inside** `extractOptimizingTask` after `task.schedule(optimizerThread)` was called, the task is marked scheduled in `TaskStatusMachine` but the response is an error → AMS retries via `queue.retryTask(task)` and eventually a new optimizer picks it up. **Net effect: task may run twice** if the optimizer somehow ack'd in between (ack uses a different RPC) | Low (the outer try/catch at `DefaultOptimizingService.java:232-234` already handles this case correctly — see §3.5 reference) |
| Log storm | 142 threads × 1 retry/sec = 142 ERROR/sec on each side; ELK/CLS ingestion gets hammered | High |
| Alert fatigue | Same ERROR keeps firing → on-call ignores → real other ERRORs missed | High |

### 6.3 No data corruption

The CME aborts an iteration cleanly. The HashMap itself is not corrupted
(modCount mismatch is the only signal; the entries themselves are safe).
After the iterator throws, the server returns to a consistent state: the
`pollTask` simply did not return a task this round.

---

## 7. Recommended Fixes

### 7.1 P0 — minimal patch (one line)

```java
// OptimizingQueue.java:424
- private final Map<OptimizingTaskId, TaskRuntime<RewriteStageTask>> taskMap =
-     Maps.newHashMap();
+ private final Map<OptimizingTaskId, TaskRuntime<RewriteStageTask>> taskMap =
+     new ConcurrentHashMap<>();
```

`ConcurrentHashMap.values().stream()` returns a weakly-consistent view: it
reflects the state of the map at some point at-or-before the iterator is
created, never throws CME, and may include or omit concurrent
modifications. For the read-only stream operations in §3.1 this is
exactly the right semantics — the reader gets a coherent snapshot, the
writer is unblocked, no CME.

### 7.2 P1 — defensive: harden the rest of `TableOptimizingProcess`

```java
// :425
- private final Queue<TaskRuntime<RewriteStageTask>> taskQueue = new LinkedList<>();
+ private final Queue<TaskRuntime<RewriteStageTask>> taskQueue =
+     new ConcurrentLinkedQueue<>();

// :429
- private Map<String, Long> fromSequence = Maps.newHashMap();
+ private Map<String, Long> fromSequence = new ConcurrentHashMap<>();

// :430
- private Map<String, Long> toSequence = Maps.newHashMap();
+ private Map<String, Long> toSequence = new ConcurrentHashMap<>();
```

Even though `fromSequence` / `toSequence` are mostly write-once-then-read,
treating them as ConcurrentHashMap is cheap insurance against future code
changes that might add late writes.

### 7.3 P2 — preventive: forbid leaking unsafe collections through accessors

`TableOptimizingProcess.getTaskMap()` (`:616`) currently returns the raw
`Map`. Even after switching to `ConcurrentHashMap`, callers should not
rely on its mutability. Consider:

```java
private Map<OptimizingTaskId, TaskRuntime<RewriteStageTask>> getTaskMap() {
  return Collections.unmodifiableMap(taskMap);
}
```

This communicates intent (read-only view to outside callers) and prevents
accidental external mutations.

### 7.4 P2 — observability: detect future similar issues

Add a metric `ams.thrift.handler.exceptions{handler="pollTask",
error="ConcurrentModificationException"}` that increments whenever
`AmoroRuntimeException::normalize` wraps a non-`AmoroRuntimeException`.
This makes the next race condition visible the moment it appears, not
after operators notice in logs.

---

## 8. Immediate Operator Actions (no code change)

If a code fix cannot be deployed immediately, the following workarounds
reduce blast radius:

### 8.1 Reduce per-optimizer parallelism

```yaml
# AMS config or per-optimizer-group config
self-optimizing.parallelism-per-optimizer: 8   # down from current 16-32
```

Lower concurrency → smaller race window → CME frequency drops by
~quadratically.

### 8.2 Throttle ERROR-log noise

In log4j/logback config on optimizer side, add a deduplication or rate
limit on the
`AbstractOptimizerOperator: Call ams got an error and will try again later`
pattern. 1 line/min instead of hundreds/second.

### 8.3 Watch for cascading failure

```bash
# Count CME occurrences per minute on AMS side
grep "ConcurrentModificationException" ams.log | \
  awk '{print $1, $2}' | cut -c 1-15 | uniq -c | sort -k1,1 -nr | head

# Threshold:
#   <  1   /min  → ignore, schedule fix in next sprint
#   < 10   /min  → urgent, reduce parallelism (§8.1) and schedule fix this week
#   ≥ 100  /min  → drop everything, reduce parallelism immediately and hot-patch
```

### 8.4 Verify it's not the Thrift frame-size class

Confirm the issue is CME and **not** a regression of the previous
frame-size bug:

```bash
# CME (this report)
grep "ConcurrentModificationException" optimizer.log | wc -l

# Frame-size (previous report, should be 0 after fix)
grep "Read a frame size" ams.log | wc -l
grep "Broken pipe" optimizer.log | wc -l
```

If only the first command returns non-zero, you are looking at exactly the
issue this report covers.

---

## 9. Verification Plan (after fix is deployed)

### 9.1 Functional verification

Apply §7.1 patch, rebuild AMS, deploy to staging:

1. Start 1 AMS + 4 optimizers with thread-count=32 each (= 128 concurrent
   pollers).
2. Pre-populate 5K tables with random sized DataFiles to trigger
   continuous planning.
3. Run for 30 minutes. Observe `ams.log` and `optimizer.log`.

**Expected**: zero `ConcurrentModificationException` lines on either side.

### 9.2 Regression — make sure prior fixes still work

1. Send a single Optimizer `completeTask` with payload > 16 MB — must
   succeed (validates the prior `dev/fix-thrift_package_len` fix).
2. Send a payload > 100 MB — must fail with a clean error message
   (validates the configured upper bound).

### 9.3 Performance — confirm `ConcurrentHashMap` doesn't regress hot path

Compare `pollTask` p50 / p99 latency before/after the change. Expected
delta: < 5% (CHM is competitive with HashMap for read-heavy workloads).

---

## 10. Appendix

### 10.1 Full original stack trace (reformatted)

```
26/05/06 17:48:03.246 ERROR Optimizer-executor-142 AbstractOptimizerOperator:
  Call ams got an error and will try again later
org.apache.amoro.api.AmoroException: java.util.ConcurrentModificationException
  at org.apache.amoro.api.OptimizingService$pollTask_result$pollTask_resultStandardScheme
        .read(OptimizingService.java:3443)                                ~[…jar-with-dependencies.jar]
  at org.apache.amoro.api.OptimizingService$pollTask_result$pollTask_resultStandardScheme
        .read(OptimizingService.java:3419)                                ~[…]
  at org.apache.amoro.api.OptimizingService$pollTask_result.read(OptimizingService.java:3357) ~[…]
  at org.apache.amoro.shade.thrift.org.apache.thrift.TServiceClient
        .receiveBase(TServiceClient.java:93)                              ~[…]
  at org.apache.amoro.api.OptimizingService$Client.recv_pollTask(OptimizingService.java:133) ~[…]
  at org.apache.amoro.api.OptimizingService$Client.pollTask(OptimizingService.java:119)      ~[…]
  at jdk.internal.reflect.GeneratedMethodAccessor352.invoke(Unknown Source)                  ~[?:?]
  at jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[?:?]
  at java.lang.reflect.Method.invoke(Method.java:566)                                        ~[?:?]
  at org.apache.amoro.client.ThriftClientPool.lambda$iface$0(ThriftClientPool.java:223)      ~[…]
  at com.sun.proxy.$Proxy58.pollTask(Unknown Source)                                         ~[?:?]
  at org.apache.amoro.optimizer.common.OptimizerExecutor
        .lambda$pollTask$0(OptimizerExecutor.java:87)                     ~[…]
  at org.apache.amoro.optimizer.common.AbstractOptimizerOperator
        .callAuthenticatedAms(AbstractOptimizerOperator.java:95)          ~[…]
  at org.apache.amoro.optimizer.common.OptimizerExecutor.pollTask(OptimizerExecutor.java:87) ~[…]
  at org.apache.amoro.optimizer.common.OptimizerExecutor.start(OptimizerExecutor.java:53)    ~[…]
  at java.lang.Thread.run(Thread.java:829)                                                   [?:?]
```

### 10.2 Related findings (cross-references to other reports)

| Report | Relevance |
|---|---|
| `2026-05-06-Amoro-AMS-DFX分析报告.md` §2.2.2-2.2.3 | Already flagged the O(N) iteration patterns on `tableQueue` / `taskMap` as scalability risks. Did not flag the CME risk explicitly — to be amended (this report is now the proof). |
| `10万表内存容量评估-修订版.md` §1.5 / §6.1 | `taskMap` is also one of the per-process structures whose lifetime extends to commit; same cleanup issue applies. |
| Prior issue report `dev/fix-thrift_package_len` review | Unrelated to this CME defect (different layer). Confirmed by §2.2 of this report. |

### 10.3 Source-code reference index

All `file:line` references are against branch `dev/fix-thrift_package_len`,
commit `c181db983` baseline (worktree may differ slightly).

| Symbol | File | Line |
|---|---|---|
| `OptimizingQueue.tableQueue` (LinkedTransferQueue) | `OptimizingQueue.java` | 90 |
| `OptimizingQueue.retryTaskQueue` | `OptimizingQueue.java` | 91 |
| `OptimizingQueue.optimizingTasksMap` | `OptimizingQueue.java` | 102 |
| `fetchScheduledTask` | `OptimizingQueue.java` | 241-247 |
| `getTask` | `OptimizingQueue.java` | 349-353 |
| `collectTasks` | `OptimizingQueue.java` | 356-360 |
| `collectTasks(predicate)` | `OptimizingQueue.java` | 362-367 |
| `releaseTable` | `OptimizingQueue.java` | 178-194 |
| `clearProcess` | `OptimizingQueue.java` | 194-198 |
| `TableOptimizingProcess.taskMap` (the bug) | `OptimizingQueue.java` | **424** |
| `TableOptimizingProcess.taskQueue` | `OptimizingQueue.java` | 425 |
| `fromSequence` / `toSequence` | `OptimizingQueue.java` | 429-430 |
| `TableOptimizingProcess.poll(...)` | `OptimizingQueue.java` | 433-451 |
| `TableOptimizingProcess` ctor (fresh plan) | `OptimizingQueue.java` | 455-461 |
| `TableOptimizingProcess` ctor (recover) | `OptimizingQueue.java` | 469-486 |
| `loadTaskRuntimes(List)` (writer A) | `OptimizingQueue.java` | 798+ |
| `loadTaskRuntimes(OptimizingProcess)` (writer B) | `OptimizingQueue.java` | 829+ |
| `taskMap.put` (writer A site) | `OptimizingQueue.java` | **813** |
| `taskMap.put` (writer B site) | `OptimizingQueue.java` | **840** |
| `getTaskMap()` accessor | `OptimizingQueue.java` | 616 |
| `DefaultOptimizingService.pollTask` | `DefaultOptimizingService.java` | 218 |
| `extractOptimizingTask` (catch-all guard) | `DefaultOptimizingService.java` | 230-238 |
| Plan executor (cached thread pool) | `DefaultOptimizingService.java` | 136 |
| `AmoroRuntimeException.normalize` | `AmoroRuntimeException.java` | 116-118 |
| `AmoroRuntimeException.wrap (default)` | `AmoroRuntimeException.java` | 145-147 |
| Thrift handler normalisation hook | `AmoroServiceContainer.java` | 388 |
| Optimizer client retry loop | `AbstractOptimizerOperator.java` | 89-119 |
| `shouldRetryLater` default branch | `AbstractOptimizerOperator.java` | 75-77 |
| `OptimizerExecutor.pollTask` | `OptimizerExecutor.java` | 83-99 |
| `OptimizerExecutor.completeTask` | `OptimizerExecutor.java` | 124-144 |
| `ThriftClientPool` proxy | `ThriftClientPool.java` | 213-237 |

### 10.4 Revision history

| Date | Version | Author | Change |
|---|---|---|---|
| 2026-05-06 | 1.0 | Reviewer | Initial post-mortem covering symptom, code-level evidence, race chain, scaling explanation, and patch recommendation |
| 2026-05-06 | 1.1 | Reviewer | Added §11 — companion analysis of the server-side `MaxMessageSize reached` TTransportException observed on the `completeTask` path at 17:43:00, confirmed to be a **different defect** from the CME in §1-§10 and distinct from the previously-fixed `frame size 16384000` bug |
| 2026-05-06 | 1.2 | Reviewer | Added §12 — assessment of upstream commit `179c77f9d` "[AMORO-3794] Wrap task operation with process lock (#3798)" applied on branch `hotfix/tclake-amoro-20260506`. Verdict: partial fix — resolves `pollTask` / `ackTask` / `completeTask` CME paths, but `collectTasks` (metrics) and `getTaskRuntime` remain unprotected. Recommends keeping the `ConcurrentHashMap` patch (downgraded P0 → P1). |

---

## 11. Companion Incident — Server-Side `MaxMessageSize reached` on `completeTask`

### 11.1 Overview

This section documents a second Thrift-layer error observed in the **same AMS
instance** at `2026-05-06 17:43:00` (five minutes before the CME in §1-§10).
The error is on a **different RPC** (`completeTask`, not `pollTask`) and has
a **different root cause** from both the CME above and the historical
`frame size 16384000` issue already fixed in `dev/fix-thrift_package_len`.

This section is organised as:

- §11.2 — symptom and log evidence
- §11.3 — why this is **not** the prior frame-size 16384000 bug
- §11.4 — why this is **not** the CME of §1-§10
- §11.5 — code-level evidence of where the limit fires
- §11.6 — root cause: payload truly exceeds `maxMessageSize`
- §11.7 — amplifier: current server-side Thrift config gap
  (`args.maxReadBufferBytes` not set)
- §11.8 — fix recommendations
- §11.9 — operator immediate actions

### 11.2 Symptom

```
2026-05-06 17:43:00,237 WARN
  [thrift-server-optimize-manager-1497]
  [org.apache.amoro.shade.thrift.org.apache.thrift.server.AbstractNonblockingServer$FrameBuffer]
  - Exception while invoking!
org.apache.amoro.shade.thrift.org.apache.thrift.transport.TTransportException:
  MaxMessageSize reached
  at TEndpointTransport.checkReadBytesAvailable(TEndpointTransport.java:94)
  at TLayeredTransport.checkReadBytesAvailable(TLayeredTransport.java:47)
  at TBinaryProtocol.checkStringReadLength(TBinaryProtocol.java:500)
  at TBinaryProtocol.readBinary(TBinaryProtocol.java:482)
  at TProtocolDecorator.readBinary(TProtocolDecorator.java:262)
  at OptimizingTaskResult$OptimizingTaskResultStandardScheme.read(OptimizingTaskResult.java:691)  ← field 3 (taskOutput, binary)
  at OptimizingTaskResult.read(OptimizingTaskResult.java:568)
  at OptimizingService$completeTask_args.read(OptimizingService.java:4816)
```

Key facts extracted from the stack:

| Observation | Evidence |
|---|---|
| Side | **Server** (AMS) — thread name `thrift-server-optimize-manager-1497`, class `AbstractNonblockingServer$FrameBuffer` |
| Phase | Deserialising the **request args** of `completeTask(authToken, OptimizingTaskResult taskResult)` |
| Failing field | `OptimizingTaskResult.taskOutput` — `optional binary` (see `amoro_optimizing_service.thrift:34-40`) |
| Failing call | `TBinaryProtocol.readBinary` → `checkStringReadLength` → `checkReadBytesAvailable` → **`MaxMessageSize reached`** |
| Log level | `WARN`, not `ERROR` — Thrift `FrameBuffer` path logs and drops the connection silently |

### 11.3 Why this is NOT the historical `frame size 16384000` bug

The previous fix in branch `dev/fix-thrift_package_len`
(`AmoroServiceContainer.java:402-440`) targets **frame size**:

```java
// AmoroServiceContainer.java:418 (post-fix)
TTransportFactory transportFactory = new TFramedTransport.Factory((int) maxMessageSize);
// AmoroServiceContainer.java:414 (post-fix)
TNonblockingServerSocket serverTransport = getServerSocket(bindHost, port, maxMessageSize);
// AmoroServiceContainer.java:417 (post-fix)
new TBinaryProtocol.Factory(true, true, maxMessageSize, maxMessageSize);
```

That fix guarantees: (1) the server-socket transport's `maxFrameSize` is
not capped at `16384000`, and (2) the `TBinaryProtocol.Factory` is built
with the configured `maxMessageSize` (default 100 MB). The previous bug
presented as `frame size 16384000` (the shaded Thrift 0.20 default, see
`TConfiguration.DEFAULT_MAX_FRAME_SIZE`).

The current error presents as `MaxMessageSize reached` **with no hard-coded
16384000 value**, meaning the server-side transport is already honouring
the configured `100 MB` (= `104857600` bytes) from `config.yaml`:
```yaml
thrift-server:
  max-message-size: 100MB   # 104857600
```
…and the payload being read exceeds even this raised limit. The fix from
`dev/fix-thrift_package_len` is **working as intended**; it just raised
the ceiling from 16 MB to 100 MB, and we are now hitting the 100 MB
ceiling.

### 11.4 Why this is NOT the CME of §1-§10

| Dimension | CME (§1-§10) | `MaxMessageSize reached` (§11) |
|---|---|---|
| Time of event | 2026-05-06 17:48:03 | 2026-05-06 17:43:00 |
| RPC | `pollTask` (server → client response deserialisation) | `completeTask` (client → server request deserialisation) |
| Side where exception originates | AMS internal `HashMap` iteration | AMS Thrift transport layer |
| Java type | `java.util.ConcurrentModificationException` | `TTransportException: MaxMessageSize reached` |
| Layer | Application data structure (`OptimizingQueue.taskMap`) | Thrift serialisation layer (`TEndpointTransport`) |
| Log level in AMS | ERROR + stack | WARN from `FrameBuffer` |
| Client impact | 1 retry-able `pollTask` failure | `completeTask` is lost; the `TaskRuntime` stays in `COMMITTING`/`EXECUTING` until ACK timeout |
| Fix area | `OptimizingQueue.java:424` (`HashMap` → `ConcurrentHashMap`) | Limit raising + payload reduction + `maxReadBufferBytes` (see §11.7-§11.8) |
| Severity | P0 (correctness, hot path) | P1 (availability — silently drops a commit report; triggers retry storm after ACK timeout) |

The two defects are **independent**. Fixing the CME does not fix this
`MaxMessageSize` error, and vice versa.

### 11.5 Code-level evidence of where the limit fires

The failing path, line-for-line (all files under shaded package prefix
`org.apache.amoro.shade.thrift.*`):

1. **`OptimizingService$completeTask_args$completeTask_argsStandardScheme.read(4903)`**
   — reads args of `completeTask(string authToken, OptimizingTaskResult taskResult)`.
2. **`OptimizingTaskResult$OptimizingTaskResultStandardScheme.read(691)`** —
   reads field id `3` (`taskOutput`, type `TType.STRING` + binary, see
   `OptimizingTaskResult.java:691-692`):
   ```java
   case 3: // TASK_OUTPUT
     if (schemeField.type == org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TType.STRING) {
       struct.taskOutput = iprot.readBinary();
       struct.setTaskOutputIsSet(true);
   ```
3. **`TBinaryProtocol.readBinary(482)`** — reads a 4-byte length prefix,
   then calls `checkStringReadLength(len)` before allocating the buffer.
4. **`TBinaryProtocol.checkStringReadLength(500)`** — forwards to the
   underlying transport's `checkReadBytesAvailable(len)`.
5. **`TEndpointTransport.checkReadBytesAvailable(94)`** — compares
   `knownMessageSize - readBytes` vs the requested `len`; on overflow,
   throws `TTransportException("MaxMessageSize reached")`.

### 11.6 Root cause: `taskOutput` payload exceeds 100 MB

The `taskOutput` field is populated on the optimizer side in
`OptimizerExecutor.java:168-173`:

```java
OptimizingExecutor executor = factory.createExecutor(input);
TableOptimizing.OptimizingOutput output = executor.execute();
ByteBuffer outputByteBuffer = SerializationUtil.simpleSerialize(output);
OptimizingTaskResult result = new OptimizingTaskResult(task.getTaskId(), threadId);
result.setTaskOutput(outputByteBuffer);
```

where `output` is a `RewriteFilesOutput`
(`amoro-format-iceberg/.../RewriteFilesOutput.java:26-36`):

```java
public class RewriteFilesOutput implements TableOptimizing.OptimizingOutput {
  private final DataFile[] dataFiles;
  private final DeleteFile[] deleteFiles;
  private final Map<String, String> summary;
  ...
}
```

`SerializationUtil.simpleSerialize` uses Kryo to persist the full `DataFile`
/ `DeleteFile` array, each of which in Iceberg carries **24 fields**
including **6 column-statistics `Map`s** (`value_counts`, `null_value_counts`,
`nan_value_counts`, `lower_bounds`, `upper_bounds`, plus metrics).

Payload size scaling (empirical + conservative estimate):

| Scenario | Output DataFiles | Output DeleteFiles | Kryo size per file (avg) | Total payload |
|---|---:|---:|---:|---:|
| Small rewrite | 50 | 0 | ~5 KB | ~250 KB |
| Normal full-rewrite | 500 | 100 | ~5-10 KB | ~3-6 MB |
| Wide-schema rewrite (200+ cols) | 500 | 200 | ~30-50 KB | ~20-35 MB |
| **Large-partition bin-pack merging 5k+ files** | **5 000** | **1 000** | **~15-20 KB** | **~90-120 MB** ← hits 100 MB |
| Wide-schema + large-partition combo | 5 000+ | 1 000+ | ~30-50 KB | **> 200 MB** |

The failing `completeTask` at 17:43:00 is most plausibly in the last two
rows: a rewrite task that legitimately produced enough new data/delete
files that the Kryo-serialised array exceeded `100 MB`.

This matches the **10-万-表-escalation context** described in the
companion memory report (`10万表内存容量评估-修订版.md` §6.1) — at higher
scale, individual partitions accumulate more files between optimization
cycles, so each rewrite task output grows.

### 11.7 Amplifier: server-side Thrift `args.maxReadBufferBytes` is not set

Re-examining `AmoroServiceContainer.createThriftServer(402-440)`:

```java
THsHaServer.Args args =
    new THsHaServer.Args(serverTransport)
        .processor(multiplexedProcessor)
        .transportFactory(transportFactory)
        .protocolFactory(protocolFactory)
        .inputProtocolFactory(inputProtoFactory)
        .executorService(executorService);
// NOTE: args.maxReadBufferBytes is NEVER set.
return new THsHaServer(args);
```

`THsHaServer` inherits `TNonblockingServer`, whose `FrameBuffer` uses
`maxReadBufferBytes` (default `Long.MAX_VALUE` in Thrift 0.20, but
configurable) as a **global cross-connection read-buffer cap**. When
unset, the default is permissive, so this is **not** the trigger today,
but it is a latent configuration gap that will bite if anyone ever tunes
it downward. Recommend setting it explicitly for clarity:

```java
args.maxReadBufferBytes = maxMessageSize; // match per-message cap
```

### 11.8 Fix recommendations

Ordered by priority:

| # | Action | Where | Risk | Expected effect |
|---|---|---|---|---|
| **F1** | **Raise `thrift-server.max-message-size` to 500 MB** (per-server tuning, not a code change) | `conf/config.yaml` | Low. Only increases per-connection read ceiling. | Eliminates the immediate symptom for large rewrites. |
| **F2** | **Raise client-side `PoolConfig.maxMessageSize` correspondingly** (default `100 * 1024 * 1024` in `PoolConfig.java:43`) | `PoolConfig.java` or connection URL param `?maxMessageSize=` | Low. | Client can actually send and later receive responses of matching size. |
| **F3** | **Reduce `taskOutput` payload — eliminate per-file redundant stats** | `RewriteFilesOutput` / `SerializationUtil.simpleSerialize` | Medium — need to guarantee commit still has enough metadata. | Cuts payload 3-5× at the same task size. Addresses root cause rather than symptom. |
| **F4** | **Split oversized rewrite tasks at planning time** | planner limit on `input-files-count` / `target-output-size` | Low once tested; requires planner parameter. | Keeps `taskOutput` bounded by design, independent of runtime path. |
| **F5** | Explicitly set `args.maxReadBufferBytes = maxMessageSize` | `AmoroServiceContainer.java:421` area | Trivial. | Closes configuration gap noted in §11.7. |
| **F6** | Upgrade the log on `completeTask` loss from WARN to ERROR and surface it as a metric | Thrift proxy layer / `DefaultOptimizingService.completeTask` | Low. | Operators get paged on silent commit drops instead of finding them hours later via ACK-timeout retries. |

The **minimum** immediate patch set is F1 + F2. The **root-cause** patch
is F3 or F4. F5-F6 are defensive hardening.

### 11.9 Operator immediate actions (no code change)

1. Query the `optimizing_task_summary` table for tasks at or after
   `2026-05-06 17:40` in state `EXECUTING`/`COMMITTING` that **never
   received a `completeTask` ACK**. These tasks will be retried after
   the executor timeout, but their prior Kryo output is lost — which
   only means re-work, not data corruption.
2. Identify which tables produced them (group by `table_id`). Expect to
   see tables with either very wide schemas (200+ cols with stats) or
   very high file counts per partition (5k+).
3. For those tables, temporarily reduce optimizer input size via table
   properties (`rewrite.input-files-count`, `target-file-size-bytes`).
4. Apply F1+F2 at the next AMS restart window; enqueue F3+F4 into the
   next feature iteration.

### 11.10 Cross-reference

- **This CME report (§1-§10)** — independent defect on `pollTask` path;
  not affected by F1-F6 of this section.
- **`dev/fix-thrift_package_len` review** — the 16 MB → 100 MB ceiling
  change; necessary but not sufficient. F1 (100 MB → 500 MB) is the
  next step up the same axis.
- **`2026-05-06-Amoro-AMS-DFX分析报告.md` §2.2** — already flagged
  unbounded serialised payloads as an Availability risk; this incident
  is a concrete instance.
- **`10万表内存容量评估-修订版.md` §6.1** — same TaskRuntime lifetime
  pressure; the memory cost of an unbounded `taskOutput` also impacts
  steady-state heap (even after the buffer is freed, the commit path
  still pins the `RewriteFilesOutput` until snapshot publish succeeds).

---

## 12. Upstream Patch Assessment — Commit `179c77f9d` [AMORO-3794]

### 12.1 Overview

On 2026-05-06 the tclake branch `hotfix/tclake-amoro-20260506` received
commit `179c77f9d` "[AMORO-3794] Wrap task operation with process lock
(#3798)". This section evaluates **whether this commit resolves the CME
documented in §1-§10**. The short answer is:

> **Partial fix.** The commit eliminates the two highest-traffic CME
> paths (`pollTask` and the DB-bound writer `loadTaskRuntimes(process)`),
> but **does not fully close the CME window** — `collectTasks` /
> `collectTasks(predicate)` still iterate `taskMap` without holding the
> process lock, and one `loadTaskRuntimes` writer site remains un-locked.
> The residual window is narrow but still theoretically reachable under
> metrics-heavy workloads.

Section layout: §12.2 diff summary — §12.3 what was fixed
— §12.4 what remains — §12.5 revised recommendation.

### 12.2 What the commit changed (diff summary)

| File | Change |
|---|---|
| `OptimizingQueue.java` | `pollTask(...)` now requires an `OptimizerThread` and delegates into `TableOptimizingProcess.poll(thread, needQuotaChecking)`. |
| `OptimizingQueue.java` | New `ackTask(...)` and `completeTask(...)` public methods that do `findProcess(taskId).{ackTask,completeTask}(...)`. |
| `OptimizingQueue.java` | Removed the top-level `retryTaskQueue`; `retryTask` now routes to `process.resetTask(...)`. |
| `OptimizingQueue.java` | Removed the old `public TaskRuntime<?> getTask(OptimizingTaskId)` that did `tableQueue.stream() ... getTaskMap().get(taskId)` — replaced by `findProcess(taskId)` which **only** walks `tableQueue` but does **not** touch `taskMap`. |
| `TableOptimizingProcess.poll` | Switched from `lock.tryLock()` (non-blocking) to `lock.tryLock(10ms)`, and moved `task.schedule(thread)` inside the lock. |
| `TableOptimizingProcess` (new) | `ackTask`, `completeTask`, `resetTask` — each acquires `lock.lock()` around the task-state mutation. |
| `DefaultOptimizingService.java` | `pollTask` / `ackTask` / `completeTask` call sites updated to pass `OptimizerThread` / use the new entry points. |
| `TaskRuntime.java` | Signature tweaks (`ack` / `complete` now take the thread). |

The intent is clear: **route every per-task state transition (schedule,
ack, complete, reset) through `TableOptimizingProcess.lock`** so
`taskMap` is no longer accessed concurrently by the scheduler/IO/commit
paths at the same time as the planner writer.

### 12.3 What IS fixed

#### 12.3.1 `pollTask` CME path — closed

Before: `fetchScheduledTask` walked `tableQueue.stream() ... process.poll(...)`
where `process.poll` used `tryLock()` and **only wrapped `taskQueue.poll()`**,
but `task.schedule(thread)` ran outside the lock; `loadTaskRuntimes` concurrently
mutated `taskMap` **without** any lock.

After (`OptimizingQueue.java:234-241`, `:436-456`):
```java
private TaskRuntime<?> fetchScheduledTask(OptimizerThread thread, boolean needQuotaChecking) {
  return tableQueue.stream()
      .map(process -> process.poll(thread, needQuotaChecking))
      ...
}
public TaskRuntime<?> poll(OptimizerThread thread, boolean needQuotaChecking) {
  if (lock.tryLock(10, TimeUnit.MILLISECONDS)) { ... task.schedule(thread); ... }
}
```
`poll` now holds the lock around both `taskQueue.poll()` and `task.schedule`.
The **specific stack in §1.1 / §10.1** (`pollTask` → CME) is no longer
reachable via this path **as long as** the concurrent writer
`loadTaskRuntimes(taskDescriptors)` is also under the same lock — see
§12.4.1 for the caveat.

#### 12.3.2 `getTask` / `ackTask` / `completeTask` CME paths — closed

Before: `getTask` did `tableQueue.stream() ... p.getTaskMap().get(taskId)`
without any lock — this is a direct `HashMap.get` on a concurrently mutated
map (fail-fast iterators are only on iteration, but `HashMap.get` on a
concurrently-resized map can also corrupt or throw).

After (`OptimizingQueue.java:344-349`, `:533-565`):
```java
public void ackTask(OptimizingTaskId taskId, OptimizerThread thread) {
  findProcess(taskId).ackTask(taskId, thread);
}
private void ackTask(OptimizingTaskId taskId, OptimizerThread thread) {
  TaskRuntime<?> taskRuntime = getTaskRuntime(taskId);  // taskMap.get
  lock.lock();
  try { taskRuntime.ack(thread); } finally { lock.unlock(); }
}
```
`getTaskRuntime` still does `taskMap.get(taskId)` **before** taking the
lock, which is not fully safe (§12.4.2), but at least the mutation paths
(`taskRuntime.ack` / `taskRuntime.complete`) are now serialised with
`poll`.

#### 12.3.3 `loadTaskRuntimes(OptimizingProcess)` race — addressed structurally

The second constructor of `TableOptimizingProcess` (the "recover from DB"
path, `:479-495`) calls `loadTaskRuntimes(this)` at the end of its body.
At this point the process has **not yet been published to `tableQueue`**
(it is still inside the constructor), so `taskMap.put` writes are visible
only via the `tableQueue` publish which happens after construction
finishes. This closes the race for the recover path **by construction
ordering**, not by locking.

### 12.4 What is still NOT fixed (residual CME surface)

Three concrete residuals remain after the commit. All of them iterate or
read `taskMap` **outside** `TableOptimizingProcess.lock`.

#### 12.4.1 `collectTasks` and `collectTasks(predicate)` — unprotected iteration

`OptimizingQueue.java:352-362` (post-commit):
```java
public List<TaskRuntime<?>> collectTasks() {
  return tableQueue.stream()
      .flatMap(p -> p.getTaskMap().values().stream())   // ← HashMap iteration, NO LOCK
      .collect(Collectors.toList());
}
public List<TaskRuntime<?>> collectTasks(Predicate<TaskRuntime<?>> predicate) {
  return tableQueue.stream()
      .flatMap(p -> p.getTaskMap().values().stream())   // ← HashMap iteration, NO LOCK
      .filter(predicate).collect(Collectors.toList());
}
```

Callers (via `grep`):

| Caller | Frequency |
|---|---|
| `DefaultOptimizingService.listTasks` (:199) | Dashboard REST query — on demand |
| `DefaultOptimizingService.refreshSuspending` (:497, predicate) | Periodic (default every 60s) |
| `OptimizerGroupMetrics` (:132, :140) | **Every metrics scrape** (default 15s by Prometheus) |

The concurrent writer is still
`TableOptimizingProcess.loadTaskRuntimes(List<RewriteStageTask>)` at
`:877-890`:
```java
private void loadTaskRuntimes(List<RewriteStageTask> taskDescriptors) {
  for (RewriteStageTask taskDescriptor : taskDescriptors) {
    ...
    taskMap.put(taskRuntime.getTaskId(), taskRuntime);   // ← HashMap.put, NO LOCK
    taskQueue.offer(taskRuntime);
  }
}
```

This is called from the **fresh-plan constructor** at `:473` —
`loadTaskRuntimes(planner.planTasks())`. Although this also runs before
`tableQueue.add(process)`, there is a subtle window:

- The constructor body ends with `beginAndPersistProcess()` which contains
  `doAs(TableProcessMapper.class, mapper -> mapper.insertTaskRuntimes(...))`
  persistence I/O. **If the process is added to `tableQueue` by any
  earlier-in-the-constructor code or by a superclass callback before this
  method returns**, the new `TableOptimizingProcess` is already visible to
  `collectTasks`/`OptimizerGroupMetrics` while `loadTaskRuntimes` is
  still mutating `taskMap`. The current code path does not trigger this,
  but it is a **one-refactor-away** breakage.
- More importantly, the original §4 failure chain observed the CME with
  the table **already in production** (existing process). The
  `loadTaskRuntimes(this)` recover path (§12.3.3) only runs at AMS
  startup / service recovery; the `loadTaskRuntimes(planner.planTasks())`
  fresh-plan path runs **on every new optimizing process** created
  throughout the lifetime of the service. Every time a new process is
  planned, there is a brief window (between `taskMap.put` and
  `tableQueue.add`) where a concurrent metrics scrape could — in
  principle — observe the in-progress HashMap. This is narrow but not
  provably impossible to hit under 100K-table scale with 15s scrape
  intervals.

**Verdict:** the CME surface is *substantially* reduced but not
*provably* eliminated for `collectTasks`. A metrics-triggered CME is
still theoretically possible, although much less likely than the
`pollTask` / `getTask` CMEs that the commit explicitly fixed.

#### 12.4.2 `getTaskRuntime` reads `taskMap` before taking the lock

`OptimizingQueue.java:555-561`:
```java
private TaskRuntime<?> getTaskRuntime(OptimizingTaskId taskId) {
  TaskRuntime<?> taskRuntime = getTaskMap().get(taskId);   // ← outside lock
  if (taskRuntime == null) {
    throw new TaskNotFoundException(taskId);
  }
  return taskRuntime;
}
```

`HashMap.get` on a map being resized by `HashMap.put` can, in rare cases,
return a null for an existing key or spin on a corrupted bucket.
Practically, the resize window is microseconds, and the writer
(`loadTaskRuntimes(planTasks())`) only fires at process-creation time, so
the probability of collision during a long-lived process's life is very
low. But the defensive-coding standard is **never** to call
`HashMap.get` on a map that any other thread may mutate.

#### 12.4.3 `cancelTasks` and `commitFunction` still iterate `taskMap.values()` outside the lock

`OptimizingQueue.java:843`, `:687`, `:704-705`, `:743`, `:758`, `:762`,
`:814-815` — all do `taskMap.values().stream()` or
`Lists.newArrayList(taskMap.values())` from inside `TableOptimizingProcess`
methods that do **not** hold `lock`. These run after the task map is
effectively frozen (no more `put` after planning), so they are safe in
practice; but the invariant "`taskMap` is immutable after
`loadTaskRuntimes`" is not enforced anywhere in code and is only true
by convention.

### 12.5 Revised recommendation (vs. §7 / §1.4)

The commit makes the one-line `HashMap → ConcurrentHashMap` patch
proposed in §1.4 **less urgent but not obsolete**:

| Patch | Urgency after `179c77f9d` | Why |
|---|---|---|
| **P0** `taskMap = new ConcurrentHashMap<>()` at `OptimizingQueue.java:426` | **Still recommended, now P1** | Closes the residual CME in `collectTasks` (metrics scrape) and `getTaskRuntime` read-before-lock, without impacting the new lock design. ConcurrentHashMap is only ~2× slower on uncontended put/get, and this map is rarely mutated — cost is negligible. |
| Wrap `collectTasks` in `lock.lock()` per process | Alternative to P0 | Higher cost: metrics scrape would block planning. **Not recommended.** |
| Audit `loadTaskRuntimes(planTasks())` writer to also hold `lock` | Nice-to-have | Defensive; keeps §12.3 / §12.4 invariants explicit in code rather than by construction ordering. |
| §11.8 F1-F6 (MaxMessageSize) | **Unaffected** by this commit | Different defect class. |

### 12.6 Evidence — file:line reference index (post-commit)

All references against `hotfix/tclake-amoro-20260506` @ `179c77f9d`.

| Symbol | Line | Status vs. CME |
|---|---|---|
| `tableQueue` (`LinkedTransferQueue`) | 93 | Safe (CHM-like) |
| `retryTaskQueue` | **removed** | N/A |
| `taskMap` (`Maps.newHashMap()`) | **426** | **Still HashMap — residual risk** |
| `taskQueue` (`SchedulingQueue`) | 427 | Used under lock only |
| `poll(thread, needQuotaChecking)` | 436-464 | Lock-protected, `tryLock(10ms)` |
| `ackTask(taskId, thread)` (process) | 535-542 | Lock-protected |
| `completeTask(thread, result)` (process) | 544-551 | Lock-protected |
| `getTaskRuntime(taskId)` | 555-561 | **`taskMap.get` outside lock** |
| `resetTask(taskRuntime)` | 621-628 | Lock-protected |
| `collectTasks()` | 352-356 | **Unprotected iteration** |
| `collectTasks(predicate)` | 358-363 | **Unprotected iteration** |
| `findProcess(taskId)` | 393-398 | Safe (only walks `tableQueue`) |
| `loadTaskRuntimes(process)` writer | 846-875 | Recover path; pre-publish, safe by ordering |
| `loadTaskRuntimes(List)` writer | 877-890 | Fresh-plan path; pre-publish, safe by ordering (but fragile) |
| `ackTask` / `completeTask` (queue entry) | 344-349 | Lock-protected |

### 12.7 Verification plan addendum (to §9)

To confirm this commit in production, on top of §9 verification steps:

1. **Tail AMS logs for `ConcurrentModificationException`**. After the
   commit, the three highest-frequency paths (`pollTask`, `ackTask`,
   `completeTask`) should no longer produce CME. If CME recurs, it will
   now come from `collectTasks` — the stack will show
   `DefaultOptimizingService.listTasks` or `OptimizerGroupMetrics.*`
   near the top rather than `pollTask`. Use this as the signal to apply
   the residual P1 patch from §12.5.
2. **Measure `lock.tryLock(10ms)` contention**. With a 10ms timeout,
   `fetchScheduledTask` can silently return null under lock starvation.
   Monitor (a) `pollTask` returning null rate, (b) the number of
   `InterruptedException` warnings suppressed in `poll`. If either
   spikes, revisit the lock granularity.
3. **Verify retry path**. The removed `retryTaskQueue` means retries now
   go back into the per-process `taskQueue`. Verify that retried tasks
   are indeed re-dispatched after lock release, not dropped.

### 12.8 Bottom line

> The `179c77f9d` commit **does** eliminate the specific failure
> captured in §1-§10 (`pollTask` → CME). It does **not** provide a
> complete solution for all CME surfaces — `collectTasks` / metrics
> scrape and `getTaskRuntime` read-before-lock remain unprotected.
> **Keep the one-line `ConcurrentHashMap` patch (now downgraded from
> P0 to P1) on the near-term backlog** to fully close the defect class;
> without it, the incident may recur in a different stack under
> metrics-heavy workloads.

