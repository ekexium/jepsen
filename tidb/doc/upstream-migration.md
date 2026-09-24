# Jepsen 0.3.14 integration

This branch replays PingCAP's `testing_ci` changes from `b778e6ff` on
upstream Jepsen `v0.3.14` (`e5c458ad`). The framework remains published under
`org.clojars.pingcap/jepsen`, now at `0.3.14-SNAPSHOT`, and uses Elle 0.2.7.

## Build and runtime

Use JDK 21 or newer and Leiningen. Install the local framework before building
TiDB so the JAR includes the PingCAP framework changes:

```sh
cd jepsen
lein install
cd ../tidb
lein uberjar
```

The result is `tidb/target/tidb-0.1.1-SNAPSHOT-standalone.jar`.
Graphviz (`dot`) and gnuplot are needed for diagnostic graphs. When invoking
Java directly, use `java -Djava.awt.headless=true -cp <jar> tidb.core ...`.
Leiningen and the derived CI image set headless mode automatically.

The current CI control image `hub.pingcap.net/qa/jepsen-control-base:250611`
contains Java 8. Build its Java 21 derivative before switching the JAR:

```sh
docker build -f docker/control/Dockerfile-java21 \
  -t jepsen-control-java21:local docker/control
```

This retains the existing CI tools, adds JDK 21 and Graphviz, and selects JDK 21
in the login shell used by the runner. `BASE_IMAGE` can override the CI base.
This Dockerfile targets the retained Debian 9 CI base; `BASE_IMAGE` may also
pin that base by digest. The node images and TiDB/TiKV/PD binaries do not require
Java upgrades.
Publishing this image, uploading the JAR, and changing the transaction-qa runner
are separate deployment steps.

## Preserved PingCAP behavior

- TiDB/TiKV/PD deployment and PD microservices; TiDB X SYSTEM TiDB and worker
  startup, readiness checks, and `--enable-tidbx` gating.
- SQL connection and transaction initialization, optimistic/pessimistic/mixed
  modes, follower reads, table cache, index paths, single-statement writes,
  transaction metadata, and existing retry/error handling.
- Process kill/stop/pause, scheduling, partition, netem, failpoint, and long
  recovery configurations. Worker faults remain TiDB X only.
- Foreign-key bank `:total-moved` integrity checks and multitable MVCC diagnostics.
- Transaction metadata in operation logs and node names in worker logs.

The stock upstream daemon helper already handles `:env`; the obsolete fork
implementation is not needed. The retained TSO graph helper is still inactive
in the checker, as it was at the fork head.

## Adaptations

- Append and transactional register workloads use Elle models. RC selects
  `:read-committed`; RR selects `:strong-snapshot-isolation`, retaining the old
  realtime ordering checks while adding G1 checks and allowing SI write skew.
- A small wrapper eagerly builds history pair indices to avoid Elle 0.2.7's
  known sparse-history deadlock. It can be removed after adopting a release
  containing Elle commit `fa0e699ec3488b9dfc660be4fcf7e9547bbea4e0`.
- Generators use the new pure API. Operation sequences and repeated operations
  are distinguished explicitly. Nemesis random intervals retain a uniform
  distribution; scheduling now follows upstream's invocation-based generator.
- Every workload heals faults at the end. Workloads with final reads wait for
  `--recovery-time`; their key tracker wraps the whole generator. Final reads
  include the highest key and a partial last batch.
- PD leader discovery runs in the nemesis worker instead of blocking the
  central generator. PD leader partitions and interrupted restart-without-PD
  tests now have complete final recovery actions.
- Multitable bank diagnostics run in the checker with the completed history;
  client teardown can run before setup in the new framework.

## Validation

```sh
cd tidb
lein update-in :aot empty -- test
cd ../jepsen
lein test jepsen.print-test jepsen.generator.worker-node-test \
  jepsen.tests.bank-test jepsen.tests.cycle.core-test
```

Regression histories cover aborted writes, permitted indeterminate outcomes,
valid SI write skew, realtime visibility, sparse histories, and final key reads.
Nemesis tests simulate operation sequences and mock only remote I/O boundaries.

Before switching CI, run real TiDB and TiDB X smoke tests, including PDML,
foreign-key bank, and failpoint activation. Offline generator tests and JAR
checks do not establish that the deployed database binaries support every
configured mode or that a remote failpoint was actually hit.
