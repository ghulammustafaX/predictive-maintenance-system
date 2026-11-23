# Module 06 isolated acceptance preparation

The offline GitHub Actions workflow uses synthetic fixtures and does not exercise a broker or certify end-to-end deployment. The Compose harness is prepared for a later separately reviewed, adequately provisioned run. It has not been started or tested against a real broker in this phase.

## Isolation

Use a fresh `pms-m6-<run-id>-<attempt>` Compose project and the matching `m6-acceptance-<run-id>-<attempt>` group. The configuration uses its own internal network, ZooKeeper, Kafka, InfluxDB, and project-named checkpoint and InfluxDB volumes, with no host ports. Its fixed topics `m6-test-input` and `m6-test-output` exist only on that isolated broker. Credentials in the Compose file are disposable test-only values.

The raw model file and scaler directory are mounted read-only. The guard checks their known hashes and the manifest before accepting the rendered Compose configuration, then queries Docker's container, volume and network inventories and rejects collisions or an unavailable inventory. These Docker calls are read-only. Artifact hashes and source code must be reviewed independently; the guard does not authenticate a compromised checkout or replace container runtime isolation. Do not use the uncertified engineered checkpoint.

Before any future startup, an operator must check memory, CPU, disk, existing project names, mounts, image and build provenance. Module 04's hosted preflight uses at least 4 GiB available RAM, 10 GiB disk and 2 CPUs; this is a reference, not a demonstrated Module 06 sizing guarantee. Abort on insufficient headroom or any guard failure. Do not use local production services or existing resources.

The exact guarded render can be checked without starting services (paths must refer to the approved local raw artifacts):

```bash
export M6_PROJECT=pms-m6-123-1 M6_GROUP=m6-acceptance-123-1
export M6_MODEL_PATH=/absolute/path/to/lstm_raw_20260626T082955Z.pt
export M6_SCALER_DIR=/absolute/path/to/validated/scalers
docker compose -p "$M6_PROJECT" -f docker-compose.module6-verification.yml config --format json > "$RUNNER_TEMP/m6-compose.json"
python tests/module06_compose_guard.py "$RUNNER_TEMP/m6-compose.json"
```

The placeholder project must be replaced with a unique run-specific identity, and the group must match it. Do not continue when a project or associated volume already exists. The guard must run against the exact render and environment that will be used to start the project. Run only on an isolated host or hosted runner under an explicitly reviewed execution plan. If a project was started, cleanup must target that exact validated project only; never invoke a broad Docker cleanup or touch other projects.

## Probe scope and remaining acceptance

The prepared probe publishes synthetic finite raw-24 vectors to the isolated input. It checks that 29 records produce no output and that cycle 30 produces one prediction matching direct inference with the verified raw model and expected identity/version metadata. It is a happy-path starting point, not a completed acceptance suite.

Before real-broker acceptance, add and review bounded probes for non-finite and malformed records, missing identity, ordering and gaps, invalid stub-model outputs, checkpoint and committed-offset behavior, clean restart and continuation, output/commit failures, and documented at-least-once duplicates. Verify the checkpoint mount permissions and verify that worker restart uses the same project-scoped volume. Capture worker logs and artifacts without credentials. All those criteria, plus actual execution and cleanup evidence, remain outstanding.
