# CI verification report

Verification date: 21 November 2026. The verified post-merge run is [PMS Python CI #7](https://github.com/ghulammustafaX/predictive-maintenance-system/actions/runs/44868579033), which checked out `c9e99ab2f3cda9c8c6ea257499293e300b9b326f` on `main`. Both jobs completed successfully. This report records that run; future runs and repository settings can change.

## Workflow and isolation

The workflow is [`.github/workflows/python-ci.yml`](../.github/workflows/python-ci.yml). It runs on pull requests targeting `main`, pushes to `main`, and manual dispatch. Both jobs use `ubuntu-24.04` and Python 3.11. The offline job installs pytest and the simulator, ingestion, ML, streaming and prediction requirements; the broker job installs pytest plus simulator and ingestion requirements and builds the simulator test image.

The offline job runs the tracked `tests/` suite, deselecting only the two classes containing four broker-dependent tests. It checks its JUnit report and fails if any selected test is skipped. The separate integration job runs those four tests against real Kafka, ZooKeeper, Mosquitto and the MQTT bridge in the `pms-ci-m1` Compose project defined by [`docker-compose.m1-integration-test.yml`](../docker-compose.m1-integration-test.yml). ZooKeeper readiness is checked using `srvr`; Kafka waits for ZooKeeper health and has a broker CLI readiness check. Compose waits for service health, the workflow verifies connectivity from the isolated Docker network, and failures trigger service-state and log collection. Cleanup runs with `if: always()` and removes the test containers and network without a volume-deletion flag.

Both jobs generate JUnit XML and upload it as a workflow artifact: `pms-regression-junit` and `pms-broker-junit`. The verified broker job checked that the report contained exactly four tests and zero failures, errors or skips.

## Results

| Job | Passed | Failed | Skipped | Other |
| --- | ---: | ---: | ---: | --- |
| Offline Python regression | 251 | 0 | 0 | 4 broker tests deselected; 218 warnings; 5 subtests passed |
| Isolated Kafka and MQTT integration | 4 | 0 | 0 | 227 warnings |

The four broker tests are separate from the offline total. The warnings include dependency deprecations in kafka-python and Paho; the runner also reported Node.js action-runtime deprecation warnings. They are not test failures, but remain maintenance work.

## Limits of the evidence

The synthetic scaler fixtures are deterministic test artifacts, not production scaler binaries. Their use does not establish numerical parity with production artifacts or replace artifact-hash validation. The workflow does not exercise full MQTT-to-InfluxDB-to-prediction recovery, production data, production checkpoint volumes, all datasets or every failure mode. Additional real InfluxDB services, model/scaler artifacts, original datasets and IMS archive tools are needed for tests beyond this CI scope. Earlier isolated recovery tests are separate evidence, described in [pipeline E2E verification](PIPELINE_E2E_VERIFICATION.md) and [prediction window validation](PREDICTION_WINDOW_VALIDATION.md).

Passing CI does not authorize production deployment or establish exactly-once processing. Kafka, SQLite and InfluxDB operations are not a single atomic transaction; replay can produce duplicate outputs.
