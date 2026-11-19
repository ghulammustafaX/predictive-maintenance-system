# Module 02 remediation baseline

Read-only observations on 2026-11-19, before remediation. This is a point-in-time baseline, not proof of a later comparison.

- Git: clean `main` at `88898b6`, one commit ahead of `origin/main`.
- Production Compose: Kafka, ZooKeeper, InfluxDB and PostgreSQL running; consumer, preprocessing, prediction, backend and simulator stopped.
- Kafka topics: `__consumer_offsets`, `sensor-telemetry`, `sensor-telemetry-dead-letter`, `preprocessed-telemetry`, `rul-predictions`.
- Consumer groups: `influxdb-writer-group` and `preprocessing-service-group` on `sensor-telemetry` partition 0 at 20,671; `rul-prediction-service-group` on `preprocessed-telemetry` partition 0 at 20,671. All had zero lag and no active members.
- InfluxDB: organization `pms-org`; buckets `sensor-data`, `_monitoring`, `_tasks`. Measurements in `sensor-data`: `sensor_telemetry`, `rul_predictions`. Counts of field values across all fields: 1,032,550 and 88,710 respectively. `sensor_telemetry` has 41,302 values for the `time_cycles` field; this is a point count only where every point has that field. A representative `sensor_1` value was 518.67 for FD001 unit 1 at 2026-11-11T14:33:42.544708744Z.
- PostgreSQL exact row counts: `alert_audit` 24, `alerts` 14, `anomaly_records` 20, `audit_logs` 48, `email_verification_tokens` 6, `password_reset_tokens` 0, `revoked_tokens` 3, `users` 3. The public schema has these eight tables and 62 columns.
- Named production volumes observed: `predictive-maintenance-system_influxdb-data`, `predictive-maintenance-system_kafka-data`, `predictive-maintenance-system_postgres-data`, `predictive-maintenance-system_zookeeper-data`, `predictive-maintenance-system_zookeeper-log`.
- The Python launcher reports no installed Python. The existing `.venv` points to an unavailable Windows Store Python 3.13 installation. It was not modified.

## Comparison after isolated tests

On 2026-11-19, the production Kafka topics and all three listed group offsets remained unchanged at 20,671. InfluxDB bucket names and the two listed aggregate field counts matched; PostgreSQL exact row counts matched. The listed production service states and named volumes matched. These are sampled observations and counts, not a complete content comparison. The isolated test project used its own network and was stopped and removed without deleting volumes.

The same sampled production comparisons were repeated after the additional reliability tests and again matched the baseline. Test containers and networks were removed; test volumes were retained. The comparison does not establish equality of every database record.

After the final Module 01 and active-ingestion checks, production Kafka topic names and three committed offsets again matched (20,671 each), as did the recorded InfluxDB bucket names and aggregate field counts, PostgreSQL row counts, production container states, and named volumes. Both final test projects were removed without deleting volumes.
