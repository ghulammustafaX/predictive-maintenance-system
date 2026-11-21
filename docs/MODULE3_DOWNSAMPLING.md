# Module 03.6: Automated downsampling task

This change adds **opt-in scheduled InfluxDB 2.x Flux tasks**. No production tasks are activated by merely merging this change. The existing production Compose stack and raw `sensor_telemetry` ingestion remain unchanged.

## Aggregation contract

- Source: operator-selected existing raw bucket, measurement `sensor_telemetry`.
- Target: a **separate existing bucket**, measurement `sensor_telemetry_downsampled`.
- Interval: explicit selection of 60, 300, 900, or 3600 seconds. There is **no implicit production interval**.
- Values: arithmetic mean of `sensor_1` to `sensor_21` and `operational_setting_1` to `operational_setting_3`. `time_cycles` is intentionally not averaged.
- Grouping: `dataset_id`, `unit_id`, `run_id`, `_field`; source `cycle` tag is removed before aggregation to avoid per-cycle series preventing downsampling.
- Scheduling: task `every` equals aggregation interval with a 10-second offset. Task processing is windowed over `-task.every` relative to task time; aggregate timestamps are Flux window boundaries. Data late beyond that window is not automatically backfilled.
- Reprocessing an identical aggregate series/window rewrites the same point identity. It is **not** exactly-once execution.
- The original raw points are neither deleted nor re-tagged. Creation of a new aggregate bucket is a separate, explicitly approved operation using `bucket_policy.py`.

## Safety and provisioning

`storage/influx_init/downsample_task.py` defaults to read-only verification. It verifies the input and output bucket exist in the specified organization and checks the exact existing task Flux. Any mismatch or inactive existing task causes refusal. No tasks or buckets are ever updated or deleted.

After explicit approval, an operator may create a **new** task using both `--create-if-missing` and exact `PMS_TASK_CREATE_CONFIRM` matching `--name`. Example for an **authorized isolated system**, after installing `storage/influx_init/requirements.txt` and configuring `INFLUXDB_URL`, `INFLUXDB_TOKEN` and `INFLUXDB_ORG`:

```sh
# Read-only inspection of a previously configured task:
python storage/influx_init/downsample_task.py \\
  --name m3-test-downsample \\
  --source-bucket m3-isolated-bucket \\
  --aggregate-bucket m3-isolated-aggregate \\
  --interval-seconds 60

# Create a missing task after a separate explicit operational approval:
PMS_TASK_CREATE_CONFIRM=m3-test-downsample \\
python storage/influx_init/downsample_task.py \\
  --name m3-test-downsample \\
  --source-bucket m3-isolated-bucket \\
  --aggregate-bucket m3-isolated-aggregate \\
  --interval-seconds 60 --create-if-missing
```

Do **not** run these examples against existing production resources. Before production activation, agree on source and target retention, storage growth, lateness tolerance, backfill policy, disaster recovery, task observability and safe disable/rollback. InfluxDB retention based on source timestamp may remove historical records; this task never enables expiry. The initial task does not automatically migrate or reconstruct historical data.

## CI verification

`tests/module3_downsample_probe.py` uses only the isolated Module 03 InfluxDB. It creates a uniquely named test task and aggregate bucket, confirms create-only/idempotent/no-overwrite policy, invokes the real task engine to aggregate two raw cycles, verifies mean and tag correctness, and verifies both original raw points remain. The existing Kafka, MQTT, persistence, Flux-query and retention tests remain unchanged.
