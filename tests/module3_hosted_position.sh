#!/usr/bin/env bash
# Run only on an ephemeral GitHub-hosted runner and a project unique to this run.
set -euo pipefail
test "${GITHUB_ACTIONS:-}" = true
test "${RUNNER_ENVIRONMENT:-}" = github-hosted
[[ "$M3_PROJECT" =~ ^pms-m3-[0-9]+-[0-9]+$ ]]
compose=(docker compose -p "$M3_PROJECT" -f docker-compose.module3-verification.yml)
consumer_id=$("${compose[@]}" ps -q consumer)
test -n "$consumer_id"
image=$(docker inspect "$consumer_id" --format '{{.Config.Image}}')
network="${M3_PROJECT}_default"
for mode in same transfer; do
  case_name="${mode}${GITHUB_RUN_ID}${GITHUB_RUN_ATTEMPT}"
  topic="m3-position-accept-${case_name}-20261122"
  group="m3-position-accept-${case_name}-group-20261122"
  volume="${M3_PROJECT}-${mode}-coordination"
  existing=$("${compose[@]}" exec -T kafka kafka-topics --bootstrap-server kafka:9092 --describe --if-exists --topic "$topic")
  test -z "$existing"
  "${compose[@]}" exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --topic "$topic" --partitions 2 --replication-factor 1
  docker volume create --label "pms.hosted.acceptance=$M3_PROJECT" "$volume" >/dev/null
  common=(--network "$network" --label "pms.hosted.acceptance=$M3_PROJECT"
    -e M3_POSITION_ACCEPT_GUARD=m3-position-acceptance-only -e "M3_POSITION_CASE=$case_name"
    -e M3_POSITION_CAPTURE=isolated-position-evidence-only -e M3_EVIDENCE_DIR=/evidence
    -e "M3_EVIDENCE_UID=$(id -u)" -e "M3_EVIDENCE_GID=$(id -g)"
    -e "M3_PROJECT=$M3_PROJECT" -e "GITHUB_SHA=$GITHUB_SHA"
    -e MODULE3_REBALANCE_FAULT_GUARD=m3-isolated-rebalance-fault-only
    -e MODULE3_REBALANCE_PROBE_GUARD=m3-isolated-rebalance-probe-only
    -e PMS_BATCH_WORKER_CONFIRM=opt-in-isolated-batch-worker -e PMS_INFLUX_BATCH_SIZE=3
    -e KAFKA_MAX_POLL_RECORDS=12 -e KAFKA_BROKER=kafka:9092 -e "SENSOR_TOPIC=$topic"
    -e DEAD_LETTER_TOPIC=m3-isolated-dead-letter -e "CONSUMER_GROUP=$group"
    -e INFLUXDB_URL=http://influxdb:8086 -e INFLUXDB_TOKEN=m3-isolated-token
    -e INFLUXDB_ORG=m3-isolated-org -e INFLUXDB_BUCKET=m3-isolated-bucket
    -v "$PWD/tests:/tests:ro" -v "$PWD/m3-evidence:/evidence" -v "$volume:/coordination" --entrypoint python
    "$image" /tests/module3_position_acceptance_runner.py)
  docker run -d --name "${M3_PROJECT}-${mode}-fault" "${common[@]}" fault
  for phase in publish-p0 await-write-ack; do docker run --rm "${common[@]}" probe "$phase"; done
  docker run -d --name "${M3_PROJECT}-${mode}-peer" "${common[@]}" peer
  for phase in trigger-rebalance verify-recovery inspect-identities; do docker run --rm "${common[@]}" probe "$phase"; done
  "${compose[@]}" exec -T kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group "$group"
  for role in fault peer; do
    name="${M3_PROJECT}-${mode}-${role}"
    timeout 80 docker stop --time 35 "$name" >/dev/null
  done
  fault_exit=$(docker inspect "${M3_PROJECT}-${mode}-fault" --format '{{.State.ExitCode}}')
  peer_exit=$(docker inspect "${M3_PROJECT}-${mode}-peer" --format '{{.State.ExitCode}}')
  if [[ "$mode" == same ]]; then evidence_file=m3-evidence/reassignment_same_owner.json; else evidence_file=m3-evidence/reassignment_different_owner.json; fi
  python - "$evidence_file" "$fault_exit" "$peer_exit" <<'PY'
import sys
import os
from pathlib import Path
sys.path.insert(0, 'tests')
from module3_runner_evidence import atomic_update
from module3_reassignment_evidence import finalize
path = Path(sys.argv[1])
record = atomic_update(path, lambda value: value.update(worker_exit_statuses={'fault': int(sys.argv[2]), 'peer': int(sys.argv[3])}))
validated = finalize(record, os.environ.get('M3_PROJECT'), os.environ.get('GITHUB_SHA'))
atomic_update(path, lambda value: (value.clear(), value.update(validated)))
PY
done
