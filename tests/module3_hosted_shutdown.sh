#!/usr/bin/env bash
set -euo pipefail
test "${GITHUB_ACTIONS:-}" = true
test "${RUNNER_ENVIRONMENT:-}" = github-hosted
[[ "$M3_PROJECT" =~ ^pms-m3-[0-9]+-[0-9]+$ ]]
compose=(docker compose -p "$M3_PROJECT" -f docker-compose.module3-verification.yml)
consumer_id=$("${compose[@]}" ps -q consumer)
image=$(docker inspect "$consumer_id" --format '{{.Config.Image}}')
for scenario in graceful exhausted; do
  topic="${M3_PROJECT}-${scenario}"
  group="${topic}-group"
  volume="${topic}-coordination"
  test -z "$("${compose[@]}" exec -T kafka kafka-topics --bootstrap-server kafka:9092 --describe --if-exists --topic "$topic")"
  "${compose[@]}" exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --topic "$topic" --partitions 1 --replication-factor 1
  docker volume create --label "pms.hosted.acceptance=$M3_PROJECT" "$volume" >/dev/null
  common=(--network "${M3_PROJECT}_default" --label "pms.hosted.acceptance=$M3_PROJECT"
    -e M3_HOSTED_GUARD=github-hosted-shutdown-only -e "M3_PROJECT=$M3_PROJECT" -e "GITHUB_SHA=$GITHUB_SHA"
    -e "M3_HOSTED_SCENARIO=$scenario" -e KAFKA_BROKER=kafka:9092
    -e "SENSOR_TOPIC=$topic" -e "CONSUMER_GROUP=$group" -e DEAD_LETTER_TOPIC=m3-isolated-dead-letter
    -e INFLUXDB_URL=http://influxdb:8086 -e INFLUXDB_TOKEN=m3-isolated-token
    -e INFLUXDB_ORG=m3-isolated-org -e INFLUXDB_BUCKET=m3-isolated-bucket
    -e PMS_BATCH_WORKER_CONFIRM=opt-in-isolated-batch-worker -e PMS_INFLUX_BATCH_SIZE=3
    -e "PMS_BATCH_SHUTDOWN_DRAIN_SECONDS=$([[ "$scenario" == exhausted ]] && echo 0.5 || echo 30)"
    -v "$PWD/tests:/tests:ro" -v "$volume:/coordination" --entrypoint python
    "$image" /tests/module3_hosted_shutdown_probe.py)
  worker="${topic}-worker"
  docker run -d --name "$worker" "${common[@]}" worker >/dev/null
  assigned=false
  for _ in $(seq 1 90); do
    if "${compose[@]}" exec -T kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group "$group" --members --verbose 2>/dev/null | grep -Fq "$topic"; then assigned=true; break; fi
    test "$(docker inspect "$worker" --format '{{.State.Running}}')" = true
    sleep 1
  done
  "$assigned" || { docker logs "$worker"; exit 1; }
  docker run --rm "${common[@]}" publish | tee "m3-evidence/${scenario}-publish.jsonl"
  ready=false
  for _ in $(seq 1 90); do
    if docker logs "$worker" 2>&1 | grep -q M3_HOSTED_QUEUED_READY; then ready=true; break; fi
    test "$(docker inspect "$worker" --format '{{.State.Running}}')" = true
    sleep 1
  done
  "$ready" || { docker logs "$worker"; exit 1; }
  docker kill --signal SIGTERM "$worker" >/dev/null
  # This delay is fault injection, not a readiness check: it makes the
  # 0.5-second cooperative deadline expire while queued work is held.
  if [[ "$scenario" == exhausted ]]; then sleep 2; fi
  docker exec "$worker" touch /coordination/release
  exit_code=$(timeout 90 docker wait "$worker")
  docker logs "$worker" > "m3-evidence/${scenario}-worker.log" 2>&1
  python -c 'import json,sys; lines=open(sys.argv[1], encoding="utf-8").read().splitlines(); entries=[json.loads(x.split("M3_SHUTDOWN_STATUS ",1)[1]) for x in lines if "M3_SHUTDOWN_STATUS " in x]; assert len(entries)==1; cleanup=entries[0]["cleanup"]; assert all(name in cleanup for name in ("producer flush","producer close","consumer close","influx close","group_departure")); json.dump(entries[0],open(sys.argv[2],"w",encoding="utf-8"),indent=2)' "m3-evidence/${scenario}-worker.log" "m3-evidence/${scenario}-cleanup.json"
  docker run --rm "${common[@]}" snapshot > "m3-evidence/${scenario}-before.json"
  if [[ "$scenario" == graceful ]]; then cp "m3-evidence/${scenario}-before.json" m3-evidence/graceful_shutdown.json; fi
  if [[ "$scenario" == exhausted ]]; then
    test "$exit_code" -ne 0
    grep -Eq 'M3_SHUTDOWN_STATUS .*"drained": false.*"status": "incomplete"' "m3-evidence/${scenario}-worker.log"
    python -c 'import json,sys; x=json.load(open(sys.argv[1])); assert x["beginning_offset"]==0 and x["end_offset"]==3 and x["committed_offset"] in (None,0) and x["observed_cycle_identities"]==[]' "m3-evidence/${scenario}-before.json"
    docker run -d --name "${topic}-recovery" "${common[@]}" recovery >/dev/null
    recovered=false
    for _ in $(seq 1 90); do
      docker run --rm "${common[@]}" snapshot > "m3-evidence/${scenario}-after.json"
      cp "m3-evidence/${scenario}-after.json" m3-evidence/exhausted_drain_replay.json
      if python -c 'import json,sys; x=json.load(open(sys.argv[1])); assert x["committed_offset"]==x["end_offset"]==3 and x["observed_cycle_identities"]==[1,2,3] and not x["missing_point_identities"] and not x["unexpected_point_identities"] and not x["duplicate_observed_identities"]' "m3-evidence/${scenario}-after.json" 2>/dev/null; then recovered=true; break; fi
      sleep 1
    done
    "$recovered" || { docker logs "${topic}-recovery"; exit 1; }
    docker kill --signal SIGTERM "${topic}-recovery" >/dev/null
    recovery_exit=$(timeout 90 docker wait "${topic}-recovery")
    test "$recovery_exit" = 0
    docker logs "${topic}-recovery" > "m3-evidence/${scenario}-recovery.log" 2>&1
    python tests/module3_shutdown_evidence.py exhausted m3-evidence/exhausted_drain_replay.json \
      "m3-evidence/${scenario}-cleanup.json" "$exit_code" "$recovery_exit" "m3-evidence/${scenario}-before.json"
  else
    test "$exit_code" = 0
    grep -Eq 'M3_SHUTDOWN_STATUS .*"drained": true.*"status": "close_completed_group_unverified"' "m3-evidence/${scenario}-worker.log"
    python -c 'import json,sys; x=json.load(open(sys.argv[1])); assert x["beginning_offset"]==0 and x["committed_offset"]==x["end_offset"]==3 and x["observed_cycle_identities"]==[1,2,3] and not x["missing_point_identities"] and not x["unexpected_point_identities"] and not x["duplicate_observed_identities"]' "m3-evidence/${scenario}-before.json"
    python tests/module3_shutdown_evidence.py graceful m3-evidence/graceful_shutdown.json \
      "m3-evidence/${scenario}-cleanup.json" "$exit_code"
  fi
  printf '{"scenario":"%s","worker_exit_status":%s,"result":"pass"}\n' "$scenario" "$exit_code" > "m3-evidence/${scenario}-result.json"
done
