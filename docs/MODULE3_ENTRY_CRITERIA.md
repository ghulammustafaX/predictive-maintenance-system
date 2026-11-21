# Module 03 entry criteria

Module 03 development should begin from the proposed reliability baseline `c9e99ab2f3cda9c8c6ea257499293e300b9b326f` and its [freeze proposal](RELIABILITY_BASELINE_FREEZE.md). The verified reference is the [post-merge CI run](https://github.com/ghulammustafaX/predictive-maintenance-system/actions/runs/44868579033); passing that run is not production deployment approval. The proposed tag has not been created.

## Before development

1. Create a separate development branch from the verified baseline or a separately reviewed successor; preserve the baseline SHA and do not rewrite the reference history.
2. Require successful **Offline Python regression** and **Isolated Kafka and MQTT integration** checks on the exact proposed change. Investigate failures and unexplained skips; do not suppress tests to obtain a green result.
3. Preserve the [frozen Module 02 ingestion contract](MODULE2_BASELINE.md). Any intentional contract change requires an explicit compatibility review and real failure/recovery evidence.
4. Use isolated source identities, topics, consumer groups, networks, databases and volumes for tests. Do not point test producers or consumers at production or reuse production checkpoint volumes.
5. Preserve model/scaler artifacts and checkpoint identity checks. Synthetic fixtures are test-only; validate production-compatible artifacts separately where numerical behavior matters.

## Acceptance gates

- Run the relevant Module 01/02 and stateful-pipeline regressions and the complete applicable suite; report passes, failures, skips and warnings without double counting.
- For changes affecting streaming or persistence, use real isolated Kafka and InfluxDB to verify end-to-end behavior, offsets, state restoration, output identity, failure recovery and reconciliation. Exercise MQTT as well when the bridge or MQTT path is affected. Do not substitute mocks for the real-service behavior under review.
- Demonstrate that partition ownership, per-engine/run state, artifact versions and prediction-window continuity remain correct within the supported single-host topology. Account for at-least-once duplicates and downstream side effects.
- Document changed behavior, configuration, migration and rollback requirements, limitations and evidence. Address regressions before acceptance.
- Obtain separate authorization for any production deployment, source admission, offset selection or database action. A passing CI run alone is insufficient.

## Rollback and reference

Keep the baseline SHA, previously validated image and artifact identities, checkpoint backups and per-partition offset records available for a separately reviewed rollback. Image rollback cannot undo writes, recover missing Kafka history or reverse downstream side effects. Never overwrite production data or reset offsets automatically; reconcile state and obtain explicit operational approval first.
