# Reliability baseline freeze proposal

This document proposes a freeze; it does not create or publish a tag. Verification date: 21 November 2026. Repository: [ghulammustafaX/predictive-maintenance-system](https://github.com/ghulammustafaX/predictive-maintenance-system), branch `main`. The exact proposed baseline is **`c9e99ab2f3cda9c8c6ea257499293e300b9b326f`**, the merge of PR #14. The post-merge [PMS Python CI run](https://github.com/ghulammustafaX/predictive-maintenance-system/actions/runs/44868579033) passed both jobs; see [CI verification](CI_VERIFICATION_REPORT.md).

Relevant accepted commits include Module 02 ingestion `af65ac69e273e84eabe0dc1de964dd17b7b4ea71`, its documentation freeze `59d37fbcf3175e248bd6ea1ac8d9b4af582dc11c`, pipeline reliability `9d7a74cb75e60a0b6dfa897a2fd4aacbf40054a1`, reliability evidence `d0da279146564291ac4eb6d19dad05b2da4c4056`, IMS crest-factor correction `47969127b032ce5959babc3739cdb9b59c81083d`, and CI fix `d9e54f21e473ab8b4ef5ba317e57c60f586dae4c`. The ingestion directory at the proposed baseline has no diff from `af65ac69`.

## Frozen scope

- Module 01 simulator, event identities and IMS crest-factor correction; IMS-to-InfluxDB ingestion is not implemented.
- The accepted [Module 02 ingestion baseline](MODULE2_BASELINE.md): QoS 1 MQTT bridge with persistent session and manual acknowledgments after confirmed Kafka delivery or confirmed dead-letter handling, plus Kafka ingestion that commits after synchronous InfluxDB write or confirmed dead-letter publication. Guarantees depend on the documented operational conditions.
- Partition-isolated preprocessing with confirmed feature publication, per-partition SQLite state, checkpoint/offset reconciliation and fail-closed handling for inconsistent recovery state.
- Prediction window checkpoint recovery for a single instance and single input partition, including artifact identity and contiguous-cycle validation.
- Compose's separate named preprocessing and prediction checkpoint volumes, deterministic paths and single-replica settings. These are configuration, not production deployment evidence.
- Artifact compatibility validation and required model, scaler and training-statistics identities. Synthetic CI fixtures do not establish production numerical parity.

## Supported recovery architecture

The supported operational topology is one host with durable local checkpoint storage. Preprocessing group members must share the same local SQLite checkpoint file, and source partition affinity must keep a dataset/run/engine together. Partition locks and ownership tokens protect same-host checkpoint handoff; recovery compares checkpoint offsets and input identity with Kafka. Prediction supports one instance, one input partition and a persistent checkpoint volume. Model and preprocessing artifact identities must match the saved state. Startup checks reject unsupported checkpoint locations but cannot prove physical durability or cross-host exclusivity. Multi-host deployment and separate preprocessing state files for owners of the same group are unsupported.

Outputs use deterministic source-derived feature keys and prediction IDs, allowing duplicate identification. They do not provide a cross-service exactly-once transaction. Confirmed publication, checkpoint persistence and Kafka commits have crash boundaries; output replay and downstream side effects require reconciliation. Existing nonzero consumer offsets require a deliberately reviewed state bootstrap; missing or inconsistent checkpoints must not be silently reset. See [offset safety](PIPELINE_OFFSET_SAFETY.md) and [production readiness](PRODUCTION_FORWARD_RECOVERY_READINESS.md).

## Restrictions and tag proposal

This freeze does not authorize automatic production migration, automatic consumer-group offset reset, deletion or recreation of production volumes, or modification of historical production data. It does not claim missing Kafka history was recovered, and passing CI does not authorize production deployment.

Proposed annotated tag: `pms-reliability-baseline-v1.0`, targeting **exactly** `c9e99ab2f3cda9c8c6ea257499293e300b9b326f`. Create or push it only under separate authorization, using the explicit SHA rather than the then-current `HEAD`. The three documentation files prepared with this proposal are not themselves included in that target commit. Before creating the tag, recheck the target object and any existing local or remote tag of the same name.

## Proposed main-branch protection

On 21 November 2026 the GitHub branch endpoint reported `main` as unprotected with required status-check enforcement off, and the repository ruleset listing returned an empty list. The detailed branch-protection endpoint returned HTTP 403 for the available integration; inherited or otherwise inaccessible rules and administrative settings were not independently verified. No setting was changed.

For separate administrator review, require pull requests before merge; require successful **Offline Python regression** and **Isolated Kafka and MQTT integration** checks for the candidate commit; block merges while required checks fail; and prohibit force pushes and branch deletion. Confirm the exact GitHub check names and bypass policy before enabling any rule. CI passing does not authorize production deployment.
