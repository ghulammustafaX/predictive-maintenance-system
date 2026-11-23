# Module 04 hosted Kafka verification plan (not yet executed)

Use a new GitHub-hosted `ubuntu-24.04` job, following the preflight and unique
project pattern in `python-ci.yml`'s Module 03 job. Do not invoke the local
Compose isolation guard as a substitute for inspecting the hosted Compose
configuration. No existing Module 03 Compose project or resource is reused.

- The dedicated workflow, Compose file and probe are present for review. The
  workflow runs on pull requests targeting main or by manual dispatch after it
  is available to GitHub. It has not yet been executed.
- Generate a project name `pms-m4-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}`;
  require GitHub-hosted runner, adequate available memory and disk, Docker,
  and a bounded job timeout. Inspect the fully rendered Compose configuration
  before `up`: no external resources, explicit container names, host ports,
  production volumes/networks, or production topics or groups.
- Start a fresh broker and worker in an isolated project network with new
  project-scoped volumes. Use test-only input/output topic names and a unique
  consumer group. Do not reset offsets. Mount a dedicated persistent local
  checkpoint volume; never bind production data. Use synthetic test-only
  scaler fixtures and a test-only startup path with appropriate validation;
  do not package or replace deployed artifacts.
- A bounded probe produces a valid cycle, then an infinity reading whose
  prior finite value gives a deterministic expected feature vector, then a
  malformed numeric string on the same partition. Verify the first two output
  records and their finite values. Poll by deadline to prove no success output
  for the malformed offset. Independently query broker committed offsets and
  inspect the checkpoint: both must remain at the successfully processed
  prefix while repeated failure/retry log markers are observed. Verify the
  same stable checkpoint and commit after another bounded retry interval.
  Failure to observe the expected retry must fail the job, not skip it.
- Capture logs and project-scoped state on failure, without secrets. In an
  `always()` cleanup step, run only `docker compose -p "$M4_PROJECT" -f
  docker-compose.module4-verification.yml down --volumes --remove-orphans`
  after verifying the unique project name. Never use global prune or operate
  on other projects. Set explicit probe and job timeouts.

A permanently invalid record is expected to keep its partition paused and
retrying. This test must not claim DLQ behavior or exactly-once processing.
It is a proposed acceptance check only; no real-broker evidence exists yet.
