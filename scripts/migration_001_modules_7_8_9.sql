-- ============================================================================
-- Migration 001: Module 7 (Anomaly Detection) + Module 8 (RBAC: floor_operator)
--                 + Module 9 (Alerting) table additions
--
-- Target:  PostgreSQL 16 (pms-postgres, pms_database)
-- Status:  PENDING APPROVAL — DO NOT APPLY WITHOUT EXPLICIT CONFIRMATION
--
-- This migration is IDEMPOTENT: it uses IF NOT EXISTS / IF NOT EXISTS clauses
-- so it can be re-run safely after a partial failure.
--
-- PREREQUISITES:
--   1. Verify production Compose project:  docker compose ps
--   2. Verify backup exists:              D:\PMS-backups\2026-11-11\postgres
--   3. Take a fresh pg_dump before applying:
--      docker exec pms-postgres pg_dump -U pms_user pms_database > pre_migration_backup.sql
--
-- ROLLBACK:
--   See migration_001_rollback.sql for the reverse script.
-- ============================================================================

-- ── 1. Add FLOOR_OPERATOR to userrole enum ──────────────────────────────────
-- PostgreSQL 16 supports ADD VALUE IF NOT EXISTS
ALTER TYPE userrole ADD VALUE IF NOT EXISTS 'floor_operator'
    BEFORE 'viewer';

-- ── 2. Create anomaly_records table ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS anomaly_records (
    event_id           VARCHAR       NOT NULL,
    dataset_id         VARCHAR       NOT NULL,
    unit_id            VARCHAR       NOT NULL,
    run_id             VARCHAR       NOT NULL,
    cycle              INTEGER       NOT NULL,
    timestamp          VARCHAR       NOT NULL,
    sensor             VARCHAR       NOT NULL,
    detection_method   VARCHAR       NOT NULL,
    observed_value     DOUBLE PRECISION NOT NULL,
    severity           VARCHAR       NOT NULL,
    score              DOUBLE PRECISION NOT NULL,
    status             VARCHAR       DEFAULT 'open' NOT NULL,
    created_at         TIMESTAMP WITHOUT TIME ZONE NOT NULL,

    CONSTRAINT anomaly_records_pkey PRIMARY KEY (event_id)
);

CREATE INDEX IF NOT EXISTS ix_anomaly_records_dataset_id ON anomaly_records (dataset_id);
CREATE INDEX IF NOT EXISTS ix_anomaly_records_unit_id    ON anomaly_records (unit_id);
CREATE INDEX IF NOT EXISTS ix_anomaly_records_run_id     ON anomaly_records (run_id);
CREATE INDEX IF NOT EXISTS ix_anomaly_records_severity   ON anomaly_records (severity);

-- ── 3. Create alerts table ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS alerts (
    id                 VARCHAR       NOT NULL,
    dedup_key          VARCHAR       NOT NULL,
    alert_type         VARCHAR       NOT NULL,
    severity           VARCHAR       NOT NULL,
    dataset_id         VARCHAR       NOT NULL,
    unit_id            VARCHAR       NOT NULL,
    run_id             VARCHAR       NOT NULL,
    cycle              INTEGER,
    message            VARCHAR       NOT NULL,
    status             VARCHAR       DEFAULT 'open' NOT NULL,
    source_event_id    VARCHAR,
    created_at         TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    acknowledged_at    TIMESTAMP WITHOUT TIME ZONE,
    acknowledged_by    VARCHAR,
    resolved_at        TIMESTAMP WITHOUT TIME ZONE,

    CONSTRAINT alerts_pkey PRIMARY KEY (id),
    CONSTRAINT alerts_acknowledged_by_fkey
        FOREIGN KEY (acknowledged_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS ix_alerts_dedup_key  ON alerts (dedup_key);
CREATE INDEX IF NOT EXISTS ix_alerts_dataset_id ON alerts (dataset_id);
CREATE INDEX IF NOT EXISTS ix_alerts_unit_id    ON alerts (unit_id);
CREATE INDEX IF NOT EXISTS ix_alerts_status     ON alerts (status);
CREATE INDEX IF NOT EXISTS ix_alerts_severity   ON alerts (severity);
CREATE INDEX IF NOT EXISTS ix_alerts_created_at ON alerts (created_at);

-- ── 4. Create alert_audit table ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS alert_audit (
    id         VARCHAR       NOT NULL,
    alert_id   VARCHAR       NOT NULL,
    user_id    VARCHAR,
    action     VARCHAR       NOT NULL,
    detail     VARCHAR,
    timestamp  TIMESTAMP WITHOUT TIME ZONE NOT NULL,

    CONSTRAINT alert_audit_pkey PRIMARY KEY (id),
    CONSTRAINT alert_audit_alert_id_fkey
        FOREIGN KEY (alert_id) REFERENCES alerts(id),
    CONSTRAINT alert_audit_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS ix_alert_audit_alert_id ON alert_audit (alert_id);

-- ── 5. Verification queries (run after migration) ────────────────────────────
-- SELECT t.typname, e.enumlabel FROM pg_type t JOIN pg_enum e ON t.oid = e.enumtypid
--   WHERE t.typname = 'userrole' ORDER BY e.enumsortorder;
-- Expected: ADMIN, MAINTENANCE_ENGINEER, FLOOR_OPERATOR, VIEWER
--
-- SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;
-- Expected: alert_audit, alerts, anomaly_records, audit_logs,
--           email_verification_tokens, password_reset_tokens,
--           revoked_tokens, users
