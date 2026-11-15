-- ============================================================================
-- Rollback for Migration 001: Module 7 + 8 + 9 table additions
--
-- WARNING: This drops the anomaly_records, alerts, and alert_audit tables.
-- All anomaly and alert data will be lost. The userrole enum value
-- 'floor_operator' CANNOT be removed in PostgreSQL without rebuilding the
-- enum type (which requires a table lock and is not safe on a live system).
-- The unused enum value is harmless if no rows reference it.
--
-- PREREQUISITES:
--   1. Confirm no critical data exists in the new tables:
--      SELECT COUNT(*) FROM alerts;
--      SELECT COUNT(*) FROM anomaly_records;
--   2. Have the pre-migration backup ready for restore if needed.
-- ============================================================================

-- Drop tables in reverse dependency order
DROP TABLE IF EXISTS alert_audit CASCADE;
DROP TABLE IF EXISTS alerts CASCADE;
DROP TABLE IF EXISTS anomaly_records CASCADE;

-- NOTE: The 'floor_operator' enum value remains in the userrole type.
-- Removing it requires recreating the enum type, which is not safe on
-- a live PostgreSQL instance. The value is inert if unused.
-- To remove it in a maintenance window:
--   1. ALTER TYPE userrole RENAME TO userrole_old;
--   2. CREATE TYPE userrole AS ENUM ('admin', 'maintenance_engineer', 'viewer');
--   3. ALTER TABLE users ALTER COLUMN role TYPE userrole USING role::text::userrole;
--   4. DROP TYPE userrole_old;

-- Verify
-- SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;
-- Expected: audit_logs, email_verification_tokens, password_reset_tokens,
--           revoked_tokens, users (no anomaly_records, alerts, alert_audit)
