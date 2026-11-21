#!/usr/bin/env sh
# Module 03 non-destructive InfluxDB bucket inspection/provisioning.
# READ-ONLY by default. To create a missing bucket, supply --create-if-missing
# and PMS_BUCKET_CREATE_CONFIRM matching the bucket name exactly.
# This wrapper does not run automatically in the production Compose stack.
set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec python "$SCRIPT_DIR/bucket_policy.py" "$@"
