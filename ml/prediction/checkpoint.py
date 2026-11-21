"""Single-owner durable checkpoint for the single-partition prediction worker."""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

try:
    import fcntl
except ImportError:  # Windows local development
    fcntl = None
    import msvcrt


class CheckpointError(RuntimeError):
    pass


def _persistent_mount(directory: Path) -> bool:
    """Require a dedicated non-memory mount; this cannot prove host durability."""
    directory = directory.resolve()
    if not os.path.ismount(directory):
        return False
    if os.name != "posix":
        return False
    try:
        for line in Path("/proc/self/mountinfo").read_text().splitlines():
            before, after = line.split(" - ", 1)
            mountpoint = before.split()[4].replace("\\040", " ").replace("\\134", "\\")
            if os.path.realpath(mountpoint) == str(directory):
                return after.split()[0] not in {"tmpfs", "ramfs", "overlay", "rootfs"}
    except (OSError, ValueError, IndexError):
        return False
    return False


def validate_checkpoint_path(path: str | None) -> None:
    if not path:
        raise CheckpointError("PREDICTION_CHECKPOINT_PATH is required")
    target = Path(path)
    if not target.is_absolute() or not target.parent.is_dir() or not _persistent_mount(target.parent):
        raise CheckpointError("checkpoint directory must be an existing dedicated non-memory mount")


class PredictionCheckpoint:
    """One local persistent volume, one owner. Not a distributed lock."""

    def __init__(self, path: str, *, require_mount: bool = False):
        target = Path(path)
        if not target.is_absolute() or not target.parent.is_dir():
            raise CheckpointError("checkpoint must be an absolute path in an existing persistent directory")
        if require_mount and not _persistent_mount(target.parent):
            raise CheckpointError("checkpoint directory must be a dedicated non-memory mount")
        self._lock = open(str(target) + ".lock", "a+b")
        try:
            if fcntl is not None:
                fcntl.flock(self._lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            else:
                self._lock.seek(0, 2)
                if self._lock.tell() == 0:
                    self._lock.write(b"\0")
                    self._lock.flush()
                self._lock.seek(0)
                msvcrt.locking(self._lock.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            self._lock.close()
            raise CheckpointError("prediction checkpoint is already owned") from exc
        try:
            self.db = sqlite3.connect(str(target))
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.execute("CREATE TABLE IF NOT EXISTS checkpoint (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL)")
            self.db.commit()
        except Exception:
            self._lock.close()
            raise

    def load(self):
        row = self.db.execute("SELECT payload FROM checkpoint WHERE id=1").fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(row[0])
        except (ValueError, TypeError) as exc:
            raise CheckpointError("corrupt checkpoint JSON") from exc
        if not isinstance(payload, dict) or payload.get("schema_version") != 1:
            raise CheckpointError("unsupported checkpoint schema")
        return payload

    def save(self, payload):
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        with self.db:
            self.db.execute("INSERT INTO checkpoint(id,payload) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", (encoded,))

    def close(self):
        try:
            self.db.close()
        finally:
            if fcntl is not None:
                fcntl.flock(self._lock.fileno(), fcntl.LOCK_UN)
            else:
                self._lock.seek(0)
                msvcrt.locking(self._lock.fileno(), msvcrt.LK_UNLCK, 1)
            self._lock.close()
