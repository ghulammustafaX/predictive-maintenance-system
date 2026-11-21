"""Partition-fenced local SQLite checkpoints for preprocessing.

All members of a group must share one SQLite file on a single host. SQLite on
network filesystems is unsupported. A new owner increments a per-partition
epoch, fencing writes from an older owner after the ownership handoff.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import uuid
from pathlib import Path

try:
    import fcntl
except ImportError:
    fcntl = None
    import msvcrt

class CheckpointError(RuntimeError):
    pass


class CheckpointBusy(CheckpointError):
    pass


def _local_persistent_mount(directory: Path) -> bool:
    """Require a dedicated, non-memory local mount; host durability is operational."""
    if os.name != 'posix' or not os.path.ismount(directory):
        return False
    try:
        for line in Path('/proc/self/mountinfo').read_text().splitlines():
            before, after = line.split(' - ', 1)
            mountpoint = before.split()[4].replace('\\040', ' ').replace('\\134', '\\')
            if os.path.realpath(mountpoint) == str(directory.resolve()):
                fs = after.split()[0].lower()
                return fs not in {'tmpfs', 'ramfs', 'overlay', 'rootfs', 'nfs', 'nfs4', 'cifs', 'smb3', '9p'} and not fs.startswith('fuse.')
    except (OSError, ValueError, IndexError):
        return False
    return False


def validate_checkpoint_path(path: str | None) -> None:
    if not path:
        raise CheckpointError('PREPROCESSING_CHECKPOINT_PATH is required')
    target = Path(path)
    if (not target.is_absolute() or not target.parent.is_dir() or target.is_symlink()
            or not _local_persistent_mount(target.parent.resolve())
            or not os.access(target.parent, os.W_OK)):
        raise CheckpointError('checkpoint requires a writable dedicated non-memory local mount')


def artifact_identity(directory: Path, version: str) -> str:
    digest = hashlib.sha256(version.encode())
    for path in sorted(directory.glob('scaler_FD00*')):
        if path.suffix in {'.pkl', '.json'}:
            digest.update(path.name.encode())
            digest.update(path.read_bytes())
    if len(list(directory.glob('scaler_FD00*.pkl'))) != 4:
        raise CheckpointError('four preprocessing scaler artifacts are required')
    if len(list(directory.glob('scaler_FD00*_stats.json'))) != 4:
        raise CheckpointError('four preprocessing statistics artifacts are required')
    return digest.hexdigest()


class PreprocessingCheckpoint:
    VERSION = 1

    def __init__(self, path: str, group: str, artifact: str):
        target = Path(path)
        if not target.is_absolute() or not target.parent.is_dir():
            raise CheckpointError('checkpoint must use an absolute path in an existing persistent directory')
        self.group, self.artifact = group, artifact
        self.path = target
        self.locks = {}
        self.owners = {}
        self.db = sqlite3.connect(str(target), timeout=10)
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA journal_mode=DELETE')
        self.db.execute('CREATE TABLE IF NOT EXISTS checkpoints (grp TEXT NOT NULL, topic TEXT NOT NULL, part INTEGER NOT NULL, owner TEXT NOT NULL, payload TEXT, checksum TEXT, PRIMARY KEY(grp,topic,part))')
        self.db.commit()

    def acquire(self, topic: str, partition: int) -> str:
        identity = (topic, partition)
        if identity in self.locks:
            raise CheckpointError('partition checkpoint already acquired')
        lock_id = hashlib.sha256(json.dumps([self.group, topic, partition]).encode()).hexdigest()
        lock = open(str(self.path) + '.' + lock_id + '.lock', 'a+b')
        try:
            if fcntl is not None:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            else:
                lock.seek(0, 2)
                if lock.tell() == 0:
                    lock.write(b'0')
                    lock.flush()
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            lock.close()
            raise CheckpointBusy('partition checkpoint is already owned') from exc
        owner = uuid.uuid4().hex
        try:
            with self.db:
                self.db.execute('INSERT INTO checkpoints(grp,topic,part,owner) VALUES(?,?,?,?) ON CONFLICT(grp,topic,part) DO UPDATE SET owner=excluded.owner', (self.group, topic, partition, owner))
        except Exception:
            lock.close()
            raise
        self.locks[identity] = lock
        self.owners[identity] = owner
        return owner

    def release(self, topic: str, partition: int):
        self.owners.pop((topic, partition), None)
        lock = self.locks.pop((topic, partition), None)
        if lock is not None:
            if fcntl is not None:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            else:
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
            lock.close()

    def load(self, topic: str, partition: int, owner: str):
        if self.owners.get((topic, partition)) != owner:
            raise CheckpointError('checkpoint ownership lost')
        row = self.db.execute('SELECT owner,payload,checksum FROM checkpoints WHERE grp=? AND topic=? AND part=?', (self.group, topic, partition)).fetchone()
        if row is None or row[0] != owner:
            raise CheckpointError('checkpoint ownership lost')
        if row[1] is None:
            if row[2] is not None:
                raise CheckpointError('corrupt empty checkpoint')
            return None
        if hashlib.sha256(row[1].encode()).hexdigest() != row[2]:
            raise CheckpointError('checkpoint checksum mismatch')
        try:
            value = json.loads(row[1])
        except (TypeError, ValueError) as exc:
            raise CheckpointError('corrupt checkpoint') from exc
        if not isinstance(value, dict) or value.get('schema_version') != self.VERSION or value.get('artifact') != self.artifact:
            raise CheckpointError('incompatible checkpoint schema or artifact')
        return value

    def save(self, topic: str, partition: int, owner: str, value: dict):
        if self.owners.get((topic, partition)) != owner:
            raise CheckpointError('checkpoint ownership lost')
        data = dict(value, schema_version=self.VERSION, artifact=self.artifact)
        raw = json.dumps(data, sort_keys=True, separators=(',', ':'), allow_nan=False)
        checksum = hashlib.sha256(raw.encode()).hexdigest()
        with self.db:
            cursor = self.db.execute('UPDATE checkpoints SET payload=?,checksum=? WHERE grp=? AND topic=? AND part=? AND owner=?', (raw, checksum, self.group, topic, partition, owner))
            if cursor.rowcount != 1:
                raise CheckpointError('checkpoint ownership lost')

    def close(self):
        try:
            self.db.close()
        finally:
            for topic, partition in list(self.locks):
                self.release(topic, partition)
