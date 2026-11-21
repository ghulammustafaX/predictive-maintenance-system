import json
import sqlite3

import pytest

from ml.streaming.checkpoint import CheckpointError, PreprocessingCheckpoint, validate_checkpoint_path
from unittest.mock import patch


def test_checkpoint_rejects_concurrent_owner_and_fences_old_owner(tmp_path):
    path = str(tmp_path / 'state.sqlite')
    a = PreprocessingCheckpoint(path, 'group', 'artifact')
    b = PreprocessingCheckpoint(path, 'group', 'artifact')
    try:
        old = a.acquire('topic', 0)
        a.save('topic', 0, old, {'next_offset': 1, 'states': []})
        with pytest.raises(CheckpointError, match='already owned'):
            b.acquire('topic', 0)
        a.release('topic', 0)
        new = b.acquire('topic', 0)
        assert b.load('topic', 0, new)['next_offset'] == 1
        with pytest.raises(CheckpointError, match='ownership lost'):
            a.save('topic', 0, old, {'next_offset': 2, 'states': []})
        other = a.acquire('topic', 1)
        assert a.load('topic', 1, other) is None
    finally:
        a.close()
        b.close()


def test_checkpoint_rejects_artifact_mismatch_and_corruption(tmp_path):
    path = str(tmp_path / 'state.sqlite')
    a = PreprocessingCheckpoint(path, 'group', 'v1')
    owner = a.acquire('topic', 0)
    a.save('topic', 0, owner, {'next_offset': 1, 'states': []})
    a.close()
    b = PreprocessingCheckpoint(path, 'group', 'v2')
    owner = b.acquire('topic', 0)
    with pytest.raises(CheckpointError, match='artifact'):
        b.load('topic', 0, owner)
    b.close()
    db = sqlite3.connect(path)
    db.execute('UPDATE checkpoints SET payload=?', (json.dumps({'schema_version': 1}),))
    db.commit()
    db.close()
    c = PreprocessingCheckpoint(path, 'group', 'v1')
    owner = c.acquire('topic', 0)
    with pytest.raises(CheckpointError, match='checksum'):
        c.load('topic', 0, owner)
    c.close()


def test_startup_requires_a_dedicated_writable_mount(tmp_path):
    path = str(tmp_path / 'state.sqlite')
    with pytest.raises(CheckpointError):
        validate_checkpoint_path(None)
    with pytest.raises(CheckpointError):
        validate_checkpoint_path('relative.sqlite')
    with patch('ml.streaming.checkpoint._local_persistent_mount', return_value=False):
        with pytest.raises(CheckpointError, match='dedicated'):
            validate_checkpoint_path(path)
    with patch('ml.streaming.checkpoint._local_persistent_mount', return_value=True), patch('ml.streaming.checkpoint.os.access', return_value=False):
        with pytest.raises(CheckpointError, match='writable'):
            validate_checkpoint_path(path)
    with patch('ml.streaming.checkpoint._local_persistent_mount', return_value=True):
        validate_checkpoint_path(path)
