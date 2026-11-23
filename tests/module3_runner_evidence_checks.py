import json
import os
import pytest
from module3_runner_evidence import atomic_update


def test_runner_replaces_read_only_container_artifact_without_world_write(tmp_path):
    root = tmp_path / 'evidence'
    root.mkdir()
    target = root / 'container.json'
    target.write_text('{"status":"incomplete"}', encoding='utf-8')
    target.chmod(0o444)
    atomic_update(target, lambda record: record.update(worker_status='running'), root=root)
    assert json.loads(target.read_text()) == {'status': 'incomplete', 'worker_status': 'running'}
    if os.name != 'nt':
        assert not (target.stat().st_mode & 0o002)


def test_runner_refuses_paths_outside_evidence_directory(tmp_path):
    root = tmp_path / 'evidence'
    root.mkdir()
    outside = tmp_path / 'outside.json'
    outside.write_text('{}')
    with pytest.raises(ValueError):
        atomic_update(outside, lambda record: None, root=root)
