"""Replace container-created JSON via the runner-owned evidence directory."""
import json
import os
from pathlib import Path
import tempfile


def atomic_update(path, update, root='m3-evidence'):
    root = Path(root).resolve()
    target = Path(path)
    if target.is_symlink() or target.resolve().parent != root or not target.is_file():
        raise ValueError('Evidence target must be a regular file in the evidence directory')
    original = json.loads(target.read_text(encoding='utf-8'))
    if not isinstance(original, dict):
        raise ValueError('Evidence must be a JSON object')
    update(original)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=root,
                                         prefix='.runner-evidence-', delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(original, stream, indent=2, sort_keys=True)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        temporary = None
        if json.loads(target.read_text(encoding='utf-8')) != original:
            raise OSError('Runner evidence readback did not match')
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return original


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3 or sys.argv[1] != 'check':
        raise SystemExit('usage: module3_runner_evidence.py check PATH')
    atomic_update(sys.argv[2], lambda value: None)
    print('Runner evidence read and atomic update succeeded')
