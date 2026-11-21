import hashlib

from ml.models.compatibility import _statistics_hash_matches


def test_statistics_hash_accepts_only_line_ending_conversion(tmp_path):
    original = b'{\r\n  "mean": 1.25\r\n}\r\n'
    expected = hashlib.sha256(original).hexdigest()
    path = tmp_path / 'stats.json'
    path.write_bytes(original.replace(b'\r\n', b'\n'))
    assert _statistics_hash_matches(path, expected)
    path.write_bytes(original.replace(b'1.25', b'1.26'))
    assert not _statistics_hash_matches(path, expected)
    path.write_bytes(original.replace(b'\r\n', b'\r'))
    assert not _statistics_hash_matches(path, expected)


def test_statistics_hash_accepts_reverse_conversion(tmp_path):
    original = b'{\n  "mean": 1.25\n}\n'
    path = tmp_path / 'stats.json'
    path.write_bytes(original.replace(b'\n', b'\r\n'))
    assert _statistics_hash_matches(path, hashlib.sha256(original).hexdigest())
