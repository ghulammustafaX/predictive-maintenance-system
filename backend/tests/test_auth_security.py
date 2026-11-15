from app.security import hash_password, validate_password_strength, verify_password


def test_password_hashing_round_trip():
    password = "SafeTestPassword1!"
    hashed = hash_password(password)

    assert hashed != password
    assert verify_password(password, hashed)
    assert not verify_password("WrongPassword1!", hashed)


def test_password_strength_rejects_weak_password():
    try:
        validate_password_strength("weak")
    except ValueError:
        return
    raise AssertionError("weak passwords must be rejected")
