import pytest

from aiopg import IsolationLevel


@pytest.mark.parametrize('isolation_level', [
    IsolationLevel.default,
    IsolationLevel.read_committed,
    IsolationLevel.repeatable_read,
])
@pytest.mark.parametrize('readonly, deferrable', [
    (True, False),
    (False, True),
    (True, True),
])
def test_isolation_level_readonly_or_deferrable_not_supported(
        isolation_level, readonly, deferrable
):
    with pytest.raises(ValueError):
        isolation_level(readonly, deferrable)


@pytest.mark.parametrize('isolation_level, name', [
    (IsolationLevel.default, 'Default'),
    (IsolationLevel.read_committed, 'Read committed'),
    (IsolationLevel.repeatable_read, 'Repeatable read'),
    (IsolationLevel.serializable, 'Serializable'),
])
def test_isolation_level_name(isolation_level, name):
    assert isolation_level(False, False).name == name
