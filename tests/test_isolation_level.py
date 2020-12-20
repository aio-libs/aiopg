import pytest

from aiopg import IsolationLevel


@pytest.mark.parametrize('isolation_level, name', [
    (IsolationLevel.default, 'Default'),
    (IsolationLevel.read_committed, 'Read committed'),
    (IsolationLevel.repeatable_read, 'Repeatable read'),
    (IsolationLevel.serializable, 'Serializable'),
])
def test_isolation_level_name(isolation_level, name):
    assert isolation_level(False, False).name == name
