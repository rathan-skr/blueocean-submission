"""Load the incumbent modules unmodified, with their external clients stubbed where we don't need them.

Each reproducer runs the SAME property against the incumbent logic and against our fix. The
incumbent case is marked xfail(strict=True): it must actually fail (if it ever passes, the suite
fails, so the reproducer cannot rot silently).
"""
import importlib.util
import os
import sys
import types

import pytest

INC = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "incumbent")

# Every reproducer runs twice: once against the incumbent (must fail), once against the fix.
# raises=AssertionError: an import error or crash must NOT count as "reproduced".
INCUMBENT = pytest.param("incumbent", marks=pytest.mark.xfail(strict=True, raises=AssertionError,
                                                              reason="incumbent defect reproduced"))
FIXED = "fixed"
IMPLS = [INCUMBENT, FIXED]


def _stub_kafka():
    if "confluent_kafka" in sys.modules:
        return
    m = types.ModuleType("confluent_kafka")

    class _Dummy:
        def __init__(self, *a, **k):
            pass

        def __getattr__(self, name):
            return lambda *a, **k: None

    m.Consumer = m.Producer = _Dummy
    m.TopicPartition = lambda *a, **k: a
    sys.modules["confluent_kafka"] = m


def load(name: str, **env):
    """Import incumbent/<name>.py as a fresh module (env vars applied first)."""
    _stub_kafka()
    for k, v in env.items():
        os.environ[k] = v
    if INC not in sys.path:
        sys.path.insert(0, INC)           # session_reader imports `leader`
    spec = importlib.util.spec_from_file_location(f"incumbent_{name}", os.path.join(INC, f"{name}.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
