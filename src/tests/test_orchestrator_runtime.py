import unittest

from emulator.orchestrator.runtime import SystemOrchestrator
from emulator.storage.engine import DbEngine


class _FakeCacheClient:
    def __init__(self, value):
        self._value = value

    def get(self, key):
        if key != "corrupted_rows":
            raise AssertionError(f"unexpected key: {key}")
        return self._value


class TestSystemOrchestratorCorruptedRows(unittest.TestCase):
    def test_get_corrupted_rows_count_returns_zero_when_missing(self):
        orchestrator = SystemOrchestrator(
            db_lookup_strategy=DbEngine.STRATEGY_LINEAR,
            corrupter_count=0,
            repairer_count=0,
            client_pause_ms=0.0,
        )
        self.addCleanup(orchestrator.stop)
        orchestrator._cache_client = _FakeCacheClient(None)  # type: ignore[assignment]

        self.assertEqual(orchestrator.get_corrupted_rows_count(), 0)

    def test_get_corrupted_rows_count_returns_integer_value(self):
        orchestrator = SystemOrchestrator(
            db_lookup_strategy=DbEngine.STRATEGY_LINEAR,
            corrupter_count=0,
            repairer_count=0,
            client_pause_ms=0.0,
        )
        self.addCleanup(orchestrator.stop)
        orchestrator._cache_client = _FakeCacheClient(7)  # type: ignore[assignment]

        self.assertEqual(orchestrator.get_corrupted_rows_count(), 7)

    def test_get_corrupted_rows_count_rejects_non_integer_values(self):
        orchestrator = SystemOrchestrator(
            db_lookup_strategy=DbEngine.STRATEGY_LINEAR,
            corrupter_count=0,
            repairer_count=0,
            client_pause_ms=0.0,
        )
        self.addCleanup(orchestrator.stop)
        orchestrator._cache_client = _FakeCacheClient("bad")  # type: ignore[assignment]

        with self.assertRaises(RuntimeError):
            orchestrator.get_corrupted_rows_count()


if __name__ == "__main__":
    unittest.main()