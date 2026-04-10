import unittest

from emulator.metrics.runtime_metrics import RuntimeMetrics


class TestRuntimeMetricsTransientVisibility(unittest.TestCase):
    def test_snapshot_includes_transient_counts(self):
        metrics = RuntimeMetrics()

        metrics.record_transient("corrupter", "frontend request pressure", count=3)
        metrics.record_transient("repairer", "frontend request pressure", count=2)
        metrics.record_transient("unknown-source", "misc transient", count=4)

        snap = metrics.snapshot()
        transient = snap.get("transient")
        self.assertIsInstance(transient, dict)

        counts = transient.get("counts")
        self.assertIsInstance(counts, dict)
        self.assertEqual(counts.get("corrupter"), 3)
        self.assertEqual(counts.get("repairer"), 2)
        self.assertEqual(counts.get("other"), 4)

    def test_snapshot_includes_recent_transient_events(self):
        metrics = RuntimeMetrics()
        metrics.record_transient("corrupter", "frontend request pressure", count=5)

        snap = metrics.snapshot()
        transient = snap.get("transient")
        recent = transient.get("recent") if isinstance(transient, dict) else None
        self.assertIsInstance(recent, list)
        self.assertGreaterEqual(len(recent), 1)

        event = recent[-1]
        self.assertEqual(event.get("source"), "corrupter")
        self.assertEqual(event.get("message"), "frontend request pressure")
        self.assertEqual(event.get("count"), 5)


if __name__ == "__main__":
    unittest.main()
