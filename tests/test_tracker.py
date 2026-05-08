"""Tests for WriteRateTracker sliding window logic."""

import time
import threading
import pytest
from aci_cache.tracker import WriteRateTracker


class TestBasicRate:
    """Write rate calculation with sliding window."""

    def test_empty_tracker_zero_rate(self):
        tracker = WriteRateTracker(window=5.0)
        assert tracker.get_rate() == 0.0

    def test_single_write(self):
        tracker = WriteRateTracker(window=5.0)
        tracker.record()
        rate = tracker.get_rate()
        # 1 write in 5 seconds = 0.2 w/s
        assert rate == pytest.approx(0.2, abs=0.05)

    def test_multiple_writes(self):
        tracker = WriteRateTracker(window=5.0)
        for _ in range(10):
            tracker.record()
        rate = tracker.get_rate()
        # 10 writes in 5 seconds = 2.0 w/s
        assert rate == pytest.approx(2.0, abs=0.1)

    def test_get_count(self):
        tracker = WriteRateTracker(window=5.0)
        for _ in range(5):
            tracker.record()
        assert tracker.get_count() == 5


class TestWindowExpiry:
    """Old timestamps should be trimmed from the window."""

    def test_expired_writes_are_trimmed(self):
        tracker = WriteRateTracker(window=0.1)  # 100ms window
        tracker.record()
        time.sleep(0.15)  # Wait for expiry
        assert tracker.get_rate() == 0.0
        assert tracker.get_count() == 0


class TestValidation:
    """Constructor validation."""

    def test_zero_window_raises(self):
        with pytest.raises(ValueError, match="positive"):
            WriteRateTracker(window=0)

    def test_negative_window_raises(self):
        with pytest.raises(ValueError, match="positive"):
            WriteRateTracker(window=-1)


class TestThreadSafety:
    """Concurrent access should not corrupt state."""

    def test_concurrent_writes(self):
        tracker = WriteRateTracker(window=5.0)
        num_threads = 10
        writes_per_thread = 100
        barrier = threading.Barrier(num_threads)

        def worker():
            barrier.wait()
            for _ in range(writes_per_thread):
                tracker.record()

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        expected = num_threads * writes_per_thread
        assert tracker.get_count() == expected
