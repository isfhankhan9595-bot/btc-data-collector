import sys

from collector.collector.disk_monitor import DiskMonitor


class _MockShutil:
    def __init__(self, free_gib):
        self._free = int(free_gib * 1024 ** 3)

    def disk_usage(self, path):
        return (1000 * 1024 ** 3, 1000 * 1024 ** 3 - self._free, self._free)


def test_disk_monitor_get_free_gb(monkeypatch):
    monkeypatch.setattr("collector.collector.disk_monitor.shutil", _MockShutil(500))
    assert DiskMonitor().get_free_gb() == 500.0


def test_disk_monitor_uses_the_configured_directory(monkeypatch):
    seen = []

    class Spy(_MockShutil):
        def disk_usage(self, path):
            seen.append(path)
            return super().disk_usage(path)

    monkeypatch.setattr("collector.collector.disk_monitor.shutil", Spy(10))
    DiskMonitor(data_dir="/some/other/dir").get_free_gb()
    assert seen == ["/some/other/dir"]


def test_disk_monitor_never_exits_or_calls_back_even_below_emergency(monkeypatch):
    """F5-QA-04: the monitor is a probe. It must not call sys.exit or a shutdown callback
    (the old emergency path ran against a half-initialised app)."""
    monkeypatch.setattr("collector.collector.disk_monitor.shutil", _MockShutil(1))

    def boom(code=None):  # pragma: no cover - must not run
        raise AssertionError("DiskMonitor must not call sys.exit")

    monkeypatch.setattr(sys, "exit", boom)
    monitor = DiskMonitor()
    assert monitor.get_free_gb() == 1.0
    assert not hasattr(monitor, "check_disk_space")
    assert not hasattr(monitor, "shutdown_callback")
