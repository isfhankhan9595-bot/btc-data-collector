import shutil
from typing import Optional


class DiskMonitor:
    """Read-only free-space probe used by the health summary.

    F5-QA-04: this class no longer classifies pressure, alerts, invokes a shutdown callback
    or calls ``sys.exit``. The old ``check_disk_space`` ran once in the Futures constructor,
    before the app was initialised, and its shutdown callback hit a half-built object. The
    continuous, state-machine-based policy lives in ``disk_pressure.StoragePressureMonitor``.
    """

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir

    def get_free_gb(self) -> Optional[float]:
        """Free GiB on the filesystem containing ``data_dir``."""
        total, used, free = shutil.disk_usage(self.data_dir)
        return free / (1024 ** 3)
