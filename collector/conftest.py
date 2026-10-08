import sys
import os

import pytest

# Insert the parent of the project root so that 'collector' resolves to the
# outer collector/ directory, allowing 'from collector.collector.X' imports.
_project_parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_parent not in sys.path:
    sys.path.insert(0, _project_parent)


@pytest.fixture(autouse=True)
def _hermetic_storage_filesystem_policy(monkeypatch):
    """F1 refuses to promote unmarked segments on a filesystem it cannot verify
    (anything but ext4/xfs). Developer machines often run tests on tmpfs or
    overlayfs, so the suite opts in to the documented operator override. The
    guard itself is tested in tests/test_f1_durable_publication.py with explicit
    ``mountinfo``/``environ`` arguments, which never read this variable."""
    monkeypatch.setenv("COLLECTOR_ALLOW_UNVERIFIED_FS", "1")
