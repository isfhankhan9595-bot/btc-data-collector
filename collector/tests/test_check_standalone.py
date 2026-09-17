"""Tests for the standalone-dependency checker.

A checker that cannot tell a hard dependency from a guarded optional one is
worse than no checker: it goes red on correct code and trains people to
ignore it. That is exactly what happened on `main`, so the checker gets its
own adversarial tests.
"""
from __future__ import annotations

import pathlib

from collector.scripts.check_standalone import find_violations, main


def _write(tmp_path: pathlib.Path, name: str, source: str) -> pathlib.Path:
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source)
    return path


# ---------------------------------------------------------------------------
# Violations
# ---------------------------------------------------------------------------


def test_module_level_bare_import_is_a_violation(tmp_path):
    _write(tmp_path, "bad.py", "import telegram_bot\n")
    violations = find_violations(tmp_path)
    assert len(violations) == 1
    assert "telegram_bot" in violations[0]


def test_module_level_from_import_is_a_violation(tmp_path):
    _write(tmp_path, "bad.py", "from telegram_bot import send\n")
    assert len(find_violations(tmp_path)) == 1


def test_submodule_import_is_a_violation(tmp_path):
    _write(tmp_path, "bad.py", "import telegram_bot.client\n")
    assert len(find_violations(tmp_path)) == 1


def test_unguarded_function_local_import_is_a_violation(tmp_path):
    """Function-local but unguarded still raises into the caller."""
    _write(tmp_path, "bad.py", "def f():\n    from telegram_bot import send\n")
    assert len(find_violations(tmp_path)) == 1


def test_import_in_try_else_is_not_protected(tmp_path):
    source = (
        "try:\n"
        "    pass\n"
        "except Exception:\n"
        "    pass\n"
        "else:\n"
        "    import telegram_bot\n"
    )
    _write(tmp_path, "bad.py", source)
    assert len(find_violations(tmp_path)) == 1


def test_import_in_finally_is_not_protected(tmp_path):
    source = (
        "try:\n"
        "    pass\n"
        "finally:\n"
        "    import telegram_bot\n"
    )
    _write(tmp_path, "bad.py", source)
    assert len(find_violations(tmp_path)) == 1


def test_try_without_handlers_does_not_protect(tmp_path):
    source = (
        "try:\n"
        "    import telegram_bot\n"
        "finally:\n"
        "    pass\n"
    )
    _write(tmp_path, "bad.py", source)
    assert len(find_violations(tmp_path)) == 1


# ---------------------------------------------------------------------------
# Permitted patterns
# ---------------------------------------------------------------------------


def test_guarded_module_level_import_is_allowed(tmp_path):
    source = (
        "try:\n"
        "    import telegram_bot\n"
        "except Exception:\n"
        "    telegram_bot = None\n"
    )
    _write(tmp_path, "ok.py", source)
    assert find_violations(tmp_path) == []


def test_guarded_function_local_import_is_allowed(tmp_path):
    """The pattern notifications.py actually uses."""
    source = (
        "def resolve():\n"
        "    try:\n"
        "        from telegram_bot import send_telegram_message\n"
        "    except Exception:\n"
        "        return None\n"
        "    return send_telegram_message\n"
    )
    _write(tmp_path, "ok.py", source)
    assert find_violations(tmp_path) == []


def test_nested_guard_inside_function_inside_try_is_allowed(tmp_path):
    source = (
        "try:\n"
        "    def f():\n"
        "        import telegram_bot\n"
        "except Exception:\n"
        "    pass\n"
    )
    _write(tmp_path, "ok.py", source)
    assert find_violations(tmp_path) == []


def test_unrelated_imports_are_ignored(tmp_path):
    _write(tmp_path, "ok.py", "import os\nfrom pathlib import Path\n")
    assert find_violations(tmp_path) == []


def test_relative_imports_are_ignored(tmp_path):
    _write(tmp_path, "ok.py", "from . import telegram_bot\n")
    assert find_violations(tmp_path) == []


def test_test_files_are_excluded(tmp_path):
    _write(tmp_path, "tests/test_x.py", "import telegram_bot\n")
    assert find_violations(tmp_path) == []


def test_unparseable_file_is_reported_not_skipped(tmp_path):
    _write(tmp_path, "broken.py", "def f(:\n")
    violations = find_violations(tmp_path)
    assert len(violations) == 1
    assert "unparseable" in violations[0]


# ---------------------------------------------------------------------------
# The real repository must pass
# ---------------------------------------------------------------------------


def test_the_actual_collector_package_is_standalone():
    root = pathlib.Path(__file__).resolve().parents[2] / "collector"
    assert find_violations(root) == []


def test_cli_returns_zero_for_clean_tree(tmp_path, capsys):
    _write(tmp_path, "ok.py", "import os\n")
    assert main([str(tmp_path)]) == 0
    assert "Standalone OK" in capsys.readouterr().out


def test_cli_returns_one_and_names_the_offender(tmp_path, capsys):
    _write(tmp_path, "bad.py", "import telegram_bot\n")
    assert main([str(tmp_path)]) == 1
    output = capsys.readouterr().out
    assert "bad.py" in output
    assert "try/except" in output
