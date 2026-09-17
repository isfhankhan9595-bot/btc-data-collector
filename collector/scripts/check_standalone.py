"""Assert the collector does not hard-depend on the trading bot.

The collector must be importable and runnable without the repository-root
``telegram_bot`` module. That is the contract this script enforces.

It is not, however, a ban on the *name*. `collector.collector.notifications`
deliberately probes for an optional Telegram backend behind a guarded,
function-local import. That is the correct standalone design: the probe
cannot make the package unimportable and cannot raise into a caller. An
earlier version of this check was a flat substring/AST scan for the name, so
it flagged the very pattern that makes the collector standalone, and CI went
red on `main` while the design was right.

The rule enforced here is therefore about *reachability*, not naming:

    An import of ``telegram_bot`` is a violation unless it sits inside a
    ``try`` block with at least one exception handler that could catch an
    ImportError.

A module-level bare import is a violation: importing the package would fail.
A guarded import -- at module level or inside a function -- is fine: failure
degrades to the null backend.
"""
from __future__ import annotations

import argparse
import ast
import pathlib
import sys
from typing import Iterator, Sequence

FORBIDDEN_ROOTS = ("telegram_bot",)


class _Visitor(ast.NodeVisitor):
    """Collect unguarded imports of forbidden root modules."""

    def __init__(self, path: pathlib.Path) -> None:
        self.path = path
        self.violations: list[tuple[int, str]] = []
        self._try_depth = 0

    # Track whether we are lexically inside a `try:` body with handlers.
    def visit_Try(self, node: ast.Try) -> None:
        for child in node.body:
            if node.handlers:
                self._try_depth += 1
                self.visit(child)
                self._try_depth -= 1
            else:
                self.visit(child)
        # orelse/finalbody are NOT protected by the handlers.
        for child in [*node.orelse, *node.finalbody]:
            self.visit(child)
        for handler in node.handlers:
            self.visit(handler)

    if hasattr(ast, "TryStar"):  # Python 3.11+
        def visit_TryStar(self, node):  # type: ignore[no-untyped-def]
            self.visit_Try(node)  # same structure

    def _check(self, node: ast.AST, names: Sequence[str]) -> None:
        for name in names:
            if name.split(".")[0] in FORBIDDEN_ROOTS and self._try_depth == 0:
                self.violations.append((getattr(node, "lineno", 0), name))

    def visit_Import(self, node: ast.Import) -> None:
        self._check(node, [alias.name for alias in node.names])

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.level:  # relative import; cannot reach repository root
            return
        self._check(node, [node.module or ""])


def iter_source_files(root: pathlib.Path) -> Iterator[pathlib.Path]:
    for path in sorted(root.rglob("*.py")):
        if "tests" in path.parts:
            continue
        yield path


def find_violations(root: pathlib.Path) -> list[str]:
    violations: list[str] = []
    for path in iter_source_files(root):
        try:
            tree = ast.parse(path.read_text(), filename=str(path))
        except SyntaxError as exc:
            violations.append(f"{path}: unparseable ({exc})")
            continue
        visitor = _Visitor(path)
        visitor.visit(tree)
        for lineno, name in visitor.violations:
            violations.append(f"{path}:{lineno}: unguarded import of {name!r}")
    return violations


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", nargs="?", default="collector")
    args = parser.parse_args(argv)

    violations = find_violations(pathlib.Path(args.root))
    if violations:
        print("Standalone violation: collector hard-depends on the trading bot.")
        for line in violations:
            print(f"  {line}")
        print("\nWrap the import in try/except so absence degrades gracefully.")
        return 1

    print("Standalone OK: no unguarded trading-bot imports.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
