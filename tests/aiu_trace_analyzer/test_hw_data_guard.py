# Copyright 2026 IBM Corporation

'''
Guard: HW timestamps and power values are accessed only through aiu_trace_analyzer.hw_data.

Fails if any other module of the package uses the raw arg keys of either dialect:
  - timestamp keys (TS1..TS5, cycles_ts) as any string constant
  - power keys (Power, charge) as a dict key: x["Power"], "Power" in x, x.get("Power")
    ("Power" is also used as the name of the generated counter events, which is fine)
'''

import ast
from pathlib import Path

import aiu_trace_analyzer

_PKG_DIR = Path(aiu_trace_analyzer.__file__).parent

_TS_KEYS = {"TS1", "TS2", "TS3", "TS4", "TS5", "cycles_ts"}
_POWER_KEYS = {"Power", "charge"}

# modules that define or implement the HW data layout
_EXCLUDED_FILES = {"hw_data.py", "dialect.py"}

# (module path relative to the package, function) still using raw keys on purpose
_ALLOWED = {
    ("pipeline/tripple_event.py", "tripple_phased_events"),
    ("pipeline/timesync.py", "_create_dev_ts_list_in_us"),
}


def _is_str(node: ast.AST, keys: set[str]) -> bool:
    return isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value in keys


class _RawKeyFinder(ast.NodeVisitor):
    def __init__(self, module: str) -> None:
        self.module = module
        self.functions: list[str] = []
        self.findings: list[str] = []

    def _report(self, node: ast.AST, key: str) -> None:
        function = self.functions[-1] if self.functions else "<module>"
        if (self.module, function) not in _ALLOWED:
            self.findings.append(f"{self.module}:{node.lineno} ({function}): '{key}'")

    def _visit_function(self, node) -> None:
        self.functions.append(node.name)
        self.generic_visit(node)
        self.functions.pop()

    visit_FunctionDef = _visit_function
    visit_AsyncFunctionDef = _visit_function

    def visit_Constant(self, node: ast.Constant) -> None:
        if _is_str(node, _TS_KEYS):
            self._report(node, node.value)

    def visit_Subscript(self, node: ast.Subscript) -> None:
        if _is_str(node.slice, _POWER_KEYS):
            self._report(node, node.slice.value)
        self.generic_visit(node)

    def visit_Compare(self, node: ast.Compare) -> None:
        if _is_str(node.left, _POWER_KEYS) and any(isinstance(op, (ast.In, ast.NotIn)) for op in node.ops):
            self._report(node, node.left.value)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Attribute) and node.func.attr in ("get", "pop") and node.args \
                and _is_str(node.args[0], _POWER_KEYS):
            self._report(node, node.args[0].value)
        self.generic_visit(node)


def test_no_raw_hw_data_keys_outside_hw_data():
    findings = []
    for path in sorted(_PKG_DIR.rglob("*.py")):
        if path.name in _EXCLUDED_FILES:
            continue
        module = path.relative_to(_PKG_DIR).as_posix()
        finder = _RawKeyFinder(module)
        finder.visit(ast.parse(path.read_text(), filename=str(path)))
        findings += finder.findings
    assert not findings, "raw HW data keys outside hw_data.py (use the hw_data accessors):\n  " + "\n  ".join(findings)


def test_guard_detects_raw_keys():
    source = '''
x = event["args"]["TS3"]
if "Power" in event["args"]:
    y = event["args"].get("charge")
name = "Power"   # counter name, not a key access
'''
    finder = _RawKeyFinder("pipeline/example.py")
    finder.visit(ast.parse(source))
    assert [f.split(" ")[-1] for f in finder.findings] == ["'TS3'", "'Power'", "'charge'"]
