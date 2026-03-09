from __future__ import annotations

import importlib
import importlib.util
import sys
import threading
import time
import types
from pathlib import Path

import pytest


def _load_bridge(monkeypatch):
    fake_bn = types.ModuleType("binaryninja")

    class SymbolType:
        FunctionSymbol = "SymbolType.FunctionSymbol"
        DataSymbol = "SymbolType.DataSymbol"
        ImportedFunctionSymbol = "SymbolType.ImportedFunctionSymbol"

    class Symbol:
        def __init__(self, symbol_type, address, name):
            self.type = symbol_type
            self.address = address
            self.name = name
            self.raw_name = name

    fake_bn.SymbolType = SymbolType
    fake_bn.Symbol = Symbol
    fake_bn.log_info = lambda *args, **kwargs: None
    fake_bn.log_warn = lambda *args, **kwargs: None
    fake_bn.log_error = lambda *args, **kwargs: None

    fake_mainthread = types.ModuleType("binaryninja.mainthread")
    fake_mainthread.execute_on_main_thread_and_wait = lambda func: func()
    fake_mainthread.is_main_thread = lambda: True

    fake_plugin = types.ModuleType("binaryninja.plugin")

    class PluginCommand:
        @staticmethod
        def register(*args, **kwargs):
            return None

    fake_plugin.PluginCommand = PluginCommand

    monkeypatch.setitem(sys.modules, "binaryninja", fake_bn)
    monkeypatch.setitem(sys.modules, "binaryninja.mainthread", fake_mainthread)
    monkeypatch.setitem(sys.modules, "binaryninja.plugin", fake_plugin)
    monkeypatch.delitem(sys.modules, "binaryninjaui", raising=False)
    package_name = "bn_test_bridge"
    module_name = f"{package_name}.bridge"
    monkeypatch.delitem(sys.modules, module_name, raising=False)
    monkeypatch.delitem(sys.modules, package_name, raising=False)

    bridge_path = Path(__file__).resolve().parents[1] / "plugin" / "bn_agent_bridge" / "bridge.py"
    package = types.ModuleType(package_name)
    package.__path__ = [str(bridge_path.parent)]
    monkeypatch.setitem(sys.modules, package_name, package)
    spec = importlib.util.spec_from_file_location(module_name, bridge_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, module_name, module)
    spec.loader.exec_module(module)
    return module


class _FakeFunction:
    def __init__(self, start: int, name: str, type_text: str = "int32_t()"):
        self.start = start
        self.name = name
        self.raw_name = name
        self.type = type_text
        self.parameter_vars = []
        self.stack_layout = []
        self.calling_convention = "__cdecl"
        self.return_type = "int32_t"
        self.basic_blocks = []


class _FakeVariable:
    def __init__(
        self,
        *,
        name: str,
        storage: int,
        var_type: str,
        identifier: int,
        index: int = 0,
        source_type: str = "StackVariableSourceType",
    ):
        self.name = name
        self.storage = storage
        self.type = var_type
        self.identifier = identifier
        self.index = index
        self.source_type = types.SimpleNamespace(name=source_type)


class _FakeBV:
    def __init__(self, *, functions=None, symbols=None):
        self.functions = list(functions or [])
        self._symbols = list(symbols or [])

    def get_function_at(self, address: int):
        for fn in self.functions:
            if int(fn.start) == int(address):
                return fn
        return None

    def get_symbols_by_name(self, name: str):
        return [symbol for symbol in self._symbols if getattr(symbol, "name", None) == name]

    def get_symbol_by_raw_name(self, name: str):
        for symbol in self._symbols:
            if getattr(symbol, "raw_name", None) == name:
                return symbol
        return None

    def get_symbols(self):
        return list(self._symbols)

    def get_symbol_at(self, address: int):
        for symbol in self._symbols:
            if int(symbol.address) == int(address):
                return symbol
        return None


class _FakeMutationBV(_FakeBV):
    def __init__(self):
        super().__init__()
        self.events: list[tuple[str, str] | str] = []

    def begin_undo_actions(self):
        self.events.append("begin")
        return "state"

    def update_analysis_and_wait(self):
        self.events.append("refresh")

    def revert_undo_actions(self, state):
        self.events.append(("revert", state))

    def commit_undo_actions(self, state):
        self.events.append(("commit", state))


class _ParseResult:
    def __init__(self, *, types=None, variables=None, functions=None):
        self.types = dict(types or {})
        self.variables = dict(variables or {})
        self.functions = dict(functions or {})


def test_resolve_rename_target_rejects_ambiguous_function_identifier(monkeypatch):
    bridge = _load_bridge(monkeypatch)
    instance = bridge.BinaryNinjaBridge()
    bv = _FakeBV(
        functions=[
            _FakeFunction(0x401000, "duplicate_name"),
            _FakeFunction(0x402000, "duplicate_name"),
        ]
    )

    with pytest.raises(bridge.OperationFailure, match="Ambiguous function identifier"):
        instance._resolve_rename_target(bv, "duplicate_name", "function")


def test_verify_rename_symbol_reports_noop(monkeypatch):
    bridge = _load_bridge(monkeypatch)
    instance = bridge.BinaryNinjaBridge()
    bv = _FakeBV(functions=[_FakeFunction(0x401000, "player_update")])

    result = instance._verify_operation(
        bv,
        {
            "op": "rename_symbol",
            "kind": "function",
            "address": "0x401000",
            "before_name": "player_update",
            "new_name": "player_update",
            "requested": {
                "op": "rename_symbol",
                "identifier": "player_update",
                "new_name": "player_update",
            },
        },
    )

    assert result["status"] == "noop"
    assert result["observed"]["name"] == "player_update"


def test_mutation_reverts_on_verification_failure(monkeypatch):
    bridge = _load_bridge(monkeypatch)
    instance = bridge.BinaryNinjaBridge()
    bv = _FakeMutationBV()

    monkeypatch.setattr(instance, "_resolve_view", lambda selector: bv)
    monkeypatch.setattr(instance, "_guess_affected_functions", lambda bv, operations: [])
    monkeypatch.setattr(instance, "_capture_function_snapshots", lambda bv, functions: {})
    monkeypatch.setattr(instance, "_capture_type_snapshots", lambda bv, operations: {})
    monkeypatch.setattr(instance, "_diff_snapshots", lambda before, after: [])
    monkeypatch.setattr(instance, "_diff_type_snapshots", lambda before, after: [])
    monkeypatch.setattr(
        instance,
        "_apply_operation",
        lambda bv, op: {
            "op": "rename_symbol",
            "kind": "function",
            "address": "0x401000",
            "new_name": "player_update",
            "requested": {"identifier": "sub_401000", "new_name": "player_update"},
        },
    )
    monkeypatch.setattr(
        instance,
        "_verify_operation",
        lambda bv, result: {
            **result,
            "status": "verification_failed",
            "message": "Live rename verification failed at 0x401000",
        },
    )

    result = instance._mutation("active", False, [{"op": "rename_symbol"}])

    assert result["success"] is False
    assert result["committed"] is False
    assert ("revert", "state") in bv.events
    assert ("commit", "state") not in bv.events


def test_refresh_updates_analysis_and_returns_target_info(monkeypatch):
    bridge = _load_bridge(monkeypatch)
    instance = bridge.BinaryNinjaBridge()
    bv = _FakeMutationBV()

    monkeypatch.setattr(instance, "_resolve_view", lambda selector: bv)
    monkeypatch.setattr(instance, "_target_info", lambda selector: {"selector": "SnailMail_unwrapped.exe.bndb"})

    result = instance._refresh("active")

    assert result["refreshed"] is True
    assert result["target"]["selector"] == "SnailMail_unwrapped.exe.bndb"
    assert "refresh" in bv.events


def test_parse_declaration_source_uses_platform_parser_with_source_path(monkeypatch, tmp_path):
    bridge = _load_bridge(monkeypatch)
    instance = bridge.BinaryNinjaBridge()
    recorded = {}

    class _Platform:
        def parse_types_from_source(self, source, **kwargs):
            recorded["source"] = source
            recorded["kwargs"] = kwargs
            return _ParseResult(types={"Player": "struct Player"})

    class _SourceBV(_FakeBV):
        def __init__(self):
            super().__init__()
            self.platform = _Platform()

        def parse_types_from_string(self, declaration):
            raise AssertionError("string parser should not be used when source parsing succeeds")

    header_path = tmp_path / "win32_min.h"
    header_path.write_text("typedef struct Player { int hp; } Player;", encoding="utf-8")
    bv = _SourceBV()

    parsed = instance._parse_declaration_source(bv, header_path.read_text(encoding="utf-8"), source_path=str(header_path))

    assert [name for name, _ in parsed["types"]] == ["Player"]
    assert recorded["kwargs"]["filename"] == str(header_path)
    assert recorded["kwargs"]["include_dirs"] == [str(header_path.parent.resolve())]


def test_op_types_declare_accepts_source_without_named_types(monkeypatch):
    bridge = _load_bridge(monkeypatch)
    instance = bridge.BinaryNinjaBridge()

    class _Platform:
        def parse_types_from_source(self, source, **kwargs):
            return _ParseResult(
                functions={"DirectInput8Create": "int32_t(void)"},
                variables={"GUID_SysKeyboard": "GUID"},
            )

    class _SourceOnlyBV(_FakeBV):
        def __init__(self):
            super().__init__()
            self.platform = _Platform()
            self.defined: list[tuple[str, str]] = []

        def parse_types_from_string(self, declaration):
            raise AssertionError("string parser should not be used when source parsing succeeds")

        def get_type_by_name(self, name):
            return None

        def define_user_type(self, name, type_obj):
            self.defined.append((name, type_obj))

    bv = _SourceOnlyBV()

    result = instance._op_types_declare(
        bv,
        {
            "op": "types_declare",
            "declaration": "extern const GUID GUID_SysKeyboard;",
            "source_path": "/tmp/win32_min.h",
        },
    )

    assert result["count"] == 0
    assert result["defined_types"] == {}
    assert result["parsed_functions"] == ["DirectInput8Create"]
    assert result["parsed_variables"] == ["GUID_SysKeyboard"]
    assert bv.defined == []


def test_list_locals_returns_stable_ids(monkeypatch):
    bridge = _load_bridge(monkeypatch)
    instance = bridge.BinaryNinjaBridge()
    fn = _FakeFunction(0x401000, "player_update", "int32_t player_update(int32_t arg1)")
    fn.parameter_vars = [
        _FakeVariable(name="arg1", storage=4, var_type="int32_t", identifier=1001, index=0)
    ]
    fn.stack_layout = [
        _FakeVariable(name="var_4", storage=-4, var_type="float", identifier=2001, index=1)
    ]
    bv = _FakeBV(functions=[fn])
    monkeypatch.setattr(instance, "_resolve_view", lambda selector: bv)

    result = instance._list_locals_for_function("active", "player_update")

    assert result["function"]["name"] == "player_update"
    assert len(result["locals"]) == 2
    assert result["locals"][0]["local_id"].startswith("0x401000:param:")
    assert result["locals"][1]["local_id"].startswith("0x401000:local:")


def test_list_locals_skips_stack_aliases_for_parameters(monkeypatch):
    bridge = _load_bridge(monkeypatch)
    instance = bridge.BinaryNinjaBridge()
    fn = _FakeFunction(0x401000, "player_update")
    parameter = _FakeVariable(name="arg1", storage=4, var_type="int32_t", identifier=1001)
    alias = _FakeVariable(name="arg1", storage=4, var_type="int32_t", identifier=1001)
    local = _FakeVariable(name="var_4", storage=-4, var_type="float", identifier=2001)
    fn.parameter_vars = [parameter]
    fn.stack_layout = [alias, local]

    locals_list = instance._list_locals(fn)

    assert len(locals_list) == 2
    assert [item["local_id"] for item in locals_list] == [
        "0x401000:param:StackVariableSourceType:4:0:1001",
        "0x401000:local:StackVariableSourceType:-4:0:2001",
    ]


def test_find_variable_selector_prefers_local_id(monkeypatch):
    bridge = _load_bridge(monkeypatch)
    instance = bridge.BinaryNinjaBridge()
    fn = _FakeFunction(0x401000, "player_update")
    shared = _FakeVariable(name="tmp", storage=-4, var_type="int32_t", identifier=2001)
    duplicate = _FakeVariable(name="tmp", storage=-8, var_type="int32_t", identifier=2002)
    fn.stack_layout = [shared, duplicate]

    local_id = instance._local_id(fn, duplicate, is_parameter=False)
    found, is_parameter = instance._find_variable_selector(fn, local_id)

    assert found is duplicate
    assert is_parameter is False


def test_function_info_includes_metadata(monkeypatch):
    bridge = _load_bridge(monkeypatch)
    instance = bridge.BinaryNinjaBridge()
    fn = _FakeFunction(0x401000, "player_update", "int32_t player_update(int32_t arg1)")
    fn.parameter_vars = [
        _FakeVariable(name="arg1", storage=4, var_type="int32_t", identifier=1001, index=0)
    ]
    bv = _FakeBV(functions=[fn])
    monkeypatch.setattr(instance, "_resolve_view", lambda selector: bv)

    result = instance._function_info("active", "player_update")

    assert result["prototype"] == "int32_t player_update(int32_t arg1)"
    assert result["return_type"] == "int32_t"
    assert result["calling_convention"] == "__cdecl"
    assert result["size"] is None


def test_list_functions_is_sorted_by_address(monkeypatch):
    bridge = _load_bridge(monkeypatch)
    instance = bridge.BinaryNinjaBridge()
    bv = _FakeBV(
        functions=[
            _FakeFunction(0x402000, "sub_402000"),
            _FakeFunction(0x401000, "sub_401000"),
        ]
    )
    monkeypatch.setattr(instance, "_resolve_view", lambda selector: bv)

    result = instance._list_functions("active", 0, 10)

    assert [item["address"] for item in result] == ["0x401000", "0x402000"]


def test_py_exec_non_serializable_result_falls_back_to_repr(monkeypatch):
    bridge = _load_bridge(monkeypatch)
    instance = bridge.BinaryNinjaBridge()
    bv = _FakeBV()
    monkeypatch.setattr(instance, "_resolve_view", lambda selector: bv)

    result = instance._py_exec("active", "result = object()")

    assert isinstance(result["result"], str)
    assert result["warnings"]


def test_diff_snapshots_marks_name_only_changes(monkeypatch):
    bridge = _load_bridge(monkeypatch)
    instance = bridge.BinaryNinjaBridge()

    diffs = instance._diff_snapshots(
        {
            0x401000: {
                "name": "sub_401000",
                "address": "0x401000",
                "text": "return 7;",
            }
        },
        {
            0x401000: {
                "name": "player_update",
                "address": "0x401000",
                "text": "return 7;",
            }
        },
    )

    assert len(diffs) == 1
    assert diffs[0]["changed"] is True
    assert diffs[0]["before_name"] == "sub_401000"
    assert diffs[0]["after_name"] == "player_update"
    assert diffs[0]["diff"] == "--- before:sub_401000\n+++ after:player_update"
    assert "before_excerpt" not in diffs[0]


def test_read_write_lock_blocks_reader_until_writer_releases(monkeypatch):
    bridge = _load_bridge(monkeypatch)
    lock = bridge._ReadWriteLock()
    writer_ready = threading.Event()
    writer_release = threading.Event()
    reader_entered = threading.Event()

    def writer():
        with lock.write():
            writer_ready.set()
            writer_release.wait(1)

    def reader():
        writer_ready.wait(1)
        with lock.read():
            reader_entered.set()

    writer_thread = threading.Thread(target=writer)
    reader_thread = threading.Thread(target=reader)
    writer_thread.start()
    reader_thread.start()

    assert writer_ready.wait(1)
    time.sleep(0.05)
    assert not reader_entered.is_set()

    writer_release.set()
    reader_thread.join(1)
    writer_thread.join(1)

    assert reader_entered.is_set()


def test_read_write_lock_allows_parallel_readers(monkeypatch):
    bridge = _load_bridge(monkeypatch)
    lock = bridge._ReadWriteLock()
    entered: list[str] = []
    both_entered = threading.Event()
    release = threading.Event()

    def reader(name: str):
        with lock.read():
            entered.append(name)
            if len(entered) == 2:
                both_entered.set()
            release.wait(1)

    first = threading.Thread(target=reader, args=("first",))
    second = threading.Thread(target=reader, args=("second",))
    first.start()
    second.start()

    assert both_entered.wait(1)

    release.set()
    first.join(1)
    second.join(1)

    assert sorted(entered) == ["first", "second"]


def test_collect_open_views_uses_tabs_api(monkeypatch):
    bridge = _load_bridge(monkeypatch)

    class _View:
        def __init__(self, data):
            self._data = data

        def getData(self):
            return self._data

    class _Frame:
        def __init__(self, data):
            self._data = data

        def getCurrentBinaryView(self):
            return self._data

        def getCurrentView(self):
            return _View(self._data)

    view_a = object()
    view_b = object()
    view_c = object()

    class _Context:
        def getCurrentViewFrame(self):
            return _Frame(view_c)

        def getTabs(self):
            return ["tab-a", "tab-b", "tab-c"]

        def getViewFrameForTab(self, tab):
            mapping = {
                "tab-a": _Frame(view_a),
                "tab-b": _Frame(view_b),
                "tab-c": _Frame(view_c),
            }
            return mapping[tab]

        def getViewForTab(self, tab):
            mapping = {
                "tab-a": _View(view_a),
                "tab-b": _View(view_b),
                "tab-c": _View(view_c),
            }
            return mapping[tab]

    fake_ui = types.SimpleNamespace(
        UIContext=types.SimpleNamespace(
            allContexts=lambda: [_Context()],
            activeContext=lambda: None,
        )
    )
    monkeypatch.setattr(bridge, "ui", fake_ui)

    views = bridge._collect_open_views()

    assert len(views) == 3
    assert set(id(view) for view in views) == {id(view_a), id(view_b), id(view_c)}
