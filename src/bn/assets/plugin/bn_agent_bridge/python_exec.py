from __future__ import annotations

import contextlib
import io
import struct
import traceback
from typing import Any

from .errors import OperationFailure


class _PythonHelpers:
    """Stable convenience helpers layered over the complete Binary Ninja API."""

    def __init__(self, bridge: Any, bv: Any):
        self.bridge = bridge
        self.bv = bv

    @property
    def byte_order(self) -> str:
        return "little" if "little" in str(getattr(self.bv, "endianness", "little")).lower() else "big"

    def address(self, identifier: Any) -> int:
        return self.bridge._resolve_address(self.bv, identifier)

    def function(self, identifier: Any, *, containing: bool = True) -> Any:
        return self.bridge._find_function(self.bv, identifier, allow_containing=containing)

    def functions_containing(self, address: Any) -> list[Any]:
        return self.bridge._functions_containing(self.bv, self.address(address))

    def read(self, address: Any, size: int) -> bytes:
        resolved = self.address(address)
        data = bytes(self.bv.read(resolved, int(size)))
        if len(data) != int(size):
            raise RuntimeError(f"Could not read {size} bytes at {hex(resolved)}")
        return data

    def _read_int(self, address: Any, size: int, *, signed: bool = False) -> int:
        return int.from_bytes(self.read(address, size), byteorder=self.byte_order, signed=signed)

    def read_u8(self, address: Any) -> int:
        return self._read_int(address, 1)

    def read_u16(self, address: Any) -> int:
        return self._read_int(address, 2)

    def read_u32(self, address: Any) -> int:
        return self._read_int(address, 4)

    def read_u64(self, address: Any) -> int:
        return self._read_int(address, 8)

    def read_i8(self, address: Any) -> int:
        return self._read_int(address, 1, signed=True)

    def read_i16(self, address: Any) -> int:
        return self._read_int(address, 2, signed=True)

    def read_i32(self, address: Any) -> int:
        return self._read_int(address, 4, signed=True)

    def read_i64(self, address: Any) -> int:
        return self._read_int(address, 8, signed=True)

    def read_ptr(self, address: Any) -> int:
        return self._read_int(address, int(getattr(self.bv, "address_size", 8)))

    def read_f32(self, address: Any) -> float:
        prefix = "<" if self.byte_order == "little" else ">"
        return float(struct.unpack(prefix + "f", self.read(address, 4))[0])

    def read_f64(self, address: Any) -> float:
        prefix = "<" if self.byte_order == "little" else ">"
        return float(struct.unpack(prefix + "d", self.read(address, 8))[0])

    def read_cstr(self, address: Any, max_length: int = 4096) -> str:
        resolved = self.address(address)
        data = bytes(self.bv.read(resolved, int(max_length)))
        return data.split(b"\0", 1)[0].decode("utf-8", errors="replace")


def _normalize_result(value: Any) -> tuple[Any, list[str]]:
    def normalize(item: Any) -> Any:
        if item is None or isinstance(item, (bool, int, float, str)):
            return item
        if isinstance(item, (list, tuple)):
            return [normalize(part) for part in item]
        if isinstance(item, dict):
            return {str(key): normalize(val) for key, val in item.items()}
        raise TypeError(type(item).__name__)

    try:
        return normalize(value), []
    except TypeError:
        return repr(value), ["`result` was not JSON-serializable; returned repr(result) instead."]


def execute_python(bridge: Any, bv: Any, script: str, bn_module: Any) -> dict[str, Any]:
    stdout = io.StringIO()
    helpers = _PythonHelpers(bridge, bv)
    scope = {
        "bn": bn_module,
        "binaryninja": bn_module,
        "bv": bv,
        "current_view": bv,
        "address": helpers.address,
        "function": helpers.function,
        "functions_containing": helpers.functions_containing,
        "read_u8": helpers.read_u8,
        "read_u16": helpers.read_u16,
        "read_u32": helpers.read_u32,
        "read_u64": helpers.read_u64,
        "read_i8": helpers.read_i8,
        "read_i16": helpers.read_i16,
        "read_i32": helpers.read_i32,
        "read_i64": helpers.read_i64,
        "read_ptr": helpers.read_ptr,
        "read_f32": helpers.read_f32,
        "read_f64": helpers.read_f64,
        "read_cstr": helpers.read_cstr,
        "result": None,
    }
    try:
        with contextlib.redirect_stdout(stdout):
            exec(script, scope, scope)
    except Exception as exc:
        raise OperationFailure(
            "python_error",
            f"Python execution failed: {type(exc).__name__}: {exc}",
            observed={
                "stdout": stdout.getvalue(),
                "traceback": traceback.format_exc(),
            },
        ) from exc

    result, warnings = _normalize_result(scope.get("result"))
    return {
        "stdout": stdout.getvalue(),
        "result": result,
        "warnings": warnings,
    }
