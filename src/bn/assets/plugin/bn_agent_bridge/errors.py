from __future__ import annotations

from typing import Any


class OperationFailure(RuntimeError):
    def __init__(
        self,
        status: str,
        message: str,
        *,
        requested: dict[str, Any] | None = None,
        observed: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.status = status
        self.message = message
        self.requested = requested or {}
        self.observed = observed or {}


def error_details(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, OperationFailure):
        return {
            "code": exc.status,
            "message": exc.message,
            "requested": exc.requested,
            "observed": exc.observed,
        }
    return {"code": type(exc).__name__, "message": str(exc)}
