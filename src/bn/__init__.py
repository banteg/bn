from __future__ import annotations

from .version import VERSION


def main(argv: list[str] | None = None) -> int:
    from .cli import main as cli_main

    return cli_main(argv)


__all__ = ["main", "VERSION"]
