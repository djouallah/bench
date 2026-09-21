"""Redact live bearer tokens from anything that might be written down.

WHY THIS IS A MODULE AND NOT AN INLINE str.replace. The OneLake token is embedded in SQL TEXT --
DuckDB's `ATTACH ... TOKEN '<tok>'`, chDB's `CREATE DATABASE ... onelake_bearer_token='<tok>'` --
and in an ENV VAR (`SAIL_CATALOG__LIST`). Engines quote the failing statement back at you in
exception messages. One unhandled attach failure prints an Entra token into a world-readable
Actions log, and from there into `results/*.json`, which is committed to git forever.

`::add-mask::` in the workflow is NOT sufficient on its own: it only masks verbatim contiguous
appearances. A token that a traceback line-wraps, elides with "..." or truncates slips straight
past it. This module catches the pieces too -- see `_fragments`.

Nothing here is a substitute for not logging the token in the first place. It is the backstop for
the exception paths nobody thought about.
"""

from __future__ import annotations

import re

MASK = "***REDACTED***"

# A JWT chunk worth masking on its own. 24 is short enough to catch a truncated token tail and
# long enough that it never fires on ordinary base64-ish text like a git sha or a file hash.
_MIN_FRAGMENT = 24

_registry: list[str] = []


def register(secret: str | None) -> None:
    """Mark a string as secret. Idempotent; None and short strings are ignored."""
    if secret and len(secret) >= _MIN_FRAGMENT and secret not in _registry:
        _registry.append(secret)


def forget_all() -> None:
    """Drop every registered secret. For tests."""
    _registry.clear()


def _fragments(secret: str) -> list[str]:
    """Substrings of `secret` that must also be masked.

    A JWT is `header.payload.signature`. A traceback that wraps or truncates the token leaves one
    of those parts intact and on its own -- enough to matter, and invisible to a whole-string
    replace. Each part is masked individually when it is long enough to be unambiguous.
    """
    parts = [p for p in secret.split(".") if len(p) >= _MIN_FRAGMENT]
    # Longest first, so masking a part never leaves a shorter part's prefix behind.
    return sorted(parts, key=len, reverse=True)


def scrub(value: object) -> str:
    """Return `value` as text with every registered secret (and its parts) masked."""
    text = value if isinstance(value, str) else str(value)
    for secret in sorted(_registry, key=len, reverse=True):
        text = text.replace(secret, MASK)
        for fragment in _fragments(secret):
            text = text.replace(fragment, MASK)
    return text


def scrub_exc(exc: BaseException, limit: int = 2000) -> str:
    """A scrubbed, length-capped one-string rendering of an exception, safe to commit to git.

    Deliberately NOT the full traceback: the frames carry local variables in some formatters, the
    attach SQL among them. Type and message are what a benchmark result needs.
    """
    text = scrub(f"{type(exc).__name__}: {exc}")
    return text if len(text) <= limit else text[: limit - 1] + "…"


def safe_print(*args: object) -> None:
    """print(), scrubbed. Use this everywhere in engine and runner code."""
    print(" ".join(scrub(a) for a in args), flush=True)


_TOKEN_SHAPED = re.compile(r"\beyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\b")


def find_token_shaped(text: str) -> list[str]:
    """Any JWT-shaped string in `text`, registered or not.

    The last line of defence, used by the leak check over logs and results before they are
    published: it catches a token this process never saw -- one minted by a subprocess, or by an
    engine that refreshed on its own.
    """
    return _TOKEN_SHAPED.findall(text)
