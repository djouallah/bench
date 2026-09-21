"""The token must not survive into anything that gets written down.

These results are committed to a PUBLIC repo and git history is forever, so a leaked bearer token
is a credential rotation at best. `::add-mask::` in the workflow only catches verbatim contiguous
appearances; these tests pin the cases it misses.
"""

from __future__ import annotations

import json
import traceback

import pytest

from bench import scrub

# Shaped like a real Entra JWT: three dot-separated base64url segments.
TOKEN = (
    "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsIng1dCI6ImFiY2RlZmdoaWprbG1ub3AifQ"
    ".eyJhdWQiOiJodHRwczovL3N0b3JhZ2UuYXp1cmUuY29tLyIsImV4cCI6MTc5MDAwMDAwMH0"
    ".SIGNATUREsignatureSIGNATUREsignatureSIGNATUREsignature"
)


@pytest.fixture(autouse=True)
def _registered():
    scrub.forget_all()
    scrub.register(TOKEN)
    yield
    scrub.forget_all()


def test_whole_token_is_masked():
    assert TOKEN not in scrub.scrub(f"ATTACH 'x' (TOKEN '{TOKEN}')")


def test_a_wrapped_fragment_is_masked():
    """The case ::add-mask:: cannot catch.

    A traceback that wraps or truncates the token leaves one JWT segment alone on a line. The
    workflow's mask never fires on it; this does.
    """
    signature = TOKEN.split(".")[2]
    assert signature not in scrub.scrub(f"...failed near {signature} in statement")


def test_token_does_not_survive_a_formatted_traceback():
    try:
        raise RuntimeError(f"CREATE DATABASE ... onelake_bearer_token='{TOKEN}'")
    except RuntimeError as exc:
        rendered = scrub.scrub("".join(traceback.format_exception(exc)))
    assert TOKEN not in rendered
    assert scrub.MASK in rendered


def test_token_does_not_survive_a_json_dump():
    payload = {"error": f"attach failed: bearer {TOKEN}", "nested": {"env": [TOKEN]}}
    assert TOKEN not in scrub.scrub(json.dumps(payload))


def test_scrub_exc_is_capped_and_masked():
    exc = RuntimeError(f"{TOKEN} " * 200)
    rendered = scrub.scrub_exc(exc, limit=500)
    assert len(rendered) <= 500
    assert TOKEN not in rendered


def test_short_strings_are_never_registered():
    """A 12-char secret would mask ordinary words all over the log."""
    scrub.register("hunter2")
    assert scrub.scrub("hunter2") == "hunter2"


def test_find_token_shaped_catches_an_unregistered_token():
    """The last line of defence: a token this process never saw.

    A subprocess or an engine that refreshed on its own would not be in the registry, so
    publish.py scans the output for JWT SHAPE rather than for known values.
    """
    scrub.forget_all()
    assert scrub.find_token_shaped(f"leaked: {TOKEN}") == [TOKEN]
    assert scrub.find_token_shaped("no tokens here, just a sha: abc1234def5678") == []
