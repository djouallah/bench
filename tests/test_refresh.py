"""Credential refresh: a fresh token really is fresh, and Gluten restarts only when it must."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from bench import auth
from bench.config import TOKEN_MIN_LIFETIME_SECONDS
from bench.tpch.engines import pyspark_gluten_iceberg as gluten


@pytest.fixture(autouse=True)
def _clean_auth():
    auth.reset()
    yield
    auth.reset()


def _fake_credential(monkeypatch, expires_on):
    """Patch `auth.credential`; record whether the process-wide credential was dropped first."""
    seen = []

    def credential():
        seen.append(auth._credential)
        return SimpleNamespace(
            get_token=lambda scope: SimpleNamespace(token=f"tok{len(seen)}", expires_on=expires_on)
        )

    monkeypatch.setattr(auth, "credential", credential)
    monkeypatch.setattr(auth.scrub, "register", lambda secret: None)
    return seen


def test_a_cached_token_is_reused_until_it_runs_low(monkeypatch):
    _fake_credential(monkeypatch, expires_on=10_000)
    monkeypatch.setattr(auth.time, "time", lambda: 0)
    assert auth.onelake_token() == "tok1"
    assert auth.onelake_token() == "tok1"
    assert auth.token_expires_on() == 10_000


def test_fresh_drops_both_caches(monkeypatch):
    """azure-identity hands the same token back until 5 min out; a new credential does not."""
    seen = _fake_credential(monkeypatch, expires_on=10_000)
    monkeypatch.setattr(auth.time, "time", lambda: 0)
    auth.onelake_token()
    auth._credential = object()  # what credential() would have cached
    assert auth.onelake_token(fresh=True) == "tok2"
    assert seen[-1] is None


def test_token_expiry_is_infinite_before_any_mint():
    assert auth.token_expires_on() == float("inf")


def _gluten(monkeypatch, now, expires):
    engine = gluten.PysparkGlutenIceberg(cfg=None)
    engine._expires = expires
    events = []
    monkeypatch.setattr(gluten.time, "time", lambda: now)
    monkeypatch.setattr(engine, "close", lambda: events.append("close"))
    monkeypatch.setattr(gluten, "_shutdown_gateway", lambda: events.append("gateway"))
    monkeypatch.setattr(
        gluten.auth, "onelake_token", lambda **kw: events.append(("token", kw.get("fresh")))
    )

    def setup():
        events.append("setup")
        engine._expires = now + 3300

    monkeypatch.setattr(engine, "setup", setup)
    return engine, events


def test_gluten_leaves_a_session_with_time_left_alone(monkeypatch):
    engine, events = _gluten(monkeypatch, now=0, expires=TOKEN_MIN_LIFETIME_SECONDS + 60)
    engine.refresh()
    assert events == []


def test_gluten_restarts_on_fresh_credentials_under_the_margin(monkeypatch):
    engine, events = _gluten(monkeypatch, now=0, expires=TOKEN_MIN_LIFETIME_SECONDS - 60)
    engine.refresh()
    assert events == ["close", "gateway", ("token", True), "setup"]
    assert engine._expires == 3300
    engine.refresh()  # the new session has 55 minutes: no second restart
    assert events.count("setup") == 1
