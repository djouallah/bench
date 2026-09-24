"""OneLake credentials from GitHub OIDC.

REPLACES cell 6, which was one line:

    token = notebookutils.credentials.getToken('storage')

`notebookutils` exists only inside a Fabric notebook, so this is the seam the whole port turns on.

WHY NOT AzureCliCredential, despite `azure/login` running in the workflow. `azure/login@v3`
performs `az login --federated-token <the GitHub OIDC assertion>`. That is a CLIENT-ASSERTION
flow, and client-assertion flows return NO REFRESH TOKEN. The assertion itself expires in minutes.
So once the first access token ages out (~60-90 min; Entra randomizes), the `az` session has
nothing left to renew with and every later `get-access-token` fails. AzureCliCredential LOOKS
auto-refreshing and is not, beyond the first token's lifetime.

ClientAssertionCredential over the GitHub OIDC endpoint has no such ceiling: it mints a FRESH
assertion on every refresh, straight from the Actions runtime. It needs no new trust
configuration -- same app registration, same federated credential, same
`api://AzureADTokenExchange` audience that `azure/login` itself uses -- and it removes the Azure
CLI from the Python process, so `bench/` behaves identically on a laptop.

WHAT THIS STILL CANNOT FIX, and why etl.yml sets `timeout-minutes: 50`. DuckDB bakes the token
into `ATTACH`, chDB into `CREATE DATABASE`, LakeSail into an env var read once at server start.
Those three capture a STRING and never ask again. Refreshing the credential does not reach inside
them, so an engine session is hard-bounded by the lifetime of the token it was handed. Mint late
(right before engine setup), never at job start.
"""

from __future__ import annotations

import os
import time
import urllib.request
from typing import TYPE_CHECKING

from bench import scrub
from bench.config import ICEBERG_ENDPOINT, ONELAKE_BLOB, STORAGE_SCOPE

if TYPE_CHECKING:  # pragma: no cover
    from azure.core.credentials import TokenCredential

    from bench.config import Config

# The audience Entra requires for a GitHub federated credential. Not configurable: it is what the
# federated credential on the app registration is created with.
OIDC_AUDIENCE = "api://AzureADTokenExchange"

_cached: tuple[str, float] | None = None
_credential: TokenCredential | None = None


def _github_oidc_assertion() -> str:
    """A fresh GitHub OIDC assertion for the Entra token exchange.

    Called by ClientAssertionCredential on EVERY token acquisition, including refreshes -- which
    is the entire reason this is a function and not a captured string.
    """
    url = os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"]
    request = urllib.request.Request(
        f"{url}&audience={OIDC_AUDIENCE}",
        headers={"Authorization": f"Bearer {os.environ['ACTIONS_ID_TOKEN_REQUEST_TOKEN']}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        import json

        return json.load(response)["value"]


def credential() -> TokenCredential:
    """The process-wide credential: OIDC in Actions, `az login` on a laptop."""
    global _credential
    if _credential is None:
        if os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL"):
            from azure.identity import ClientAssertionCredential

            _credential = ClientAssertionCredential(
                tenant_id=os.environ["AZURE_TENANT_ID"],
                client_id=os.environ["AZURE_CLIENT_ID"],
                func=_github_oidc_assertion,
            )
        else:
            from azure.identity import AzureCliCredential

            _credential = AzureCliCredential()
    return _credential


def onelake_token(skew: int = 300) -> str:
    """A bearer for https://storage.azure.com/, re-minted when under `skew` seconds remain.

    Every token this returns is registered with `scrub` before it leaves the function, so any
    later traceback that quotes it is masked. That ordering is deliberate -- register first,
    return second.
    """
    global _cached
    now = time.time()
    if _cached is None or _cached[1] - now < skew:
        token = credential().get_token(STORAGE_SCOPE)
        scrub.register(token.token)
        _cached = (token.token, float(token.expires_on))
    return _cached[0]


def catalog(cfg: Config):
    """A pyiceberg RestCatalog on the Fabric OneLake Iceberg endpoint.

    Used by `prepare` (table creation and add_files) and by the Polars engine. The other three
    engines attach the catalog natively and never import pyiceberg.

    `token` signs the catalog's REST calls; `adls.token` is what FileIO uses to read and write the
    blobs themselves. They are separate properties and both are required -- pyiceberg does not
    derive one from the other.
    """
    from pyiceberg.catalog import load_catalog

    token = onelake_token()
    return load_catalog(
        "onelake",
        **{
            "uri": ICEBERG_ENDPOINT,
            "token": token,
            "warehouse": cfg.warehouse,
            "adls.account-name": "onelake",
            "adls.account-host": ONELAKE_BLOB,
            "adls.token": token,
        },
    )


def reset() -> None:
    """Drop the cached credential and token. For tests."""
    global _cached, _credential
    _cached = None
    _credential = None
