"""OneLake filesystem access.

Small on purpose: the only thing here that is not a path string is the ADLS client, and the only
thing interesting about that is what it is NOT.

THE StaticToken SHIM IS GONE. Cell 9 wrapped the Fabric bearer token like this:

    class StaticToken:
        def __init__(self, tok): self._tok = tok
        def get_token(self, *scopes, **kw):
            return AccessToken(self._tok, int(time.time()) + 3600)

That tells the Azure SDK "this token is good for an hour", every single time it is asked --
including an hour and a half in. The SDK believes it, never refreshes, and the upload gets a 403
that looks exactly like a permissions problem. Handing the SDK a real credential costs nothing
and makes the failure impossible.
"""

from __future__ import annotations

from bench import auth
from bench.config import ONELAKE_DFS, Config


def service_client():
    """A DataLakeServiceClient on OneLake, backed by the refreshing credential."""
    from azure.storage.filedatalake import DataLakeServiceClient

    return DataLakeServiceClient(account_url=f"https://{ONELAKE_DFS}", credential=auth.credential())


def file_system(cfg: Config):
    """The workspace-scoped filesystem. Paths inside it start with the lakehouse id."""
    return service_client().get_file_system_client(cfg.workspace_id)


def table_root(cfg: Config, table: str) -> str:
    """abfss:// location of one Iceberg table, as the catalog records it."""
    return f"{cfg.base_path}/Tables/{cfg.schema}/{table}"


def relative(cfg: Config, table: str, filename: str) -> str:
    """Path of one data file relative to the filesystem client (i.e. below the workspace)."""
    return f"{cfg.lakehouse_id}/Tables/{cfg.schema}/{table}/{filename}"
