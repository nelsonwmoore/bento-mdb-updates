"""MDB helper functions."""

from __future__ import annotations

from bento_meta.mdb import MDB
from bento_meta.mdb.writeable import WriteableMDB
from prefect.blocks.system import Secret

from bento_mdb_updates.constants import VALID_MDB_IDS


def init_mdb_connection(
    mdb_id: str,
    uri: str,
    user: str,
    *,
    writeable: bool = False,
    allow_empty: bool = False,
) -> MDB | WriteableMDB:
    """Initialize MDB connection."""
    if mdb_id not in VALID_MDB_IDS:
        msg = f"Invalid MDB ID: {mdb_id}. Valid IDs: {VALID_MDB_IDS}"
        raise ValueError(msg)
    pwd_secret_name = mdb_id + "-pwd"
    password = Secret.load(pwd_secret_name).get()  # type: ignore reportAttributeAccessIssue
    if mdb_id.startswith("og-mdb"):
        password = ""
    if uri.startswith("jdbc:neo4j:"):
        uri = uri.replace("jdbc:neo4j:", "")
    if writeable:
        mdb = WriteableMDB(
            uri=uri,
            user=user,
            password=password,
        )
    else:
        mdb = MDB(
            uri=uri,
            user=user,
            password=password,
        )
    verify_mdb_connection(mdb, allow_empty=allow_empty)
    return mdb


def verify_mdb_connection(mdb: MDB, *, allow_empty: bool = False) -> None:
    """
    Validate that MDB connection is working and has models.

    Raises:
        ConnectionError: if connection fails.
        RuntimeError: if MDB empty when it shouldn't be.
    """
    if mdb.driver is None:
        msg = f"Failed to connect to MDB: {mdb.uri}"
        raise ConnectionError(msg)
    if (
        not allow_empty
        or not hasattr(mdb, "models")
        or mdb.models is None
        or len(mdb.models) == 0
    ):
        msg = f"No model information could be retrieved from MDB: {mdb.uri}"
        raise RuntimeError(msg)
    print(f"MDB connection validated: {len(mdb.models)} models found in database")
