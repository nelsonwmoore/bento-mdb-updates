"""Test Prefect flow."""

from __future__ import annotations

from bento_meta.mdb.mdb import MDB
from bento_meta.mdb.writeable import WriteableMDB
from prefect import flow, task
from prefect.blocks.system import Secret


@task(log_prints=True)
def connect_to_mdb(
    uri: str,
    user: str = "neo4j",
    mode: str = "r",
) -> MDB | WriteableMDB:
    """
    Connect to the MDB.

    Will read from the MDB if mode is 'r', and write to the MDB if mode is 'w'.
    """
    neo4j_password = Secret.load("fnl-mdb-dev-pwd").get()
    if mode == "r":
        return MDB(uri=uri, user=user, password=neo4j_password)
    if mode == "w":
        return WriteableMDB(uri=uri, user=user, password=neo4j_password)
    msg = f"Invalid mode {mode}"
    raise ValueError(msg)


@task(log_prints=True)
def execute_update(
    mdb: MDB | WriteableMDB,
    qry: str,
    parms: dict[str, str] = {},
    mode: str = "r",
) -> None:
    """Execute the update."""
    if mode == "r":
        mdb.get_with_statement(qry, parms)
    if mode == "w":
        mdb.put_with_statement(qry, parms)  # type: ignore reportAttributeAccessIssue


@flow(log_prints=True)
def test_update_neo4j_flow(
    uri: str,
    user: str,
    mode: str,
    qry: str,
    parms: dict[str, str] = {},
) -> None:
    """Update the Neo4j database."""
    mdb = connect_to_mdb(uri, user, mode)

    return execute_update(mdb, qry, parms, mode)
