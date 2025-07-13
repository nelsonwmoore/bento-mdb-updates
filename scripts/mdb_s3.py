"""Export MDB data from Neo4j into S3."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from prefect import flow, get_run_logger, task
from zoneinfo import ZoneInfo

from bento_mdb_updates.mdb_utils import init_mdb_connection

if TYPE_CHECKING:
    from bento_meta.mdb import MDB
    from bento_meta.mdb.writeable import WriteableMDB

DEFAULT_S3_ENDPOINT = "s3.us-east-1.amazonaws.com"


def get_current_date() -> str:
    """Get current date in YYYYMMDD format."""
    return datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")


@task(name="Build S3 URL")
def build_s3_url(bucket: str, key: str, endpoint: str = DEFAULT_S3_ENDPOINT) -> str:
    """Build S3 URL from bucket and key."""
    logger = get_run_logger()

    url = f"s3://{endpoint}/{bucket}/{key}"
    logger.info("S3 URL: %s", url)

    return url


@task(name="Export MDB to S3")
def export_mdb_to_s3(mdb: MDB, s3_url: str) -> None:
    """Export MDB to graphml file in S3."""
    logger = get_run_logger()
    logger.info("Exporting MDB to S3: %s", s3_url)
    apoc_export_stmt = (
        f"CALL apoc.export.graphml.all('{s3_url}', "
        "{{useTypes: true, storeNodeIds: true, batchSize: 10000}}) "
        "YIELD nodes, relationships, properties "
        "RETURN nodes, relationships, properties"
    )
    result = mdb.get_with_statement(apoc_export_stmt)

    if result:
        logger.info("Export result: %s", result)
        return
    export_fail_msg = "Export failed - no results returned"
    raise RuntimeError(export_fail_msg)


@task(name="Import MDB from S3")
def import_mdb_from_s3(mdb: WriteableMDB, s3_url: str) -> None:
    """Import MDB from graphml file in S3."""
    logger = get_run_logger()
    logger.info("Importing MDB from S3: %s", s3_url)
    apoc_import_stmt = (
        f"CALL apoc.import.graphml('{s3_url}', "
        "{{storeNodeIds: true, readLabels: true, batchSize: 10000}}) "
        "YIELD nodes, relationships, properties "
        "RETURN nodes, relationships, properties"
    )
    result = mdb.put_with_statement(apoc_import_stmt)
    if result:
        logger.info("Import result: %s", result)
        return
    import_fail_msg = "Import failed - no results returned"
    raise RuntimeError(import_fail_msg)


@flow(name="mdb-export-s3")
def mdb_export_flow(
    mdb_id: str,
    mdb_uri: str,
    mdb_user: str,
    bucket: str,
    endpoint: str = DEFAULT_S3_ENDPOINT,
) -> str:
    """
    Export MDB data from Neo4j into S3.

    Returns S3 URL for chaining operations.
    """
    mdb = init_mdb_connection(mdb_id, mdb_uri, mdb_user)
    s3_key = f"{mdb_id}_{get_current_date()}.graphml"
    s3_url = build_s3_url(bucket, s3_key, endpoint)
    export_mdb_to_s3(mdb=mdb, s3_url=s3_url)
    return s3_url


@flow(name="mdb-import-s3")
def mdb_import_flow(
    mdb_id: str,
    mdb_uri: str,
    mdb_user: str,
    key: str,
    bucket: str,
    endpoint: str = DEFAULT_S3_ENDPOINT,
) -> None:
    """Import MDB data from S3 into Neo4j."""
    mdb = init_mdb_connection(mdb_id, mdb_uri, mdb_user, writeable=True)
    s3_url = build_s3_url(bucket, key, endpoint)
    import_mdb_from_s3(mdb=mdb, s3_url=s3_url)
