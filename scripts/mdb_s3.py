"""Export MDB data from Neo4j into S3."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

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


@task(name="Export MDB to S3", cache_policy=NO_CACHE)
def export_mdb_to_s3(mdb: MDB, s3_url: str) -> None:
    """Export MDB to graphml file in S3."""
    logger = get_run_logger()
    logger.info("Exporting MDB to S3: %s", s3_url)
    apoc_export_stmt = (
        f"CALL apoc.export.graphml.all('{s3_url}', "
        "{useTypes: true, batchSize: 10000}) "
        "YIELD nodes, relationships, properties "
        "RETURN nodes, relationships, properties"
    )
    result = mdb.get_with_statement(apoc_export_stmt)

    if result:
        logger.info("Export result: %s", result)
        return
    export_fail_msg = "Export failed - no results returned"
    raise RuntimeError(export_fail_msg)


@task(name="Clear MDB Database", cache_policy=NO_CACHE)
def clear_mdb_database(mdb: WriteableMDB) -> None:
    """Clear all existing nodes and relationships from the database."""
    logger = get_run_logger()
    logger.info("Clearing all nodes and relationships from MDB")

    clear_stmt = (
        "CALL apoc.periodic.iterate("
        '"MATCH (n) RETURN id(n) as id", '
        '"MATCH (n) WHERE id(n) = $id DETACH DELETE n", '
        "{batchSize: 10000, parallel: false}) "
        "YIELD batches, total "
        "RETURN batches, total"
    )

    logger.info("Starting database clear operation")
    result = mdb.put_with_statement(clear_stmt)
    if result:
        logger.info("Clear database result: %s", result)
    else:
        clear_fail_msg = "Clear database failed - no results returned"
        raise RuntimeError(clear_fail_msg)

    count_stmt = "MATCH (n) RETURN count(n) as node_count"
    count_result = mdb.get_with_statement(count_stmt)
    logger.info("Count result: %s", count_result)

    if count_result and len(count_result) > 0:
        first_result = count_result[0]
        node_count = (
            first_result.get("node_count", 0) if isinstance(first_result, dict) else 0
        )
        if node_count > 0:
            logger.warning(
                "Database still contains %d nodes after clear operation",
                node_count,
            )

            sample_stmt = (
                "MATCH (n) RETURN labels(n) as labels, keys(n) as properties LIMIT 5"
            )
            sample_result = mdb.get_with_statement(sample_stmt)
            logger.info("Sample remaining nodes: %s", sample_result)

            error_msg = f"Clear operation incomplete: {node_count} nodes remaining"
            raise RuntimeError(error_msg)
        logger.info("Database successfully cleared - 0 nodes remaining")


@task(name="Import MDB from S3", cache_policy=NO_CACHE)
def import_mdb_from_s3(
    mdb: WriteableMDB,
    s3_url: str,
    *,
    clear_db: bool = False,
) -> None:
    """Import MDB from graphml file in S3."""
    logger = get_run_logger()

    if clear_db:
        clear_mdb_database(mdb)

    logger.info("Importing MDB from S3: %s", s3_url)
    apoc_import_stmt = (
        f"CALL apoc.import.graphml('{s3_url}', "
        "{readLabels: true, batchSize: 10000}) "
        "YIELD nodes, relationships, properties "
        "RETURN nodes, relationships, properties"
    )
    logger.info("Importing GraphML with fresh node ID assignment")
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
def mdb_import_flow(  # noqa: PLR0913
    mdb_id: str,
    mdb_uri: str,
    mdb_user: str,
    key: str,
    bucket: str,
    endpoint: str = DEFAULT_S3_ENDPOINT,
    *,
    clear_db: bool = False,
) -> None:
    """Import MDB data from S3 into Neo4j."""
    mdb = init_mdb_connection(
        mdb_id,
        mdb_uri,
        mdb_user,
        writeable=True,
        allow_empty=True,
    )
    s3_url = build_s3_url(bucket, key, endpoint)
    import_mdb_from_s3(mdb=mdb, s3_url=s3_url, clear_db=clear_db)


@flow(name="mdb-clear-database")
def mdb_clear_flow(
    mdb_id: str,
    mdb_uri: str,
    mdb_user: str,
) -> None:
    """Clear all nodes and relationships from MDB."""
    mdb = init_mdb_connection(
        mdb_id,
        mdb_uri,
        mdb_user,
        writeable=True,
        allow_empty=True,
    )
    clear_mdb_database(mdb)
