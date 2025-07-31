"""Run arbitrary Cypher queries on MDB"""

from __future__ import annotations
import logging
from typing import Optional
from pathlib import Path
from bento_meta.mdb import MDB
from bento_mdb_updates.mdb_utils import init_mdb_connection

import click
from prefect import flow, get_run_logger, task
from prefect.cache_policies import INPUTS

no_cache_mdb = INPUTS - 'mdb'

@task
def create_connection(
        mdb_id: str,
        mdb_uri: str,
        mdb_user: str,
        writeable: bool = True,
        allow_empty: bool = True,
) -> MDB:
    logger = get_run_logger()
    logger.setLevel(logging.INFO)
    mdb = None
    try:
        mdb = init_mdb_connection(mdb_id, mdb_uri, mdb_user,
                                  writeable=writeable, allow_empty=allow_empty)
    except Exception as e:
        logger.error(f"Error on MDB connection attempt: {e}")
        raise
    return mdb
    

@task(cache_policy=no_cache_mdb)
def execute_cypher(  # noqa: C901, PLR0912
    mdb: MDB,
    query: str,
    params: dict,
    is_write: bool = False,
) -> None:
    """Run Cypher on MDB."""
    logger = get_run_logger()
    logger.setLevel(logging.INFO)
    qfn = None
    result = None
    logger.info(f"Run query '{query}'...")
    if (is_write):
        qfn = mdb.put_with_statement
    else:
        qfn = mdb.get_with_statement
    try:
        result = qfn(query, params)
        logger.info("...completed")
    except Exception as e:
        logger.error(f"Error in MDB query run: {e}")
        raise
    return result


@flow(name="run-cypher", log_prints=True)
def run_cypher_flow(  # noqa: PLR0913
    mdb_id: str,
    query: list,
    params: dict = {},
    mdb_uri: Optional[str] = None,
    mdb_user: Optional[str] = None,
) -> None:
    """Run arbitrary Cypher queries on MDB."""
    logger = get_run_logger()
    logger.setLevel(logging.INFO)
    logger.info(f"Running query:\n{query}")
    results = []
    try:
        mdb = create_connection(mdb_id, mdb_uri, mdb_user, True, False)
    except Exception as e:
        raise e
    if (params):
        logger.info(f" with params:\n{params}")

    if (len(query) == 1):
        qpath = Path(query[0])
        if qpath.exists():
            query = [ll for ll in qpath.open()]
            
    for q in query:
        try:
            result = execute_cypher(mdb, q, params)
            results.append(result)
        except Exception as e:
            logger.error(f"Query '{q}' failed with exception: {e}")
    logger.info("Queries finished.")
    logger.info(results)


@click.command()
@click.option(
    "--mdb_uri",
    required=False,
    type=str,
    prompt=True,
    help="metamodel database URI",
)
@click.option(
    "--mdb_user",
    required=False,
    type=str,
    prompt=True,
    help="metamodel database username",
)
@click.option("--mdb_id", type=str, help="MDB ID", prompt=True, required=True)
@click.option("--param", nargs=2, type=str, multiple=True)
@click.option("--log_level", type=str, help="Log level", prompt=True, required=True)
@click.argument('query', type=str, required=True)
def main(mdb_id: str,
         mdb_uri: str,
         mdb_user: str,
         query: str,
         param: list = {},
         ):

    if param:
        params = {p[0]: p[1] for p in param}
    qpath = Path(query)
    if qpath.exists():
        query = [ll for ll in qpath.open()]
        qpath.close()
    else:
        query = [query]
    run_cypher_flow(mdb_id=mdb_id,
                    mdb_uri=mdb_uri, 
                    mdb_user=mdb_user, 
                    query=query,
                    params=params)
    

if __name__ == "__main__":
    main()  # type: ignore reportCallIssue
    
    
