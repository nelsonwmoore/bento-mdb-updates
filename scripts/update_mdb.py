"""Run Liquibase Update on Changelog."""

from __future__ import annotations

import logging
import os
import stat
import tempfile
from pathlib import Path

import click
from dotenv import load_dotenv
from prefect import flow, task
from pyliquibase import Pyliquibase

load_dotenv(override=True, dotenv_path="config/.env")
logger = logging.getLogger(__name__)


def set_defaults_file(
    mdb_uri: str,
    mdb_user: str,
    mdb_pass: str,
    changelog_file: str,
) -> Path:
    """Create temporary defaults file and return path."""
    uri = mdb_uri or os.environ.get("NEO4J_MDB_URI")
    user = mdb_user or os.environ.get("NEO4J_MDB_USER")
    password = mdb_pass or os.environ.get("NEO4J_MDB_PASS")
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        f.write(f"changelogFile: {changelog_file}\n")
        f.write(f"url: {uri}\n")
        f.write(f"username: {user}\n")
        f.write(f"password: {password}\n")
        f.write("classpath: ./app/drivers/liquibase-neo4j-4.31.1-full.jar\n")
        f.write("driver: liquibase.ext.neo4j.database.jdbc.Neo4jDriver\n")
        f.write("logLevel: info\n")
        temp_file_path = Path(f.name)
    temp_file_path.chmod(stat.S_IRUSR | stat.S_IWUSR)  # User read/write only
    return temp_file_path


@task
def run_liquibase_update(defaults_file: Path | str, *, dry_run: bool = False) -> None:
    """Run Liquibase Update on Changelog."""
    try:
        liquibase = Pyliquibase(str(defaults_file))
        if dry_run:
            liquibase.updateSQL()
        else:
            liquibase.update()
    except Exception:
        logger.exception("Liquibase error")
        raise


@flow(name="liquibase-update")
def liquibase_update_flow(
    mdb_uri: str,
    mdb_user: str,
    mdb_pass: str,
    changelog_file: str,
    *,
    dry_run: bool = False,
) -> None:
    """Run Liquibase Update on Changelog."""
    defaults_file = set_defaults_file(mdb_uri, mdb_user, mdb_pass, changelog_file)
    try:
        run_liquibase_update(defaults_file, dry_run=dry_run)
    finally:
        defaults_file.unlink(missing_ok=True)


@click.option(
    "--mdb_uri",
    required=True,
    type=str,
    prompt=True,
    help="metamodel database URI",
)
@click.option(
    "--mdb_user",
    required=True,
    type=str,
    prompt=True,
    help="metamodel database username",
)
@click.option(
    "--mdb_pass",
    required=True,
    type=str,
    prompt=True,
    help="metamodel database password",
    hide_input=True,
)
@click.option(
    "--changelog_file",
    required=True,
    type=str,
    prompt=True,
    help="Changelog file to update",
)
@click.option(
    "--dry_run",
    is_flag=True,
    default=False,
    help="Dry run flag",
)
def main(
    mdb_uri: str,
    mdb_user: str,
    mdb_pass: str,
    changelog_file: str,
    *,
    dry_run: bool = False,
) -> None:
    """Run Liquibase Update on Changelog."""
    liquibase_update_flow(
        mdb_uri=mdb_uri,
        mdb_user=mdb_user,
        mdb_pass=mdb_pass,
        changelog_file=changelog_file,
        dry_run=dry_run,
    )


if __name__ == "__main__":
    main()
