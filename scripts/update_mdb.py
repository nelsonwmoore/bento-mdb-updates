"""Run Liquibase Update on Changelog."""

from __future__ import annotations

import shutil
import stat
import subprocess
import tempfile
from pathlib import Path

import click
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from pyliquibase import Pyliquibase

DRIVER_PATH = "/app/drivers"
DRIVER_JAR = "liquibase-neo4j-4.31.1-full.jar"
LIQUIBASE_VERSION = "4.31.1"
DRIVER_NAME = "liquibase.ext.neo4j.database.jdbc.Neo4jDriver"
VALID_MDB_IDS = [
    "fnl-mdb-dev",
    "og-mdb-dev",
    "og-mdb-nightly",
    "og-mdb-prod",
]
VALID_LOG_LEVELS = [
    "debug",
    "info",
    "warning",
    "severe",
    "off",
]


@task(log_prints=True)
def set_defaults_file(
    mdb_uri: str,
    mdb_user: str,
    changelog_file: str,
    mdb_id: str,
    log_level: str,
) -> Path:
    """Create temporary defaults file and return path."""
    logger = get_run_logger()
    if mdb_id not in VALID_MDB_IDS:
        msg = f"Invalid MDB ID: {mdb_id}. Valid IDs: {VALID_MDB_IDS}"
        raise ValueError(msg)
    if log_level not in VALID_LOG_LEVELS:
        msg = (
            f"Invalid log level: {log_level}. Valid levels: {VALID_LOG_LEVELS}.",
            "Defaulting to 'info'.",
        )
        logger.warning(msg)
        log_level = "info"
    pwd_secret_name = mdb_id + "-pwd"
    uri = mdb_uri
    user = mdb_user
    password = Secret.load(pwd_secret_name).get()  # type: ignore reportAttributeAccessIssue
    if mdb_id.startswith("og-mdb"):
        password = ""
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        f.write(f"changelogFile: {changelog_file}\n")
        f.write(f"url: {uri}\n")
        f.write(f"username: {user}\n")
        f.write(f"password: {password}\n")
        f.write(f"classpath: {DRIVER_PATH}\n")
        f.write(f"driver: {DRIVER_NAME}\n")
        f.write(f"logLevel: {log_level}")
        temp_file_path = Path(f.name)
    temp_file_path.chmod(
        stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP,
    )  # User/group read/write only
    return temp_file_path


@task(log_prints=True)
def run_liquibase_update(defaults_file: Path | str, *, dry_run: bool = False) -> None:
    """Run Liquibase Update on Changelog."""
    logger = get_run_logger()
    plb = Pyliquibase(
        defaultsFile=str(defaults_file),
        jdbcDriversDir=DRIVER_PATH,
        version=LIQUIBASE_VERSION,
    )

    # copy Neo4j extension JAR to Liquibase's lib folder
    ext_jar = Path(DRIVER_PATH) / DRIVER_JAR
    dest_lib = Path(plb.liquibase_lib_dir)
    shutil.copy(ext_jar, dest_lib)
    logger.info("Copied Neo4j extension JAR from %s to %s", ext_jar, dest_lib)

    # try liquibase cli
    lb_dir = Path(plb.liquibase_dir)
    logger.info("Liquibase directory: %s", lb_dir)
    lb_bin = lb_dir / "liquibase"
    mode = lb_bin.stat().st_mode
    if not (mode & stat.S_IXUSR):
        lb_bin.chmod(mode | stat.S_IXUSR)
        logger.info("Added execute bit to %s", lb_bin)
    action = "updateSQL" if dry_run else "update"
    cmd = [
        str(lb_bin),
        f"--defaults-file={defaults_file}",
        action,
    ]
    msg = f"Invoking Liquibase CLI →{' '.join(cmd)}"
    logger.info(msg)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        result = e
    logger.info("── Liquibase STDOUT ──")
    logger.info(result.stdout or "<no stdout>")
    logger.info("── Liquibase STDERR ──")
    logger.info(result.stderr or "<no stderr>")
    if result.returncode != 0:
        msg = f"Liquibase `{action}` failed with exit code {result.returncode}"
        raise RuntimeError(msg)

    if dry_run:
        logger.info("Running updateSQL (dry run)...")
        plb.updateSQL()
    else:
        logger.info("Running update...")
        plb.update()


@flow(name="liquibase-update", log_prints=True)
def liquibase_update_flow(  # noqa: PLR0913
    mdb_uri: str,
    mdb_user: str,
    changelog_file: str,
    mdb_id: str,
    log_level: str = "info",
    *,
    dry_run: bool = False,
) -> None:
    """Run Liquibase Update on Changelog."""
    logger = get_run_logger()
    defaults_file = set_defaults_file(
        mdb_uri,
        mdb_user,
        changelog_file,
        mdb_id,
        log_level,
    )
    # print out the contents of the defaults file
    raw = Path(defaults_file).read_text().splitlines()  # type:ignore reportArgumentType
    for line in raw:
        if line.lower().startswith("password"):
            logger.info("password: ********")
        else:
            logger.info(line)
    logger.info("Changelog file: %s", Path(changelog_file).resolve())

    try:
        run_liquibase_update(defaults_file, dry_run=dry_run)
    finally:
        defaults_file.unlink(missing_ok=True)  # type:ignore reportAttributeAccessIssue


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
    "--changelog_file",
    required=True,
    type=str,
    prompt=True,
    help="Changelog file to update",
)
@click.option("--mdb_id", type=str, help="MDB ID", prompt=True, required=True)
@click.option("--log_level", type=str, help="Log level", prompt=True, required=True)
@click.option(
    "--dry_run",
    is_flag=True,
    default=False,
    help="Dry run flag",
)
def main(  # noqa: PLR0913
    mdb_uri: str,
    mdb_user: str,
    changelog_file: str,
    mdb_id: str,
    log_level: str,
    *,
    dry_run: bool = False,
) -> None:
    """Run Liquibase Update on Changelog."""
    liquibase_update_flow(
        mdb_uri=mdb_uri,
        mdb_user=mdb_user,
        changelog_file=changelog_file,
        mdb_id=mdb_id,
        log_level=log_level,
        dry_run=dry_run,
    )


if __name__ == "__main__":
    main()  # type: ignore reportCallIssue
