"""Run Liquibase Update on Changelog."""

from __future__ import annotations

import io
import logging
import shutil
import stat
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import click
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect.logging.handlers import APILogHandler
from pyliquibase import Pyliquibase

DRIVER_PATH = "/app/drivers"
DRIVER_JAR = "liquibase-neo4j-4.31.1-full.jar"
LIQUIBASE_VERSION = "4.31.1"
DRIVER_NAME = "liquibase.ext.neo4j.database.jdbc.Neo4jDriver"
VALID_MDB_IDS = [
    "fnl-mdb-dev",
    "cloud-one-mdb-dev",
    "og-mdb-dev",
    "og-mdb-nightly",
    "og-mdb-prod",
]
VALID_LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "severe": logging.CRITICAL,
    "off": logging.NOTSET,
}
# configure jvm
JVM_HEAP_MIN = "1g"
JVM_HEAP_MAX = "3g"


@task
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
        logger.warning(
            "Invalid log level %r (valid: %s); defaulting to 'info'.",
            log_level,
            list(VALID_LOG_LEVELS.keys()),
        )
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


@task
def run_liquibase_update(defaults_file: Path | str, *, dry_run: bool = False) -> None:
    """Run Liquibase Update on Changelog."""
    logger = get_run_logger()

    # Set Java system properties to control logging
    from jnius import autoclass

    System = autoclass("java.lang.System")  # noqa: N806
    System.setProperty("liquibase.logLevel", "INFO")
    System.setProperty("liquibase.logChannels", "all")
    LogManager = autoclass("java.util.logging.LogManager")  # noqa: N806
    Level = autoclass("java.util.logging.Level")  # noqa: N806
    root_logger = LogManager.getLogManager().getLogger("")
    liquibase_logger = LogManager.getLogManager().getLogger("liquibase")
    if root_logger:
        root_logger.setLevel(Level.INFO)
    if liquibase_logger:
        liquibase_logger.setLevel(Level.INFO)

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

    out_capture = io.StringIO()
    err_capture = io.StringIO()
    try:
        with redirect_stdout(out_capture), redirect_stderr(err_capture):
            if dry_run:
                logger.info("Running updateSQL (dry run)...")
                plb.updateSQL()
            else:
                logger.info("Running update...")
                plb.update()
    finally:
        for line in out_capture.getvalue().splitlines():
            if not line.strip():
                continue
            logger.info(line)
        for line in err_capture.getvalue().splitlines():
            if not line.strip():
                continue
            logger.error(line)


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
    # configure jvm
    import jnius_config

    jnius_config.add_options(f"-Xms{JVM_HEAP_MIN}", f"-Xmx{JVM_HEAP_MAX}")

    # set up pyliquibase logger use prefect api log handler
    plb_logger = logging.getLogger("pyliquibase")
    plb_logger.setLevel(VALID_LOG_LEVELS[log_level])
    if not any(isinstance(h, APILogHandler) for h in plb_logger.handlers):
        plb_logger.addHandler(APILogHandler())

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

    logger.info("Liquibase finished.")


@click.command()
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
