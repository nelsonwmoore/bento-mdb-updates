"""Run Liquibase Update on Changelog."""

from __future__ import annotations

import os
import stat
import subprocess
import tempfile
from pathlib import Path

import click
from dotenv import load_dotenv
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from pyliquibase import Pyliquibase

load_dotenv(override=True, dotenv_path="config/.env")
DRIVER_PATH = "/app/drivers"
DRIVER_JAR = f"{DRIVER_PATH}/liquibase-neo4j-4.31.1-full.jar"


@task(log_prints=True)
def check_environment() -> dict[str, str | bool | Path]:
    """Check environment configuration."""
    results = {}
    try:
        java_version = subprocess.check_output(
            ["java", "-version"],
            stderr=subprocess.STDOUT,
            text=True,
        )
        results["java_version"] = java_version
    except Exception as e:
        results["java_version_error"] = str(e)

    results["java_home"] = os.environ.get("JAVA_HOME", "Not set")

    results["driver_exists"] = Path(DRIVER_PATH).exists()
    results["driver_path_absolute"] = Path(DRIVER_PATH).resolve()

    results["working_directory"] = Path.cwd()
    return results


@task(log_prints=True)
def verify_environment() -> None:
    """Verify environment configuration."""
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            check=True,
        )
        print(f"Java version stderr: {result.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"Java check failed: {e}")

    # Check file system for driver JARs
    paths_to_check = [
        "/app/drivers",
        "/app",
        "./drivers",
        "../drivers",
        "/app/bento-mdb-updates-main/drivers",
    ]

    for path in paths_to_check:
        print(f"Checking path: {path}")
        if Path(path).exists():
            try:
                files = list(Path(path).glob("*.jar"))
                print(f"  JAR files in {path}: {files}")

                # Check file permissions if files exist
                for jar_file in files:
                    file_stat = jar_file.stat()
                    print(
                        f"  {jar_file}: {stat.filemode(file_stat.st_mode)}, size: {file_stat.st_size}",
                    )
            except Exception as e:
                print(f"  Error listing {path}: {e}")
        else:
            print(f"  Path does not exist: {path}")


@task(log_prints=True)
def set_defaults_file(
    mdb_uri: str,
    mdb_user: str,
    changelog_file: str,
) -> Path:
    """Create temporary defaults file and return path."""
    uri = mdb_uri or os.environ.get("NEO4J_MDB_URI")
    user = mdb_user or os.environ.get("NEO4J_MDB_USER")
    password = Secret.load("fnl-mdb-dev-pwd").get()
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        f.write(f"changelogFile: {changelog_file}\n")
        f.write(f"url: {uri}\n")
        f.write(f"username: {user}\n")
        f.write(f"password: {password}\n")
        f.write("driver: liquibase.ext.neo4j.database.jdbc.Neo4jDriver\n")
        f.write("logLevel: DEBUG")
        temp_file_path = Path(f.name)
    temp_file_path.chmod(
        stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP,
    )  # User/group read/write only
    print(f"Created defaults file at {temp_file_path} with content:")
    print(f"changelogFile: {changelog_file}")
    print(f"url: {uri}")
    print(f"username: {user}")
    print("password: ********")
    print("logLevel: DEBUG")
    return temp_file_path


@task(log_prints=True)
def run_liquibase_update(defaults_file: Path | str, *, dry_run: bool = False) -> None:
    """Run Liquibase Update on Changelog."""
    print(f"Running liquibase {'updateSQL' if dry_run else 'update'}")
    print(f"Defaults file: {defaults_file}")
    print(f"Driver directory: {DRIVER_PATH}")
    print(f"Driver JAR exists: {Path(DRIVER_JAR).exists()}")
    print(
        f"Driver directory contents: {list(Path(DRIVER_PATH).glob('*')) if Path(DRIVER_PATH).exists() else 'Directory not found'}",
    )

    liquibase = Pyliquibase(
        defaultsFile=str(defaults_file),
        jdbcDriversDir=DRIVER_PATH,
        additionalClasspath=DRIVER_JAR,
    )
    print("Checking status...")
    liquibase.status()
    print("Validating...")
    liquibase.validate()

    if dry_run:
        print("Running updateSQL (dry run)...")
        liquibase.updateSQL()
    else:
        print("Running update...")
        liquibase.update()


@flow(name="liquibase-update", log_prints=True)
def liquibase_update_flow(
    mdb_uri: str,
    mdb_user: str,
    changelog_file: str,
    *,
    dry_run: bool = False,
) -> None:
    """Run Liquibase Update on Changelog."""
    logger = get_run_logger()
    env_check = check_environment()
    logger.info("Environment check results: %s", env_check)
    verify_environment()
    defaults_file = set_defaults_file(mdb_uri, mdb_user, changelog_file)
    print(f"Changelog file: {Path(changelog_file).resolve()}")

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
    changelog_file: str,
    *,
    dry_run: bool = False,
) -> None:
    """Run Liquibase Update on Changelog."""
    liquibase_update_flow(
        mdb_uri=mdb_uri,
        mdb_user=mdb_user,
        changelog_file=changelog_file,
        dry_run=dry_run,
    )


if __name__ == "__main__":
    main()
