"""Orchestration script to update MDB and Data Hub sequentially."""

import click
from prefect import flow, get_run_logger
from prefect.deployments import run_deployment


@flow(name="update-mdb-and-dh")
def update_mdb_and_dh_flow(  # noqa: PLR0913
    mdb_uri: str,
    mdb_user: str,
    changelog_file: str,
    mdb_id: str,
    log_level: str,
    tier: str,
    *,
    dry_run: bool = False,
    no_commit: bool = True,
) -> None:
    """Orchestration script to update MDB and Data Hub sequentially."""
    logger = get_run_logger()
    logger.info("Running update-mdb-and-dh flow...")
    run_deployment(
        name="liquibase-update/liquibase-update",
        parameters={
            "mdb_uri": mdb_uri,
            "mdb_user": mdb_user,
            "changelog_file": changelog_file,
            "mdb_id": mdb_id,
            "log_level": log_level,
            "dry_run": dry_run,
        },
        timeout=None,
        as_subflow=True,
    )
    run_deployment(
        name="update-datahub/update-datahub",
        parameters={
            "mdb_uri": mdb_uri,
            "mdb_user": mdb_user,
            "mdb_id": mdb_id,
            "tier": tier,
            "no_commit": no_commit,
        },
        timeout=None,
        as_subflow=True,
    )


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
@click.option(
    "--mdb_id",
    required=True,
    type=str,
    prompt=True,
    help="MDB ID",
)
@click.option(
    "--log_level",
    required=True,
    type=str,
    prompt=True,
    help="Log level",
)
@click.option(
    "--tier",
    required=True,
    type=str,
    prompt=True,
    help="Data Hub tier to update (lower or upper)",
)
@click.option(
    "--dry_run",
    is_flag=True,
    default=False,
    show_default=True,
    help="Dry run flag",
)
@click.option(
    "--no_commit",
    type=bool,
    default=True,
    show_default=True,
    help="Don't commit changes",
)
def main(  # noqa: PLR0913
    mdb_uri: str,
    mdb_user: str,
    changelog_file: str,
    mdb_id: str,
    log_level: str,
    tier: str,
    *,
    dry_run: bool = False,
    no_commit: bool = True,
) -> None:
    """Orchestration script to update MDB and Data Hub sequentially."""
    update_mdb_and_dh_flow(
        mdb_uri=mdb_uri,
        mdb_user=mdb_user,
        changelog_file=changelog_file,
        mdb_id=mdb_id,
        log_level=log_level,
        tier=tier,
        dry_run=dry_run,
        no_commit=no_commit,
    )


if __name__ == "__main__":
    main()
