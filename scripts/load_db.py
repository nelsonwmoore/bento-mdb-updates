"""Orchestration script to load Neo4j database from S3 dump."""

from __future__ import annotations

import boto3
import click
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect_shell import shell_run_command

from bento_mdb_updates.constants import VALID_MDB_IDS

DEFAULT_TASK_DEFINITION_FAMILY = "fnlmdbdevneo4jtaskDef"


@task
def get_running_task(
    cluster: str,
    task_definition_family: str = DEFAULT_TASK_DEFINITION_FAMILY,
) -> None:
    """Get ECS task ARN of running Neo4j container."""
    logger = get_run_logger()
    ecs = boto3.client("ecs")

    tasks = ecs.list_tasks(
        cluster=cluster,
        desiredStatus="RUNNING",
    )

    if not tasks["taskArns"]:
        msg = "No running ECS tasks found."
        raise ValueError(msg)

    if not task_definition_family:
        logger.error(
            "No task definition family specified.",
        )
        msg = "No task definition family specified."
        raise ValueError(msg)

    task_details = ecs.describe_tasks(
        cluster=cluster,
        tasks=tasks["taskArns"],
    )

    for tsk in task_details["tasks"]:
        task_def_arn = tsk["taskDefinitionArn"]
        task_family = task_def_arn.split("/")[-1].split(":")[0]

        logger.info("Found task with family: %s", task_family)

        if task_family == task_definition_family:
            logger.info("Matched Neo4j task: %s", tsk["taskArn"])
            return tsk["taskArn"]

    available_families = [
        task["taskDefinitionArn"].split("/")[-1].split(":")[0]
        for task in task_details["tasks"]
    ]
    msg = (
        f"No running task found with family '{task_definition_family}'"
        f". Available families: {available_families}"
    )
    raise ValueError(msg)


@task
def download_from_s3(cluster: str, task_arn: str, s3_bucket: str, s3_key: str) -> str:
    """Download Neo4j database dump from S3."""
    logger = get_run_logger()

    mkdir_command = f"""
    aws ecs execute-command \
        --cluster {cluster} \
        --task {task_arn} \
        --container neo4j \
        --command "mkdir -p /tmp/dumps"
    """

    mkdir_result = shell_run_command.fn(command=mkdir_command, return_all=True)
    logger.info("Mkdir Exit Code: %s", mkdir_result.exit_code)

    # Download dump file from S3
    download_command = f"""
    aws ecs execute-command \
        --cluster {cluster} \
        --task {task_arn} \
        --container neo4j \
        --command "aws s3 cp s3://{s3_bucket}/{s3_key} /tmp/dumps/neo4j.dump"
    """

    result = shell_run_command.fn(command=download_command, return_all=True)

    logger.info("S3 Download Exit Code: %s", result.exit_code)
    logger.info("S3 Download Stdout: %s", result.stdout)
    logger.info("S3 Download Stderr: %s", result.stderr)

    if result.exit_code != 0:
        msg = "S3 download failed."
        raise RuntimeError(msg)

    return "Download successful."


@task
def stop_neo4j_database(
    cluster: str,
    task_arn: str,
    database_name: str,
    database_pwd: str,
) -> str:
    """Stop Neo4j database before loading."""
    logger = get_run_logger()

    command = f"""
    aws ecs execute-command \
        --cluster {cluster} \
        --task {task_arn} \
        --container neo4j \
        --command "cypher-shell -u "neo4j" -p {database_pwd} 'STOP DATABASE {database_name}'"
    """

    result = shell_run_command.fn(command=command, return_all=True)

    logger.info("Database Stop Exit Code: %s", result.exit_code)
    logger.info("Database Stop Stdout: %s", result.stdout)
    logger.info("Database Stop Stderr: %s", result.stderr)

    return result.exit_code


@task
def execute_load_command(
    cluster: str,
    task_arn: str,
    database_name: str,
    *,
    overwrite: bool = True,
) -> str:
    """Execute Neo4j database load command."""
    logger = get_run_logger()

    overwrite_flag = "--overwrite-destination=true" if overwrite else ""

    command = f"""
    aws ecs execute-command \
        --cluster {cluster} \
        --task {task_arn} \
        --container neo4j \
        --command "neo4j-admin database load \
        --from-path=/tmp/dumps/neo4j.dump \
        --database={database_name} \
        {overwrite_flag}"
    """

    result = shell_run_command.fn(command=command, return_all=True)

    logger.info("Load Command Exit Code: %s", result.exit_code)
    logger.info("Load Command Stdout: %s", result.stdout)
    logger.info("Load Command Stderr: %s", result.stderr)

    return result.exit_code


@task
def start_neo4j_database(
    cluster: str,
    task_arn: str,
    database_name: str,
    database_pwd: str,
) -> str:
    """Start Neo4j database after loading."""
    logger = get_run_logger()

    command = f"""
    aws ecs execute-command \
        --cluster {cluster} \
        --task {task_arn} \
        --container neo4j \
        --command "cypher-shell -u neo4j -p {database_pwd} 'START DATABASE {database_name}'"
    """

    result = shell_run_command.fn(command=command, return_all=True)

    logger.info("Database Start Exit Code: %s", result.exit_code)
    logger.info("Database Start Stdout: %s", result.stdout)
    logger.info("Database Start Stderr: %s", result.stderr)

    return result.exit_code


@task
def cleanup_temp_files(cluster: str, task_arn: str) -> str:
    """Clean up temporary dump files."""
    logger = get_run_logger()

    command = f"""
    aws ecs execute-command \
        --cluster {cluster} \
        --task {task_arn} \
        --container neo4j \
        --command "rm -rf /tmp/dumps/neo4j.dump"
    """

    result = shell_run_command.fn(command=command, return_all=True)

    logger.info("Cleanup Exit Code: %s", result.exit_code)
    logger.info("Cleanup Stdout: %s", result.stdout)
    logger.info("Cleanup Stderr: %s", result.stderr)

    return "Cleanup completed."


@flow(name="neo4j-database-load")
def neo4j_load_flow(  # noqa: PLR0913
    cluster: str,
    s3_bucket: str,
    s3_key: str,
    database_name: str,
    task_definition_family: str,
    mdb_id: str,
    *,
    dry_run: bool = False,
    skip_stop: bool = False,
    skip_cleanup: bool = False,
) -> None:
    """Load Neo4j database from S3 dump file."""
    logger = get_run_logger()
    logger.info("Running neo4j-database-load flow...")

    if mdb_id not in VALID_MDB_IDS:
        msg = f"Invalid MDB ID: {mdb_id}. Valid IDs: {VALID_MDB_IDS}"
        raise ValueError(msg)
    pwd_secret_name = mdb_id + "-pwd"
    password = Secret.load(pwd_secret_name).get()

    if dry_run:
        logger.info(
            "DRY RUN: Would load database %s from s3://%s/%s",
            database_name,
            s3_bucket,
            s3_key,
        )
        return

    try:
        task_arn = get_running_task(cluster, task_definition_family)
        logger.info("Found running task: %s", task_arn)

        download_from_s3(cluster, task_arn, s3_bucket, s3_key)
        logger.info("Successfully downloaded dump file from S3")

        if not skip_stop:
            stop_exit_code = stop_neo4j_database(
                cluster,
                task_arn,
                database_name,
                password,
            )
            if stop_exit_code != 0:
                logger.warning(
                    "Database stop command returned non-zero exit code: %s",
                    stop_exit_code,
                )

        load_exit_code = execute_load_command(
            cluster,
            task_arn,
            database_name,
            overwrite=not skip_stop,
        )

        if not skip_stop:
            start_exit_code = start_neo4j_database(
                cluster,
                task_arn,
                database_name,
                password,
            )
            if start_exit_code != 0:
                logger.error("Failed to start Neo4j database after load")
                msg = "Neo4j database failed to start"
                raise RuntimeError(msg)

        if load_exit_code == 0:
            logger.info("Neo4j database load completed successfully")
            # Clean up temporary files
            if not skip_cleanup:
                cleanup_temp_files(cluster, task_arn)
        else:
            logger.error(
                "Neo4j database load failed with exit code: %s",
                load_exit_code,
            )
            msg = f"Database load failed with exit code: {load_exit_code}"
            raise RuntimeError(msg)

    except Exception:
        logger.exception("Neo4j load flow failed")
        if not skip_stop:
            try:
                task_arn = get_running_task(cluster, task_definition_family)
                start_neo4j_database(cluster, task_arn, database_name)
            except Exception:
                logger.exception(
                    "Failed to restart Neo4j database after error",
                )
        raise


@click.command()
@click.option(
    "--cluster",
    required=True,
    type=str,
    prompt=True,
    help="ECS cluster name",
)
@click.option(
    "--s3_bucket",
    required=True,
    type=str,
    prompt=True,
    help="S3 bucket name containing the dump file",
)
@click.option(
    "--s3_key",
    required=True,
    type=str,
    prompt=True,
    help="S3 key (path) to the dump file",
)
@click.option(
    "--database_name",
    required=True,
    type=str,
    prompt=True,
    help="Target Neo4j database name",
)
@click.option(
    "--task_definition_family",
    type=str,
    default="fnlmdbdevneo4jtaskDef",
    show_default=True,
    help="ECS task definition family name to identify Neo4j task",
)
@click.option(
    "--mdb_id",
    required=True,
    type=str,
    prompt=True,
    help="MDB ID",
)
@click.option(
    "--dry_run",
    is_flag=True,
    default=False,
    show_default=True,
    help="Dry run flag - show what would be done without executing",
)
@click.option(
    "--skip_stop",
    is_flag=True,
    default=False,
    show_default=True,
    help="Skip stopping/starting database (for Enterprise edition)",
)
@click.option(
    "--skip_cleanup",
    is_flag=True,
    default=False,
    show_default=True,
    help="Skip cleanup of temporary files",
)
def main(  # noqa: PLR0913
    cluster: str,
    s3_bucket: str,
    s3_key: str,
    database_name: str,
    task_definition_family: str,
    mdb_id: str,
    *,
    dry_run: bool = False,
    skip_stop: bool = False,
    skip_cleanup: bool = False,
) -> None:
    """Load Neo4j database from S3 dump file."""
    neo4j_load_flow(
        cluster=cluster,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        database_name=database_name,
        task_definition_family=task_definition_family,
        mdb_id=mdb_id,
        dry_run=dry_run,
        skip_stop=skip_stop,
        skip_cleanup=skip_cleanup,
    )


if __name__ == "__main__":
    main()
