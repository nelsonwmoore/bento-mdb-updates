"""Orchestration script to load Neo4j database from S3 dump."""

from __future__ import annotations

import boto3
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect_shell import shell_run_command

from bento_mdb_updates.constants import VALID_MDB_IDS

DEFAULT_TASK_DEFINITION_FAMILY = "fnlmdbdevneo4jtaskDef"


@task
def get_running_task(
    cluster: str,
    task_definition_family: str = DEFAULT_TASK_DEFINITION_FAMILY,
) -> str:
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
async def download_from_s3(
    cluster: str,
    task_arn: str,
    s3_bucket: str,
    s3_key: str,
) -> str:
    """Download Neo4j database dump from S3."""
    logger = get_run_logger()

    mkdir_command = f"""
    aws ecs execute-command \
        --cluster {cluster} \
        --task {task_arn} \
        --container neo4j \
        --interactive \
        --command "mkdir -p /tmp/dumps"
    """

    mkdir_result = await shell_run_command(command=mkdir_command, return_all=True)
    logger.info("mkdir_result: %s", mkdir_result)

    # Download dump file from S3
    download_command = f"""
    aws ecs execute-command \
        --cluster {cluster} \
        --task {task_arn} \
        --container neo4j \
        --interactive \
        --command "aws s3 cp s3://{s3_bucket}/{s3_key} /tmp/dumps/neo4j.dump"
    """

    result = await shell_run_command(command=download_command, return_all=True)

    logger.info("S3 download result: %s", result)

    return "Download successful."


@task
async def stop_neo4j_database(
    cluster: str,
    task_arn: str,
    database_name: str,
    database_pwd: str,
) -> None:
    """Stop Neo4j database before loading."""
    logger = get_run_logger()

    command = f"""
    aws ecs execute-command \
        --cluster {cluster} \
        --task {task_arn} \
        --container neo4j \
        --interactive \
        --command "cypher-shell -u neo4j -p {database_pwd} 'STOP DATABASE {database_name}'"
    """

    result = await shell_run_command(command=command, return_all=True)

    logger.info("Database stop result: %s", result)


@task
async def execute_load_command(
    cluster: str,
    task_arn: str,
    database_name: str,
    *,
    overwrite: bool = True,
) -> None:
    """Execute Neo4j database load command."""
    logger = get_run_logger()

    overwrite_flag = "--overwrite-destination=true" if overwrite else ""

    command = f"""
    aws ecs execute-command \
        --cluster {cluster} \
        --task {task_arn} \
        --container neo4j \
        --interactive \
        --command "neo4j-admin database load --from-path=/tmp/dumps/neo4j.dump --database={database_name} {overwrite_flag}"
    """

    result = await shell_run_command(command=command, return_all=True)

    logger.info("Load command result: %s", result)


@task
async def start_neo4j_database(
    cluster: str,
    task_arn: str,
    database_name: str,
    database_pwd: str,
) -> None:
    """Start Neo4j database after loading."""
    logger = get_run_logger()

    command = f"""
    aws ecs execute-command \
        --cluster {cluster} \
        --task {task_arn} \
        --container neo4j \
        --interactive \
        --command "cypher-shell -u neo4j -p {database_pwd} 'START DATABASE {database_name}'"
    """

    result = await shell_run_command(command=command, return_all=True)

    logger.info("Database start result: %s", result)


@task
async def cleanup_temp_files(cluster: str, task_arn: str) -> str:
    """Clean up temporary dump files."""
    logger = get_run_logger()

    command = f"""
    aws ecs execute-command \
        --cluster {cluster} \
        --task {task_arn} \
        --container neo4j \
        --interactive \
        --command "rm -rf /tmp/dumps/neo4j.dump"
    """

    result = await shell_run_command(command=command, return_all=True)

    logger.info("Cleanup result: %s", result)

    return "Cleanup completed."


@flow(name="neo4j-database-load")
async def neo4j_load_flow(  # noqa: PLR0913
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
    password = (await Secret.load(pwd_secret_name)).get()

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

        await download_from_s3(cluster, task_arn, s3_bucket, s3_key)
        logger.info("Successfully downloaded dump file from S3")

        if not skip_stop:
            await stop_neo4j_database(
                cluster,
                task_arn,
                database_name,
                password,
            )

        await execute_load_command(
            cluster,
            task_arn,
            database_name,
            overwrite=not skip_stop,
        )

        if not skip_stop:
            await start_neo4j_database(
                cluster,
                task_arn,
                database_name,
                password,
            )

        if not skip_cleanup:
            await cleanup_temp_files(cluster, task_arn)
    except Exception:
        logger.exception("Neo4j load flow failed")
        if not skip_stop:
            try:
                task_arn = get_running_task(cluster, task_definition_family)
                await start_neo4j_database(cluster, task_arn, database_name, password)
            except Exception:
                logger.exception(
                    "Failed to restart Neo4j database after error",
                )
        raise
