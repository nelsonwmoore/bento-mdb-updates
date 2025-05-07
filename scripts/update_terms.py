#!/usr/bin/env python3
"""Check for new caDSR PVs and NCIT mappings and generate Cypher to update MDB."""

from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import TYPE_CHECKING

import click
from bento_meta.mdb.mdb import MDB
from github import Github, GithubException
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret

from bento_mdb_updates.cde_cypher import convert_model_cdes_to_changelog
from bento_mdb_updates.clients import CADSRClient, NCItClient
from bento_mdb_updates.constants import (
    GITHUB_TOKEN_SECRET_NWM,
    MDB_UPDATES_GH_REPO,
    VALID_MDB_IDS,
)
from bento_mdb_updates.model_cdes import (
    add_ncit_synonyms_to_model_cde_spec,
    get_cdes_from_mdb,
)

if TYPE_CHECKING:
    from bento_mdb_updates.datatypes import MDBCDESpec, ModelCDESpec


@task
def get_current_mdb_cdes(
    mdb_uri: str,
    mdb_user: str,
    mdb_id: str,
) -> list[MDBCDESpec]:
    """Get current MDB CDEs."""
    if mdb_id not in VALID_MDB_IDS:
        msg = f"Invalid MDB ID: {mdb_id}. Valid IDs: {VALID_MDB_IDS}"
        raise ValueError(msg)
    pwd_secret_name = mdb_id + "-pwd"
    password = Secret.load(pwd_secret_name).get()  # type: ignore reportAttributeAccessIssue
    if mdb_id.startswith("og-mdb"):
        password = ""
    if mdb_uri.startswith("jdbc:neo4j:"):
        mdb_uri = mdb_uri.replace("jdbc:neo4j:", "")

    # Get current MDB CDE Pvs & Synonyms
    mdb = MDB(
        uri=mdb_uri,
        user=mdb_user,
        password=password,
    )
    return get_cdes_from_mdb(mdb)


@task
def update_mdb_cdes_from_term_sources(
    mdb_cdes: list[MDBCDESpec],
) -> ModelCDESpec:
    """Update ModelCDESpec with new CDE PVs and synonyms from caDSR and NCIt."""
    logger = get_run_logger()
    today = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y%m%d")
    update_cde_spec: ModelCDESpec = {
        "handle": "TERM_UPDATES",
        "version": today,
        "annotations": [],
    }

    # Check caDSR for new PVs
    logger.info("Checking caDSR for new PVs...")
    cadsr_client = CADSRClient()
    cadsr_annotations = cadsr_client.check_cdes_against_mdb(mdb_cdes)
    update_cde_spec["annotations"].extend(cadsr_annotations)

    # get NCIt synonyms for new PVs
    logger.info("Getting NCIt synonyms for new PVs...")
    ncit_client = NCItClient()
    add_ncit_synonyms_to_model_cde_spec(update_cde_spec, ncit_client)

    # check NCIt for new PV synonyms
    if ncit_client.check_ncit_for_updated_mappings(force_update=True):
        logger.info("Checking NCIt for new PV synonyms...")
        ncit_annotations = ncit_client.check_synonyms_against_mdb(
            mdb_cdes,
        )
        update_cde_spec["annotations"].extend(ncit_annotations)
    return update_cde_spec


@task
def commit_new_files(files: list[Path]) -> list:
    """Commit new files to GitHub."""
    logger = get_run_logger()
    github_token = Secret.load(GITHUB_TOKEN_SECRET_NWM).get()  # type: ignore reportAttributeAccessIssue
    gh = Github(github_token)
    repo = gh.get_repo(MDB_UPDATES_GH_REPO)

    results = []
    for file_path in files:
        try:
            path_str = str(file_path)
            with file_path.open("r", encoding="utf-8") as f:
                file_content = f.read()
            try:
                file_contents = repo.get_contents(path_str)
                sha = (
                    file_contents[0].sha
                    if isinstance(file_contents, list)
                    else file_contents.sha
                )
                logger.info("File %s already exists", path_str)
                commit_msg = f"Update {file_path.name} (GitHub Actions)"
                result = repo.update_file(
                    path=path_str,
                    message=commit_msg,
                    content=file_content,
                    sha=sha,
                )
                results.append(
                    f"Updated {path_str} (commit: {result['commit'].sha[:7]})",
                )
            except GithubException:
                logger.info("File %s does not exist", path_str)
                commit_msg = f"Add {file_path.name} (GitHub Actions)"
                result = repo.create_file(
                    path=path_str,
                    message=commit_msg,
                    content=file_content,
                )
                results.append(
                    f"Created {path_str} (commit: {result['commit'].sha[:7]})",
                )
        except Exception as e:  # noqa: PERF203
            error_msg = f"Error updating {file_path}: {e}"
            results.append(error_msg)
            logger.exception(error_msg)

    for result in results:
        logger.info(result)

    if any("Error" in result for result in results):
        msg = "Some files failed to update. See logs for details."
        raise RuntimeError(msg)

    return results


@flow(name="update-terms")
def update_terms(  # noqa: PLR0913
    mdb_uri: str,
    mdb_user: str,
    mdb_id: str,
    author: str,
    output_file: str | Path | None = None,
    commit: str | None = None,
    *,
    no_commit: bool = True,
) -> None:
    """Check for new CDE PVs and synonyms and generate Cypher to update the database."""
    logger = get_run_logger()
    today = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y%m%d")
    if output_file is None:
        output_file = Path(f"data/output/mdb_cdes/mdb_cdes_{today}.json")
    else:
        output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    mdb_cdes = get_current_mdb_cdes(mdb_uri, mdb_user, mdb_id)
    update_cde_spec = update_mdb_cdes_from_term_sources(mdb_cdes)

    # convert annotation updates to liquibase changelog
    changelog = convert_model_cdes_to_changelog(update_cde_spec, author, commit)
    output_dir = Path().cwd() / "data/output/term_changelogs"
    changelog_file = output_dir / f"{today}_term_updates.xml"
    changelog_file.parent.mkdir(parents=True, exist_ok=True)
    changelog.save_to_file(str(changelog_file), encoding="UTF-8")

    # Update mdb_cdes JSON file
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(mdb_cdes, f, indent=2)

    if not no_commit:
        logger.info("Committing changes...")
        commit_new_files([output_file, changelog_file])

    # Print changlog file as JSON for GitHub Actions
    print(json.dumps([str(changelog_file)]))  # noqa: T201


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
    "--mdb_id",
    required=True,
    type=str,
    prompt=True,
    help="MDB ID",
)
@click.option(
    "-a",
    "--author",
    required=True,
    type=str,
    help="Author for changeset",
)
@click.option(
    "--output-file",
    required=False,
    type=str,
    help="Output file path for MDB CDE JSON",
)
@click.option(
    "-c",
    "--commit",
    required=False,
    type=str,
    help="Commit string",
)
@click.option(
    "--no_commit",
    required=False,
    type=str,
    help="Commit string",
    default=False,
)
def main(  # noqa: PLR0913
    mdb_uri: str,
    mdb_user: str,
    mdb_id: str,
    author: str,
    output_file: str | Path | None = None,
    commit: str | None = None,
    *,
    no_commit: bool = False,
) -> None:
    """Check for new CDE PVs and syonyms and generate Cypher to update the database."""
    update_terms(
        mdb_uri=mdb_uri,
        mdb_user=mdb_user,
        mdb_id=mdb_id,
        author=author,
        output_file=output_file,
        commit=commit,
        no_commit=no_commit,
    )


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameters
