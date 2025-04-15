#!/usr/bin/env python3
"""Check for new caDSR PVs and NCIT mappings and generate Cypher to update MDB."""

from __future__ import annotations

import datetime
import json
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

import click
from bento_meta.mdb.mdb import MDB
from dotenv import load_dotenv
from prefect import flow

from bento_mdb_updates.cde_cypher import convert_model_cdes_to_changelog
from bento_mdb_updates.clients import CADSRClient, NCItClient
from bento_mdb_updates.model_cdes import (
    add_ncit_synonyms_to_model_cde_spec,
    get_cdes_from_mdb,
)

if TYPE_CHECKING:
    from bento_mdb_updates.datatypes import ModelCDESpec

logger = logging.getLogger(__name__)


@flow(name="update-terms")
def update_terms(
    mdb_uri: str,
    mdb_user: str,
    mdb_pass: str,
    author: str,
    output_file: str | Path | None = None,
    commit: str | None = None,
) -> None:
    """Check for new CDE PVs and syonyms and generate Cypher to update the database."""
    # Setup
    load_dotenv(Path("config/.env"), override=True)
    today = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y%m%d")
    if output_file is None:
        output_file = Path(f"data/output/mdb_cdes/mdb_cdes_{today}.json")
    else:
        output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    affected_models: set[tuple[str, str]] = set()

    # Get current MDB CDE Pvs & Synonyms
    mdb = MDB(
        uri=mdb_uri or os.environ.get("NEO4J_MDB_URI"),
        user=mdb_user or os.environ.get("NEO4J_MDB_USER"),
        password=mdb_pass or os.environ.get("NEO4J_MDB_PASS"),
    )
    mdb_cdes = get_cdes_from_mdb(mdb)
    update_cde_spec: ModelCDESpec = {
        "handle": "TERM_UPDATES",
        "version": today,
        "annotations": [],
    }

    # Check caDSR for new PVs
    logger.info("Checking caDSR for new PVs...")
    cadsr_client = CADSRClient()
    cadsr_annotations, cadsr_models = cadsr_client.check_cdes_against_mdb(mdb_cdes)
    update_cde_spec["annotations"].extend(cadsr_annotations)
    affected_models.update(cadsr_models)

    # get NCIt synonyms for new PVs
    logger.info("Getting NCIt synonyms for new PVs...")
    ncit_client = NCItClient()
    add_ncit_synonyms_to_model_cde_spec(update_cde_spec, ncit_client)

    # check NCIt for new PV synonyms
    if ncit_client.check_ncit_for_updated_mappings(force_update=True):
        logger.info("Checking NCIt for new PV synonyms...")
        ncit_annotaitons, ncit_models = ncit_client.check_synonyms_against_mdb(
            mdb_cdes,
        )
        update_cde_spec["annotations"].extend(ncit_annotaitons)
        affected_models.update(ncit_models)

    # convert annotation updates to liquibase changelog
    changelog = convert_model_cdes_to_changelog(update_cde_spec, author, commit)
    output_dir = Path().cwd() / "data/output/term_changelogs"
    changelog_file = output_dir / f"{today}_term_updates.xml"
    changelog_file.parent.mkdir(parents=True, exist_ok=True)
    changelog.save_to_file(str(changelog_file), encoding="UTF-8")

    # Update mdb_cdes JSON file
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(mdb_cdes, f, indent=2)

    # Print affected models and changlog file as JSON for GitHub Actions
    result = {
        "affected_models": json.dumps(
            [
                {"model": model, "version": version}
                for model, version in affected_models
            ],
        ),
        "changelog_files": json.dumps([str(changelog_file)]),
    }
    print(result)  # noqa: T201


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
    "--mdb_pass",
    required=True,
    type=str,
    prompt=True,
    help="metamodel database password",
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
    help="Output file path for MDB CDE JSON (defaults to data/output/mdb_cdes/mdb_cdes_<date>.json)",
)
@click.option(
    "-c",
    "--commit",
    required=False,
    type=str,
    help="Commit string",
)
def main(
    mdb_uri: str,
    mdb_user: str,
    mdb_pass: str,
    author: str,
    output_file: str | Path | None = None,
    commit: str | None = None,
) -> None:
    """Check for new CDE PVs and syonyms and generate Cypher to update the database."""
    update_terms(
        mdb_uri=mdb_uri,
        mdb_user=mdb_user,
        mdb_pass=mdb_pass,
        author=author,
        output_file=output_file,
        commit=commit,
    )


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameters
