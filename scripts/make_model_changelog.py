#!/usr/bin/env python3
"""
Script to take one MDF file representing a model and produce a Liquibase Changelog.

This contains the necessary cypher statements to add the model to an MDB.
"""

from __future__ import annotations

import logging
from pathlib import Path

import click
from bento_mdf.mdf import MDF

from bento_mdb_updates.changelogs import ModelToChangelogConverter

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--model_handle",
    required=True,
    type=str,
    prompt=True,
    help="CRDC Model Handle (e.g. 'GDC')",
)
@click.option(
    "--mdf_files",
    required=True,
    type=str,
    prompt=True,
    multiple=True,
    help="path or URL to MDF YAML file(s)",
)
@click.option(
    "--output_file_path",
    required=True,
    type=click.Path(),
    prompt=True,
    help="File path for output changelog",
)
@click.option(
    "--author",
    required=True,
    type=str,
    help="Author for changeset",
)
@click.option(
    "--_commit",
    required=False,
    type=str,
    help="Commit string",
)
@click.option(
    "--add_rollback",
    required=False,
    type=bool,
    help="Add cypher stmts with rollback to changesets",
)
@click.option(
    "--model_version",
    required=False,
    type=str,
    help="Manually set model version (e.g., 1.2.3) if not included in MDF.",
)
@click.option(
    "--latest_version",
    required=False,
    type=bool,
    help="Is this the latest data model version?",
)
@click.option(
    "--terms_only",
    required=False,
    type=bool,
    help="MDF is only terms with empty nodes/rels/propdefs",
)
def main(
    model_handle: str,
    mdf_files: str | list[str],
    output_file_path: str | Path,
    author: str,
    _commit: str | None,
    model_version: str | None,
    *,
    add_rollback: bool,
    latest_version: bool,
    terms_only: bool,
) -> None:
    """Get liquibase changelog from mdf files for a model."""
    logger.info("Script started")

    mdf = MDF(*mdf_files, handle=model_handle, _commit=_commit, raise_error=True)
    if not mdf.model:
        msg = "Error getting model from MDF"
        raise RuntimeError(msg)
    logger.info("Model MDF loaded successfully")

    converter = ModelToChangelogConverter(
        model=mdf.model,
        add_rollback=add_rollback,
        terms_only=terms_only,
    )
    changelog = converter.convert_model_to_changelog(
        author,
        model_version,
        latest_version=latest_version,
    )
    logger.info("Changelog converted from MDF successfully")

    changelog.save_to_file(str(Path(output_file_path)), encoding="UTF-8")
    logger.info("Changelog saved at: {output_file_path}")


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
