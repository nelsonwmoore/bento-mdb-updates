#!/usr/bin/env python3
"""Generate matrix with models/versions to be added to MDB."""

from __future__ import annotations

import json
import os
from pathlib import Path

import click
import dotenv
from bento_meta.mdb import MDB
from prefect import flow

from bento_mdb_updates.model_cdes import (
    compare_model_specs_to_mdb,
    get_yaml_files_from_spec,
    load_model_specs_from_yaml,
)

dotenv.load_dotenv(Path("config/.env"), override=True)


@flow(name="generate-model-version-matrix")
def model_matrix_flow(
    mdb_uri: str,
    mdb_user: str,
    mdb_pass: str,
    model_specs_yaml: str,
    datahub_only: bool,
) -> None:
    """Generate matrix with models/versions to be added to MDB."""
    model_specs = load_model_specs_from_yaml(Path(model_specs_yaml))
    mdb = MDB(
        uri=mdb_uri or os.environ.get("NEO4J_MDB_URI"),
        user=mdb_user or os.environ.get("NEO4J_MDB_USER"),
        password=mdb_pass or os.environ.get("NEO4J_MDB_PASS"),
    )
    models_to_update = compare_model_specs_to_mdb(
        model_specs,
        mdb,
        datahub_only=datahub_only,
    )
    matrix = {
        "include": [
            {
                "model": model,
                "version": version,
                "mdf_files": get_yaml_files_from_spec(model_specs[model], version),
            }
            for model, versions in models_to_update.items()
            for version in versions
        ]
    }
    print(json.dumps(matrix))  # noqa: T201


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
    "--model_specs_yaml",
    required=True,
    type=str,
    prompt=True,
    help="path to model specs yaml file",
)
@click.option(
    "--datahub_only",
    required=False,
    type=bool,
    default=False,
    help="only include datahub models",
)
def main(
    mdb_uri: str,
    mdb_user: str,
    mdb_pass: str,
    model_specs_yaml: str,
    datahub_only: bool,
) -> None:
    """Generate matrix with models/versions to be added to MDB."""
    model_matrix_flow(
        mdb_uri=mdb_uri,
        mdb_user=mdb_user,
        mdb_pass=mdb_pass,
        model_specs_yaml=model_specs_yaml,
        datahub_only=datahub_only,
    )


if __name__ == "__main__":
    main()
