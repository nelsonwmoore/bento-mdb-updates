#!/usr/bin/env python3
"""Generate matrix with models/versions to be added to MDB."""

from __future__ import annotations

import json
from pathlib import Path

import click
from bento_meta.mdb import MDB
from prefect import flow
from prefect.blocks.system import Secret

from bento_mdb_updates.constants import MDB_IDS_WITH_PRERELEASES, VALID_MDB_IDS
from bento_mdb_updates.model_cdes import (
    compare_model_specs_to_mdb,
    get_yaml_files_from_spec,
    load_model_specs_from_yaml,
)


def make_matrix_output_more_visible(matrix: dict) -> None:
    """
    Make the matrix output more visible in logs.

    Print multiple times with clear markers.
    """
    result_json = json.dumps(matrix)
    print("\n" + "*" * 80)  # noqa: T201
    print("MATRIX_JSON_BEGIN")  # noqa: T201
    print(f"MATRIX_JSON:{result_json}")  # noqa: T201
    print("MATRIX_JSON_END")  # noqa: T201
    print("*" * 80 + "\n")  # noqa: T201
    print(f"MATRIX_JSON:{result_json}")  # noqa: T201


def verify_mdb_connection(mdb: MDB) -> None:
    """
    Validate that MDB connection is working and has models.

    Raises:
        ConnectionError: if connection fails.
        RuntimeError: if MDB empty when it shouldn't be.
    """
    if mdb.driver is None:
        msg = f"Failed to connect to MDB: {mdb.uri}"
        raise ConnectionError(msg)
    if not hasattr(mdb, "models") or mdb.models is None or len(mdb.models) == 0:
        msg = f"No model information could be retrieved from MDB: {mdb.uri}"
        raise RuntimeError(msg)
    print(f"MDB connection validated: {len(mdb.models)} models found in database")


@flow(name="generate-model-version-matrix", log_prints=True)
def model_matrix_flow(
    mdb_uri: str,
    mdb_user: str,
    mdb_id: str,
    model_specs_yaml: str,
    *,
    datahub_only: bool,
) -> None:
    """Generate matrix with models/versions to be added to MDB."""
    model_specs = load_model_specs_from_yaml(Path(model_specs_yaml))

    if mdb_id not in VALID_MDB_IDS:
        msg = f"Invalid MDB ID: {mdb_id}. Valid IDs: {VALID_MDB_IDS}"
        raise ValueError(msg)
    pwd_secret_name = mdb_id + "-pwd"
    password = Secret.load(pwd_secret_name).get()  # type: ignore reportAttributeAccessIssue
    if mdb_id.startswith("og-mdb"):
        password = ""
    if mdb_uri.startswith("jdbc:neo4j:"):
        mdb_uri = mdb_uri.replace("jdbc:neo4j:", "")
    mdb = MDB(
        uri=mdb_uri,
        user=mdb_user,
        password=password,
    )
    verify_mdb_connection(mdb)

    include_prerelease = mdb_id in MDB_IDS_WITH_PRERELEASES
    models_to_update = compare_model_specs_to_mdb(
        model_specs,
        mdb,
        datahub_only=datahub_only,
        include_prerelease=include_prerelease,
    )
    matrix = {
        "include": [
            {
                "model": model,
                "version": version,
                "mdf_files": get_yaml_files_from_spec(
                    model_specs[model],
                    model,
                    version,
                ),
            }
            for model, versions in models_to_update.items()
            for version in versions
        ],
    }
    make_matrix_output_more_visible(matrix)


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
    mdb_id: str,
    model_specs_yaml: str,
    *,
    datahub_only: bool,
) -> None:
    """Generate matrix with models/versions to be added to MDB."""
    model_matrix_flow(
        mdb_uri=mdb_uri,
        mdb_user=mdb_user,
        mdb_id=mdb_id,
        model_specs_yaml=model_specs_yaml,
        datahub_only=datahub_only,
    )


if __name__ == "__main__":
    main()
