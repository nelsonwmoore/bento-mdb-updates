#!/usr/bin/env python3
"""Generate matrix with models/versions to be added to MDB."""

from __future__ import annotations

import json
import os
from pathlib import Path

import dotenv
from bento_meta.mdb import MDB

from bento_mdb_updates.model_cdes import (
    compare_model_specs_to_mdb,
    get_yaml_files_from_spec,
    load_model_specs_from_yaml,
)

dotenv.load_dotenv(Path("config/.env"), override=True)


def main() -> None:
    """Generate matrix with models/versions to be added to MDB."""
    model_specs_yaml = Path("config/mdb_models.yml")
    datahub_only = True
    model_specs = load_model_specs_from_yaml(model_specs_yaml)
    mdb = MDB(
        uri=os.environ.get("NEO4J_MDB_URI"),
        user=os.environ.get("NEO4J_MDB_USER"),
        password=os.environ.get("NEO4J_MDB_PASS"),
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


if __name__ == "__main__":
    main()
