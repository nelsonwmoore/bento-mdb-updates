#!/usr/bin/env python3
"""Given a set of model/version tuples, check if they're in datahub."""

from __future__ import annotations

import json
from pathlib import Path

import click

from bento_mdb_updates.model_cdes import load_model_specs_from_yaml


@click.command()
@click.option(
    "--affected_models_json",
    help="JSON string with affected models",
    required=True,
)
@click.option(
    "--model_specs_yaml",
    help="Path to model specs YAML file",
    default="config/mdb_models.yml",
    type=click.Path(exists=True, dir_okay=False, file_okay=True),
)
def main(affected_models_json: str, model_specs_yaml: str) -> None:
    """Filter models to only those in datahub."""
    model_specs = load_model_specs_from_yaml(Path(model_specs_yaml))
    # Parse the JSON string
    affected_models_list = json.loads(affected_models_json)

    filtered_models = [
        model
        for model in affected_models_list
        if model["model"] in model_specs
        and model_specs[model["model"]].get("in_data_hub", False)
    ]
    # Print the filtered models JSON to stdout
    print(json.dumps(filtered_models))  # noqa: T201


if __name__ == "__main__":
    main()
