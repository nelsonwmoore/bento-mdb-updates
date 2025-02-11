"""Convert cdes yaml to neo4j cypher statements."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from tqdm import tqdm

from bento_mdb_updates.changelogs import convert_model_cdes_to_changelog
from bento_mdb_updates.model_cdes import (
    load_cdes_from_model_spec,
    load_model_specs_from_yaml,
)

_COMMIT = f"CDEPV-{datetime.today().strftime('%Y%m%d')}"
AUTHOR = "NWM"
BASE_OUTPUT_PATH = Path().cwd() / "output" / "changelogs"


def main() -> None:
    """Do stuff."""
    crdc_models_yml = Path().cwd() / "src/crdc_models.yml"
    model_specs = load_model_specs_from_yaml(crdc_models_yml)
    for spec in tqdm(model_specs, desc="Model specs", total=len(model_specs)):
        model_cdes = load_cdes_from_model_spec(spec)
        changelog = convert_model_cdes_to_changelog(model_cdes)
        output_path = (
            BASE_OUTPUT_PATH / f"{spec['handle']}_{spec['version']}_cde_changelog.xml"
        )
        changelog.save_to_file(str(output_path), encoding="UTF-8")


if __name__ == "__main__":
    main()
