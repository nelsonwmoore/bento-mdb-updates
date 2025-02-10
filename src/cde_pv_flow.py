"""Script to process CDEs that annotate CRDC data model entities."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from prefect import flow
from pytz import timezone

from clients import CADSRClient, NCItClient
from model_cde_utils import (
    add_cde_pvs_to_model_cde_spec,
    add_ncit_synonyms_to_model_cde_spec,
    count_model_cdes,
    load_model_specs_from_yaml,
    make_model,
    make_model_cde_spec,
    save_cde_spec_to_yaml,
)

LOG_FILE = (
    Path().cwd()
    / "logs"
    / f"cdepv_syns_{datetime.now(tz=timezone('UTC')).strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


@flow
def main() -> None:
    """Do stuff."""
    crdc_models_yml = Path().cwd() / "src/crdc_models.yml"
    model_specs = load_model_specs_from_yaml(crdc_models_yml)
    ncit_client = NCItClient(Path("src/NCIt_Metathesaurus_Mapping_202408.txt"))
    cadsr_client = CADSRClient()

    for spec in model_specs:
        # get CDEs from model files
        logging.info(
            "Getting CDEs from %s v%s MDFs...",
            spec["handle"],
            spec["version"],
        )
        model = make_model(spec)
        (f"{model.handle} v{model.version} has {count_model_cdes(model)} CDEs.")
        model_cde_spec = make_model_cde_spec(model)

        add_cde_pvs_to_model_cde_spec(model_cde_spec, cadsr_client)
        add_ncit_synonyms_to_model_cde_spec(model_cde_spec, ncit_client)

        # save cde spec to yaml
        output_dir = Path().cwd() / "output/model_cdes"
        model_cdes_yml = (
            output_dir / model.handle / f"{model.handle}_{model.version}_cdes.yml"
        )

        save_cde_spec_to_yaml(model_cde_spec, model_cdes_yml)


if __name__ == "__main__":
    main()
