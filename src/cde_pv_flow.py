"""Contains functions to work with CDEs that annotate CRDC data model entities."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import yaml
from bento_mdf.mdf import MDF
from prefect import flow, task
from pytz import timezone

from clients import CADSRClient, NCItClient

if TYPE_CHECKING:
    from bento_meta.model import Model

    from datatypes import ModelCDESpec, ModelSpec


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


@task
def load_model_specs_from_yaml(yaml_file: Path) -> list[ModelSpec]:
    """Load model specs from YAML file."""
    with Path(yaml_file).open(mode="r", encoding="utf-8") as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as exc:
            msg = f"Error parsing YAML file {yaml_file}: {exc}"
            raise ValueError(msg) from exc


@task
def make_model(
    model_spec: ModelSpec,
) -> Model:
    """Get a Model obejct from CRDC model spec."""
    mdf = MDF(*model_spec["yaml_file_names"], handle=model_spec["handle"])
    mdf.model.version = model_spec["version"]
    return mdf.model


@task
def count_model_cdes(model: Model) -> int:
    """Count CDEs in a model."""
    count = 0
    for term_key in model.terms:
        if "cadsr" not in term_key[1].lower():
            count += 1
    return count


@task
def make_model_cde_spec(model: Model) -> ModelCDESpec:
    """Get CDEs from a bento-meta model."""
    cde_spec: ModelCDESpec = {
        "handle": str(model.handle),
        "version": str(model.version),
        "annotations": [],
    }
    for entity_type in ["nodes", "edges", "props"]:
        model_entities = getattr(model, entity_type)
        for entity_key, entity in model_entities.items():
            if not entity.concept or not entity.concept.terms:
                continue
            for term_key, term in entity.concept.terms.items():
                # if 'caDSR' not in origin name, not a CDE
                if "cadsr" not in term_key[1].lower():
                    continue
                cde_spec["annotations"].append(
                    {
                        "entity": {
                            "key": entity_key,
                            "attrs": entity.get_attr_dict(),
                            "entity_has_enum": bool(entity.value_set),
                        },
                        "annotation": {
                            "key": term_key,
                            "attrs": term.get_attr_dict(),
                        },
                        "value_set": [],
                    },
                )
    return cde_spec


@task
def save_cde_spec_to_yaml(cde_spec: ModelCDESpec, yaml_file: Path) -> None:
    """Save CDE spec to YAML file."""
    with Path(yaml_file).open(mode="w", encoding="utf-8") as f:
        yaml.dump(cde_spec, f)


@task
def add_cde_pvs_to_model_cde_spec(
    cde_spec: ModelCDESpec,
    cadsr_client: CADSRClient,
) -> None:
    """Add CDE PVs to a ModelCDESpec."""
    logging.info("Getting CDE value sets from caDSR...")
    for annotation in cde_spec["annotations"]:
        if (
            annotation["entity"]["attrs"]["value_domain"] != "value_set"
            and not annotation["entity"]["entity_has_enum"]
        ):
            continue
        cde_id = annotation["annotation"]["attrs"].get("origin_id")
        cde_version = annotation["annotation"]["attrs"].get("origin_version")
        entity_key = str(annotation["entity"]["key"])
        value_set = cadsr_client.fetch_cde_valueset(
            cde_id,
            cde_version,
            entity_key,
        )
        if not value_set:
            continue
        annotation["value_set"] = value_set


@task
def add_ncit_synonyms_to_model_cde_spec(
    cde_spec: ModelCDESpec,
    ncit_client: NCItClient,
) -> None:
    """Add NCIt synonyms to a ModelCDESpec."""
    logging.info("Getting synonyms from NCIt...")
    for annotation in cde_spec["annotations"]:
        if (
            annotation["entity"]["attrs"]["value_domain"] != "value_set"
            and not annotation["entity"]["entity_has_enum"]
        ):
            continue
        value_set = annotation.get("value_set")
        if not value_set:
            continue
        for pv in value_set:
            ncit_concept_codes = pv["ncit_concept_codes"]
            for code in ncit_concept_codes:
                if code and code in ncit_client.ncim_mapping:
                    syn_dicts = ncit_client.ncim_mapping[code]
                    for syn_attrs in syn_dicts:
                        pv["synonyms"].append(syn_attrs)

        annotation["value_set"] = value_set


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
