"""Convert cdes yaml to neo4j cypher statements."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import yaml
from bento_meta.mdb.mdb import make_nanoid
from bento_meta.objects import Concept, Term, ValueSet
from liquichange.changelog import Changelog, Changeset, CypherChange
from minicypher.clauses import Match, Merge, OnCreateSet
from minicypher.entities import N, R, T
from tqdm import tqdm

from bento_mdb_updates.changelog_utils import Statement, escape_quotes_in_attr
from cde_pv_flow import load_model_specs_from_yaml

if TYPE_CHECKING:
    from datatypes import AnnotationSpec, ModelCDESpec, ModelSpec

if TYPE_CHECKING:
    from bento_meta.entity import Entity
_COMMIT = f"CDEPV-{datetime.today().strftime('%Y%m%d')}"
AUTHOR = "NWM"
BASE_OUTPUT_PATH = Path().cwd() / "output" / "changelogs"


def load_cdes_from_model_spec(spec: ModelSpec) -> ModelCDESpec:
    """Load model cdes from spec."""
    path = (
        Path().cwd()
        / "output/model_cdes"
        / spec["handle"]
        / f"{spec['handle']}_{spec['version']}_cdes.yml"
    )
    with path.open(mode="r", encoding="utf-8") as f:
        try:
            return yaml.load(f, Loader=yaml.FullLoader)  # noqa: S506
        except yaml.YAMLError as exc:
            msg = f"Error parsing YAML file {path}: {exc}"
            raise ValueError(msg) from exc


def cypherize_entity(entity: Entity) -> N:
    """Represent metamodel Entity object as a property graph Node."""
    return N(label=entity.get_label(), props=entity.get_attr_dict())


def convert_annotation_to_changesets(
    annotation: AnnotationSpec,
    changeset_id: int,
) -> list[Changeset]:
    """Convert annotation to list of Liquibase Changesets."""
    # only care about value set annotations, others already added to neo4j
    if (
        annotation["entity"]["attrs"]["value_domain"] != "value_set"
        and not annotation["entity"]["entity_has_enum"]
    ):
        return []
    statements = []
    changesets = []
    cde_attrs = annotation["annotation"]["attrs"]
    base_url = "https://cadsrapi.cancer.gov/rad/NCIAPI/1.0/api/DataElement/"
    cde_id = cde_attrs.get("origin_id", "")
    cde_ver = cde_attrs.get("origin_version", "")
    cde_vs = ValueSet(
        {
            "url": f"{base_url}{cde_id}{f'?version={cde_ver}' if cde_ver else ''}",
            "handle": f"{cde_id}|{cde_ver}",
            "_commit": _COMMIT,
        },
    )
    cypher_cde_vs = cypherize_entity(cde_vs)
    vs_commit = cypher_cde_vs.props.pop("_commit")
    statements.append(Statement(Merge(cypher_cde_vs), OnCreateSet(vs_commit)))
    # load entities from annotation
    # need terms for each pv in value set and synonyms for each of those pvs
    for pv in tqdm(
        annotation["value_set"],
        desc="PVs",
        total=len(annotation["value_set"]),
    ):
        synonyms = pv.pop("synonyms")  # separate synonyms dict from pv attrs
        pv_term = Term(pv)
        pv_term._commit = _COMMIT  # noqa: SLF001
        escape_quotes_in_attr(pv_term)
        cypher_pv_term = cypherize_entity(pv_term)
        term_commit = cypher_pv_term.props.pop("_commit")
        statements.append(Statement(Merge(cypher_pv_term), OnCreateSet(term_commit)))

        # add relationship between cde value set & pv term(s)
        cypher_rel = R(Type="has_term")
        stmt_merge_trip = T(
            cypher_cde_vs.plain_var(),
            cypher_rel,
            cypher_pv_term.plain_var(),
        )
        statements.append(
            Statement(Match(cypher_cde_vs, cypher_pv_term), Merge(stmt_merge_trip)),
        )
        # make concept for pv & its synonyms
        concept = Concept({"_commit": _COMMIT, "nanoid": make_nanoid()})
        cypher_concept = cypherize_entity(concept)
        concept_commit = cypher_concept.props.pop("_commit")

        # create statements for synonyms
        if synonyms:
            # add statement to create concept and link pv term to concept
            statements.append(
                Statement(Merge(cypher_concept), OnCreateSet(concept_commit)),
            )
            pv_concept_cypher_rel = R(Type="represents")
            pv_stmt_merge_trip = T(
                cypher_pv_term.plain_var(),
                pv_concept_cypher_rel,
                cypher_concept.plain_var(),
            )
            statements.append(
                Statement(
                    Match(cypher_pv_term, cypher_concept),
                    Merge(pv_stmt_merge_trip),
                ),
            )
            for syn_attrs in synonyms:
                syn_term = Term(syn_attrs)
                syn_term._commit = _COMMIT  # noqa: SLF001
                escape_quotes_in_attr(syn_term)
                cypher_syn_term = cypherize_entity(syn_term)
                syn_commit = cypher_syn_term.props.pop("_commit")
                # add synonym term
                statements.append(
                    Statement(Merge(cypher_syn_term), OnCreateSet(syn_commit)),
                )
                # link synonym term to pv term via concept
                syn_cypher_rel = R(Type="represents")
                syn_stmt_merge_trip = T(
                    cypher_syn_term.plain_var(),
                    syn_cypher_rel,
                    cypher_concept.plain_var(),
                )
                statements.append(
                    Statement(
                        Match(cypher_syn_term, cypher_concept),
                        Merge(syn_stmt_merge_trip),
                    ),
                )

    # create changesets for each statement
    cs_id = changeset_id
    for stmt in statements:
        changesets.append(
            Changeset(
                id=str(cs_id),
                author=AUTHOR,
                change_type=CypherChange(text=str(stmt)),
            ),
        )
        cs_id += 1

    del statements  # garbage collection
    return changesets


def convert_model_cdes_to_changelog(model_cdes: ModelCDESpec) -> Changelog:
    """Convert model cde annotations with PVs and synonyms to Liquibase Changelog."""
    changelog = Changelog()
    changeset_id = 1
    for annotation in tqdm(model_cdes["annotations"], desc="Annotations"):
        print(f"Annotation: {annotation['entity']['key']}")
        changesets = convert_annotation_to_changesets(annotation, changeset_id)
        if not changesets:
            continue
        for changeset in tqdm(changesets, desc="Changesets", total=len(changesets)):
            changelog.add_changeset(changeset)
        del changesets  # garbage collection
    return changelog


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
