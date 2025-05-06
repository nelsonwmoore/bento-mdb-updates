"""Affter updating database or mappings, update data hub JSON files."""

import json
import os
from pathlib import Path

import click
import dotenv
from bento_meta.mdb.mdb import MDB
from prefect import flow

dotenv.load_dotenv(Path("config/.env"), override=True)

# TODO: mirror bento-sts.mdb.mdb.get_all_pvs_and_synonyms until importable
QUERY = (
    "MATCH (cde:term) WHERE toLower(cde.origin_name) CONTAINS 'cadsr' WITH cde "
    "OPTIONAL MATCH (ent)-[:has_property]->(p:property)-[:has_concept]->"
    "(:concept)<-[:represents]-(cde) WHERE p.model IS NOT NULL AND p.version "
    "IS NOT NULL WITH cde,COLLECT(DISTINCT {model: p.model, version: p.version,"
    " property: ent.handle + '.' + p.handle}) AS models "
    "WITH cde, models, cde.origin_id + '|' + COALESCE(cde.origin_version, '') "
    "AS cde_hdl "
    "OPTIONAL MATCH (prop: property)-[:has_concept]->(c:concept)<-[:represents]"
    "-(cde) OPTIONAL MATCH (prop)-[:has_value_set]->(:value_set)-[:has_term]->"
    "(model_pv:term) WITH cde, models, cde_hdl, COLLECT(DISTINCT model_pv) AS "
    "model_pvs OPTIONAL MATCH (vs:value_set {handle: cde_hdl})-[:has_term]->"
    "(cde_pv:term) WITH cde, models, model_pvs, COLLECT(DISTINCT cde_pv) AS "
    "cde_pvs WITH cde, models, model_pvs, cde_pvs, CASE WHEN size(cde_pvs) > 0 "
    "AND NONE(p in cde_pvs WHERE p.value =~ 'https?://.*') THEN cde_pvs "
    "WHEN size(cde_pvs) > 0 AND ANY(p in cde_pvs WHERE p.value =~ "
    "'https?://.*') AND size(model_pvs) > 0 THEN model_pvs "
    "WHEN size(model_pvs) > 0 THEN model_pvs ELSE [] END AS pvs "
    "WHERE size(pvs) > 0 UNWIND pvs AS pv "
    "OPTIONAL MATCH (pv)-[:represents]->(c_cadsr:concept)<-[:represents]-"
    "(ncit_term:term {origin_name: 'NCIt'}), "
    "(c_cadsr)-[:has_tag]->(:tag {key: 'mapping_source', value: 'caDSR'}) "
    "OPTIONAL MATCH (ncit_term)-[:represents]->(c_ncim:concept)<-[:represents]"
    "-(syn:term), "
    "(c_ncim)-[:has_tag]->(:tag {key: 'mapping_source', value: 'NCIm'}) "
    "WHERE pv <> syn and pv.value <> syn.value "
    "WITH cde, pv, models, pv.value as pv_val, ncit_term.origin_id AS ncit_oid,"
    " ncit_term.value AS ncit_value, COLLECT(DISTINCT syn.value) "
    "AS distinct_syn_vals WITH cde, models, pv_val, ncit_oid, "
    "CASE WHEN ncit_value IS NOT NULL THEN distinct_syn_vals + [ncit_value] "
    "ELSE distinct_syn_vals END AS syn_vals "
    "WITH cde, models, COLLECT({value: pv_val, synonyms: syn_vals, "
    "ncit_concept_code: ncit_oid}) AS formatted_pvs "
    "RETURN cde.origin_id AS CDECode, cde.origin_version AS CDEVersion, "
    "cde.value AS CDEFullName, models, formatted_pvs AS permissibleValues "
)


@flow(name="get-pvs-json")
def get_pvs_json(
    mdb_uri: str,
    mdb_user: str,
    mdb_pass: str,
    model: str,
    version: str,
) -> None:
    """Get JSON from MDB with CDE PVs and Synonyms in Data Hub format."""
    mdb = MDB(
        uri=mdb_uri or os.environ.get("NEO4J_MDB_URI"),
        user=mdb_user or os.environ.get("NEO4J_MDB_USER"),
        password=mdb_pass or os.environ.get("NEO4J_MDB_PASS"),
    )
    parms = {"dataCommons": model, "version": version}
    result = mdb.get_with_statement(QUERY, parms)
    processed = [
        {**item, "property": item.get("property", {}).get("handle", "")}
        for item in result
    ]
    print(json.dumps(processed, indent=2))  # noqa: T201


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
    "-m",
    "--model",
    required=True,
    type=str,
    prompt=True,
    help="CRDC Model Handle (e.g. 'GDC')",
)
@click.option(
    "-v",
    "--version",
    required=True,
    type=str,
    prompt=True,
    help="CRDC Model Version (e.g. '1.2.3')",
)
def main(mdb_uri: str, mdb_user: str, mdb_pass: str, model: str, version: str) -> None:
    """Get JSON from MDB with CDE PVs and Synonyms in Data Hub format."""
    get_pvs_json(
        mdb_uri=mdb_uri,
        mdb_user=mdb_user,
        mdb_pass=mdb_pass,
        model=model,
        version=version,
    )


if __name__ == "__main__":
    main()
