"""Affter updating database or mappings, update data hub JSON files."""

import json
import os
from pathlib import Path

import click
import dotenv
from bento_meta.mdb.mdb import MDB
from prefect import flow

dotenv.load_dotenv(Path("config/.env"), override=True)

QUERY = (
    "MATCH (n {model: $dataCommons, version: $version})-[:has_property]->"
    "(p:property) "
    "WITH collect(p) AS props "
    "UNWIND props AS prop "
    "OPTIONAL MATCH (prop)-[:has_concept]->(c:concept)<-[:represents]-"
    "(cde:term) WHERE toLower(cde.origin_name) CONTAINS 'cadsr' "
    "OPTIONAL MATCH (prop)-[:has_value_set]->(:value_set)-[:has_term]->(t:term)"
    " WITH prop, cde.origin_id AS CDECode, cde.origin_version AS CDEVersion, "
    "cde.value AS CDEFullName, cde.origin_id + '|' + cde.origin_version "
    "AS cde_hdl, collect(t) AS model_pvs, "
    "CASE WHEN cde IS NOT NULL THEN true ELSE false END AS has_cde "
    "OPTIONAL MATCH (v:value_set {handle: cde_hdl})-[:has_term]->(cde_pv:term) "
    "WITH prop, CDECode, CDEVersion, CDEFullName, model_pvs, has_cde, "
    "collect(cde_pv) AS cde_pvs WITH prop, CDECode, CDEVersion, CDEFullName, "
    "model_pvs, has_cde, cde_pvs, CASE WHEN has_cde AND size(cde_pvs) > 0 "
    "AND NONE(p in cde_pvs WHERE p.value =~ 'https?://.*') THEN cde_pvs "
    "WHEN has_cde and size(cde_pvs) > 0 "
    "AND ANY(p in cde_pvs WHERE p.value =~ 'https?://.*') "
    "AND size(model_pvs) > 0 THEN model_pvs "
    "WHEN NOT has_cde AND size(model_pvs) > 0 THEN model_pvs "
    "ELSE [null] END AS pvs "
    "UNWIND pvs AS pv "
    "OPTIONAL MATCH (pv)-[:represents]->(c:concept)<-[:represents]-"
    "(syn:term), (c)-[:has_tag]->(g:tag {key: 'mapping_source', value: 'NCIm'})"
    " WHERE pv <> syn and pv.value <> syn.value "
    "WITH prop, CDECode, CDEVersion, CDEFullName, model_pvs, "
    "pv.value AS pv_val, collect(DISTINCT syn.value) AS syn_vals "
    "WITH prop, CDECode, CDEVersion, CDEFullName, model_pvs, "
    "collect({value: pv_val, synonyms: syn_vals}) AS formatted_pvs "
    "RETURN $dataCommons AS dataCommons, $version AS version, "
    "prop AS property, CDECode, CDEVersion, CDEFullName, "
    "formatted_pvs AS permissibleValues"
)


@flow(name="get-pvs-json")
def get_pvs_json(
    mdb_uri: str, mdb_user: str, mdb_pass: str, model: str, version: str
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
