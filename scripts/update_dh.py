"""Generate CDE PV & Synonym JSON and update Data Hub terms repo."""

import json

import click
from bento_meta.mdb.mdb import MDB
from github import Github, GithubException
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret

VALID_MDB_IDS = [
    "fnl-mdb-dev",
    "cloud-one-mdb-dev",
    "og-mdb-dev",
    "og-mdb-nightly",
    "og-mdb-prod",
]
VALID_TIERS = {
    "lower": ["dev", "dev2", "qa", "qa2"],
    "upper": ["stage", "prod"],
}

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


@task
def get_pvs_json(
    mdb_uri: str,
    mdb_user: str,
    mdb_id: str,
) -> str:
    """Get JSON from MDB with CDE PVs and Synonyms in Data Hub format."""
    if mdb_id not in VALID_MDB_IDS:
        msg = f"Invalid MDB ID: {mdb_id}. Valid IDs: {VALID_MDB_IDS}"
        raise ValueError(msg)
    pwd_secret_name = mdb_id + "-pwd"
    password = Secret.load(pwd_secret_name).get()  # type: ignore reportAttributeAccessIssue
    if mdb_id.startswith("og-mdb"):
        password = ""
    mdb = MDB(
        uri=mdb_uri,
        user=mdb_user,
        password=password,
    )
    result = mdb.get_with_statement(QUERY)
    return json.dumps(result, indent=2)


@task
def update_datahub_terms(
    pvs_json: str,
    tier: str,
) -> list:
    """Update crdc-datahub-terms repo with new CDE JSON."""
    logger = get_run_logger()
    if tier not in VALID_TIERS:
        msg = f"Invalid tier: {tier}. Valid tiers: {VALID_TIERS}"
        raise ValueError(msg)
    branches_to_update = VALID_TIERS[tier]

    github_token = Secret.load("GITHUB_TOKEN").get()  # type: ignore reportAttributeAccessIssue
    gh = Github(github_token)
    repo = gh.get_repo("CBIIT/crdc-datahub-terms")
    file_path = "mdb_pvs.json"

    results = []
    for branch in branches_to_update:
        try:
            try:
                file_contents = repo.get_contents(file_path, ref=branch)
                sha = (
                    file_contents[0].sha
                    if isinstance(file_contents, list)
                    else file_contents.sha
                )
                logger.info("File %s already exists in branch %s", file_path, branch)
                commit_msg = (
                    f"Update CDE PVs and synonyms from STS for {branch} branch."
                )
                result = repo.update_file(
                    path=file_path,
                    message=commit_msg,
                    content=pvs_json,
                    sha=sha,
                    branch=branch,
                )
                results.append(
                    f"Updated {file_path} in {branch} branch "
                    f"(commit: {result['commit'].sha[:7]})",
                )
            except GithubException:
                logger.info("File %s does not exist in branch %s", file_path, branch)
                commit_msg = f"Add CDE PVs and synonyms from STS for {branch} branch."
                result = repo.create_file(
                    path=file_path,
                    message=commit_msg,
                    content=pvs_json,
                    branch=branch,
                )
                results.append(
                    f"Created {file_path} in {branch} branch "
                    f"(commit: {result['commit'].sha[:7]})",
                )
        except Exception as e:  # noqa: PERF203
            error_msg = f"Error updating {file_path} in {branch} branch: {e}"
            results.append(error_msg)
            logger.exception(error_msg)

    for result in results:
        logger.info(result)

    if any("Error" in result for result in results):
        msg = "Some branches failed to update. See logs for details."
        raise RuntimeError(msg)

    return results


@flow(name="update-datahub")
def update_datahub_flow(
    mdb_uri: str,
    mdb_user: str,
    mdb_id: str,
    tier: str,
    *,
    no_commit: bool = True,
) -> None:
    """Generate CDE PV & Synonym JSON and update Data Hub terms repo."""
    logger = get_run_logger()
    pvs_json = get_pvs_json(
        mdb_uri=mdb_uri,
        mdb_user=mdb_user,
        mdb_id=mdb_id,
    )
    if not pvs_json:
        logger.error("No PVs JSON found. Exiting.")
        return
    if not no_commit:
        logger.info("Committing changes...")
        update_datahub_terms(
            pvs_json=pvs_json,
            tier=tier,
        )


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
    "--tier",
    required=True,
    type=str,
    prompt=True,
    help="Data Hub tier to update (lower or upper)",
)
@click.option(
    "--no_commit",
    type=bool,
    default=True,
    show_default=True,
    help="Don't commit changes",
)
def main(
    mdb_uri: str,
    mdb_user: str,
    mdb_id: str,
    tier: str,
    *,
    no_commit: bool = True,
) -> None:
    """Generate CDE PV & Synonym JSON and update Data Hub terms repo."""
    update_datahub_flow(
        mdb_uri=mdb_uri,
        mdb_user=mdb_user,
        mdb_id=mdb_id,
        tier=tier,
        no_commit=no_commit,
    )


if __name__ == "__main__":
    main()
