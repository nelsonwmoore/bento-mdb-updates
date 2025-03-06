#!/usr/bin/env python3
"""Check for new caDSR PVs and NCIT mappings and generate Cypher to update MDB."""

from __future__ import annotations

import datetime
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

from bento_meta.mdb.mdb import MDB
from dotenv import load_dotenv

from bento_mdb_updates.changelogs import convert_model_cdes_to_changelog
from bento_mdb_updates.clients import CADSRClient, NCItClient
from bento_mdb_updates.model_cdes import (
    add_ncit_synonyms_to_model_cde_spec,
    get_cdes_from_mdb,
)

if TYPE_CHECKING:
    from bento_mdb_updates.datatypes import ModelCDESpec

load_dotenv(Path("config/.env"), override=True)

today = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y%m%d")
OUTPUT_FILE = Path(
    f"data/output/mdb_cdes/mdb_cdes_{today}.json",
)


def main(
    author: str,
    _commit: str | None = None,
) -> None:
    """Check for new CDE PVs and update the database."""
    # Get current MDB CDE Pvs & Synonyms
    mdb = MDB(
        uri=os.environ.get("NEO4J_MDB_URI"),
        user=os.environ.get("NEO4J_MDB_USER"),
        password=os.environ.get("NEO4J_MDB_PASS"),
    )
    mdb_cdes = get_cdes_from_mdb(mdb)
    update_cde_spec: ModelCDESpec = {
        "handle": "TERM_UPDATES",
        "version": today,
        "annotations": [],
    }

    # TEST - remove a PV to see if it gets picked up
    print(mdb_cdes[0]["permissibleValues"][0])
    mdb_cdes[0]["permissibleValues"][0] = {"synonyms": [], "value": ""}

    # Check caDSR for new PVs
    print(update_cde_spec["annotations"])
    print("Checking caDSR for new PVs...")
    cadsr_client = CADSRClient()
    update_cde_spec["annotations"].extend(cadsr_client.check_cdes_against_mdb(mdb_cdes))

    # get NCIt synonyms for new PVs
    print(update_cde_spec["annotations"])
    print("Getting NCIt synonyms for new PVs...")
    ncit_client = NCItClient()
    add_ncit_synonyms_to_model_cde_spec(update_cde_spec, ncit_client)

    # TEST - remove PV synonyms to see if it gets picked up
    mdb_cdes[0]["permissibleValues"][3]["synonyms"][0] = {}

    # check NCIt for new PV synonyms
    if ncit_client.check_ncit_for_updated_mappings():
        print("Checking NCIt for new PV synonyms...")
        update_cde_spec["annotations"].extend(
            ncit_client.check_synonyms_against_mdb(mdb_cdes),
        )

    # convert annotation updates to liquibase changelog
    changelog = convert_model_cdes_to_changelog(update_cde_spec, author, _commit)
    output_dir = Path().cwd() / "data/output/term_changelogs"
    changelog_file = output_dir / f"{today}_term_updates.xml"
    changelog.save_to_file(str(changelog_file), encoding="UTF-8")

    # Update mdb_cdes JSON file
    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(mdb_cdes, f, indent=2)


if __name__ == "__main__":
    main(author="TEST", _commit="TEST-COMMIT")  # pylint: disable=no-value-for-parameters
