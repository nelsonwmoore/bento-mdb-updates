"""
Renumber changeset ids for all changesets in changelog.

Starts with n and increments by 1. Saves the resulting changelog at the given path.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

NAMESPACE = "http://www.liquibase.org/xml/ns/dbchangelog"
XSI_NAMESPACE = "http://www.w3.org/2001/XMLSchema-instance"
NEO4J_NAMESPACE = "http://www.liquibase.org/xml/ns/dbchangelog-ext"
SCHEMA_LOCATION = f"{NAMESPACE} {NAMESPACE}/dbchangelog-latest.xsd"


def renumber_changelog_id(
    file_path: str | Path,
    starting_id: int,
    new_file_path: str | Path | None = None,
) -> None:
    """Load xml changelog & renumber ids for all changesets."""
    id_num = starting_id
    if not new_file_path:
        new_file_path = file_path

    tree = ET.parse(file_path)
    root = tree.getroot()

    ET.register_namespace("", NAMESPACE)
    ET.register_namespace("xsi", XSI_NAMESPACE)
    ET.register_namespace("neo4j", NEO4J_NAMESPACE)

    for child in root:
        child.attrib["id"] = str(id_num)
        id_num += 1

    tree.write(
        file_or_filename=new_file_path,
        xml_declaration=True,
        method="xml",
    )

    print(
        f"\nRenumbered changelog ids for {file_path} starting with {starting_id}"
        f" and ending with {id_num - 1}. Next id will be {id_num}",
    )


if __name__ == "__main__":
    PATH_TO_CHANGELOG = Path(
        "C:/dev/projects/CBIIT/bento-mdb/changelogs/models/"
        "CDS/CDS_6.0.0_cde_changelog.xml",
    )
    STARTING_ID = 1

    renumber_changelog_id(
        file_path=PATH_TO_CHANGELOG,
        starting_id=STARTING_ID,
        new_file_path=PATH_TO_CHANGELOG,
    )
