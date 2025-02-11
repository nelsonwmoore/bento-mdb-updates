"""Check MDF repos for MDFs not in MDB. If found, add to models yaml."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

from dotenv import load_dotenv
from packaging.version import Version
from packaging.version import parse as parse_version

from bento_mdb_updates.clients import GitHubClient
from bento_mdb_updates.model_cdes import load_model_specs_from_yaml

if TYPE_CHECKING:
    from bento_mdb_updates.datatypes import ModelSpec


load_dotenv(Path("config/.env"))


def normalize_tag_version(tag: str) -> str:
    """
    Extract a semantic version string (e.g., "2.1.0") from a tag.

    Returns an tag as-is if no valid semantic version is found.
    """
    match = re.search(r"(\d+\.\d+\.\d+)", tag)
    if match:
        return match.group(1)
    logging.warning("No semantic version found in tag %s", tag)
    return tag


def update_model_versions(
    model_specs: dict[str, ModelSpec],
    github_client: GitHubClient,
    *,
    new_only: bool = True,
) -> bool:
    """Update ModelSpec with missing version tags from GitHub."""
    updated = False
    for model, spec in model_specs.items():
        logging.info("Checking %s...", model)
        repo = spec.get("repository")
        if not repo:
            logging.warning("No repository specified for %s", model)
            continue
        raw_tags = github_client.get_repo_tags(repo)
        normalized_tags = [normalize_tag_version(t) for t in raw_tags]
        current_versions = spec.get("versions")
        current_latest_version = Version(spec.get("latest_version", "0.0.0"))

        for tag in normalized_tags:
            tag_version = Version(tag)
            if new_only and tag_version <= current_latest_version:
                logging.info(
                    "Skipping %s, version %s is not newer than latest version %s",
                )
                continue
            if tag not in current_versions:
                logging.info("Adding %s to versions for %s", tag, model)
                current_versions.append(tag)
                updated = True

        if current_versions:
            sorted_versions = sorted(current_versions, key=parse_version)
            spec["versions"] = sorted_versions
            spec["latest_version"] = sorted_versions[-1]
    return updated


def main() -> None:
    """Do stuff."""
    github_client = GitHubClient()
    model_specs = load_model_specs_from_yaml(Path("config/mdb_models.yml"))
    update_model_versions(model_specs, github_client, new_only=True)


if __name__ == "__main__":
    main()
