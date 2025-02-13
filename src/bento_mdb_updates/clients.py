"""API clients for Data Hub terms."""

from __future__ import annotations

import csv
import logging
import os
import pickle
import re
import subprocess
from json import JSONDecodeError
from pathlib import Path
from typing import TYPE_CHECKING

import requests

if TYPE_CHECKING:
    from bento_mdb_updates.datatypes import PermissibleValue

CADSR_API_CACHE = Path().cwd() / "cadsr_api_cache.pkl"
RESPONSE_200 = 200


class CADSRClient:
    """Client for caDSR II API."""

    def __init__(self, cache: dict | None = None) -> None:
        """Initialize client."""
        self.cache = cache

    def get_valueset_from_json(
        self,
        json_response: dict,
    ) -> list[PermissibleValue | None]:
        """Get value set from JSON response."""
        try:
            vs = []
            for pv in json_response["DataElement"]["ValueDomain"]["PermissibleValues"]:
                pv_dict = {
                    "value": pv["value"],
                    "origin_version": pv["ValueMeaning"]["version"],
                    "origin_id": pv["ValueMeaning"]["publicId"],
                    "origin_definition": pv["ValueMeaning"]["definition"],
                    "origin_name": "caDSR",
                    "ncit_concept_codes": [],
                    "synonyms": [],
                }
                for concept in pv["ValueMeaning"]["Concepts"]:
                    if concept.get("evsSource") != "NCI_CONCEPT_CODE":
                        continue
                    pv_dict["ncit_concept_codes"].append(concept["conceptCode"])
                    pv_dict["synonyms"].append(
                        {
                            "value": concept["longName"],
                            "origin_id": concept["conceptCode"],
                            "origin_definition": concept["definition"],
                            "origin_name": "NCIt",
                        },
                    )
                vs.append(pv_dict)
        except Exception as e:
            msg = f"Exception occurred when getting value set from JSON: {e}\n{json_response}"
            logging.exception(msg)
            return []
        else:
            return vs

    def fetch_cde_valueset(
        self,
        cde_id: str,
        cde_version: str | None = None,
        entity_key: str | None = None,
    ) -> list[PermissibleValue | None]:
        """Fetch CDE value set from caDSR II API."""
        ver_str = (
            f"?version={cde_version}"
            if cde_version and re.match(r"^v?\d{1,3}(\.\d{1,3}){0,2}$", cde_version)
            else ""
        )
        cde_id_ver_str = f"{cde_id}{ver_str}"
        if self.cache and cde_id_ver_str in self.cache:
            return self.cache[cde_id_ver_str]
        url = f"https://cadsrapi.cancer.gov/rad/NCIAPI/1.0/api/DataElement/{cde_id_ver_str}"
        headers = {"accept": "application/json"}
        try:
            response = requests.get(url, timeout=60, headers=headers)
            response.raise_for_status()
            json_response = response.json()
            value_set = self.get_valueset_from_json(json_response)
            if self.cache:
                self.cache[cde_id_ver_str] = value_set
        except requests.RequestException as e:
            msg = f"HTTP request for entity {entity_key} failed: {e}\nurl: {url}"
            logging.exception(msg)
            return []
        except JSONDecodeError as e:
            msg = f"Failed to parse JSON response for entity {entity_key}: {e}\nurl: {url}"
            logging.exception(msg)
            return []
        else:
            return value_set

    def load_cadsr_api_cache(self) -> dict:
        """Load cached api responses from pickle file."""
        try:
            with CADSR_API_CACHE.open("rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return {}

    def save_cadsr_api_cache(self) -> None:
        """Save cached api responses to pickle file."""
        with CADSR_API_CACHE.open("wb") as f:
            pickle.dump(self.cache, f)


class NCItClient:
    """Client for NCIt API."""

    DEFAULT_NCIM_TSV = (
        Path().cwd() / "data/source/NCIt/NCIt_Metathesaurus_Mapping_202408.txt"
    )

    def __init__(self, ncim_tsv: Path | None = None) -> None:
        """Initialize client."""
        if not ncim_tsv:
            ncim_tsv = self.DEFAULT_NCIM_TSV
        self.ncim_mapping: dict = self.load_ncim_tsv_to_dict(ncim_tsv)

    def load_ncim_tsv_to_dict(self, ncim_tsv: Path | None = None) -> dict:
        """Load NCIm TSV file to dict."""
        if not ncim_tsv:
            return {}
        ncim = {}
        with ncim_tsv.open(mode="r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter="\t")
            for row in reader:
                nci_code = row[2]
                syn_attrs = {
                    "origin_id": row[4],
                    "origin_name": row[6],
                    "origin_version": row[7],
                    "value": row[5],
                }
                if nci_code in ncim:
                    ncim[nci_code].append(syn_attrs)
                else:
                    ncim[nci_code] = [syn_attrs]
        return ncim


class GitHubClient:
    """Client to interact with GitHub API."""

    BASE_URL = "https://api.github.com"

    def __init__(self, github_token: str | None = None) -> None:
        """Initialize client."""
        self.github_token = github_token if github_token else os.environ["GITHUB_TOKEN"]

    def get_repo_tags(self, repo: str) -> list[str]:
        """Query GitHub API for tags on a given repository."""
        url = f"{self.BASE_URL}/repos/{repo}/tags"
        headers = {"Authorization": f"token {self.github_token}"}
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != RESPONSE_200:
            msg = f"Failed to get tags for repo {repo}: {response.status_code}"
            logging.error(msg)
            return []
        tags = response.json()
        return [tag["name"] for tag in tags]

    def commit_and_push_changes(
        self,
        file_to_commit: Path,
        commit_msg: str | None = None,
    ) -> None:
        """Commit and push changes to repo."""
        try:
            subprocess.run(["git", "add", str(file_to_commit)], check=True)
            commit_msg = commit_msg or f"Update {file_to_commit.name}"
            subprocess.run(["git", "commit", "-m", commit_msg], check=True)
            subprocess.run(["git", "push"], check=True)
            logging.info("Changes committed and pushed successfully.")
        except subprocess.CalledProcessError:
            logging.exception("Failed to add %s to git", file_to_commit.name)
            return
