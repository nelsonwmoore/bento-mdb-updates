"""API clients for Data Hub terms."""

from __future__ import annotations

import csv
import datetime
import io
import logging
import os
import re
import subprocess
import zipfile
from json import JSONDecodeError
from pathlib import Path
from typing import TYPE_CHECKING

import requests
import stamina
import yaml
from tqdm import tqdm

if TYPE_CHECKING:
    from bento_mdb_updates.datatypes import AnnotationSpec, MDBCDESpec, PermissibleValue


RESPONSE_200 = 200
DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 3
DEFAULT_RETRY_DELAY = 1.0

SYNC_STATUS_YAML = Path("config/sync_status.yml")


def get_last_sync_date(
    source: str,
    yaml_path: Path = SYNC_STATUS_YAML,
) -> datetime.datetime:
    """Get last updated date from sync_status.yml."""
    if not yaml_path.exists():
        msg = f"File {yaml_path} does not exist."
        raise FileNotFoundError(msg)
    with yaml_path.open(mode="r", encoding="utf-8") as f:
        sync_status = yaml.safe_load(f)
    return datetime.datetime.strptime(
        sync_status[source]["last_updated"],
        sync_status[source]["date_format"],
    ).replace(tzinfo=datetime.timezone.utc)


class CADSRClient:
    """Client for caDSR II API."""

    def __init__(self) -> None:
        """Initialize client."""

    def get_valueset_from_json(
        self,
        json_response: dict,
    ) -> list[PermissibleValue | None]:
        """Get value set from JSON response."""
        try:
            vs = []
            cde_pvs = json_response["DataElement"]["ValueDomain"]["PermissibleValues"]
            if not cde_pvs:
                logging.warning(
                    "No permissible values found for CDE %s v%s",
                    json_response["DataElement"]["publicId"],
                    json_response["DataElement"]["version"],
                )
                return vs
            for pv in cde_pvs:
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

    @stamina.retry(on=requests.RequestException, attempts=DEFAULT_RETRIES)
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
        url = f"https://cadsrapi.cancer.gov/rad/NCIAPI/1.0/api/DataElement/{cde_id_ver_str}"
        headers = {"accept": "application/json"}

        try:
            response = requests.get(url, timeout=DEFAULT_TIMEOUT, headers=headers)
            response.raise_for_status()
            json_response = response.json()
            value_set = self.get_valueset_from_json(json_response)
        except JSONDecodeError as e:
            msg = f"Failed to parse JSON response for entity {entity_key}: {e}\nurl: {url}"
            logging.exception(msg)
            return []
        else:
            return value_set

    def check_cdes_against_mdb(
        self,
        mdb_cdes: list[MDBCDESpec],
    ) -> tuple[list[AnnotationSpec], set[tuple[str, str]]]:
        """For MDB CDEs with PVs, check caDSR for new PVs."""
        result = []
        for cde_spec in tqdm(mdb_cdes, desc="Checking caDSR for new PVs..."):
            affected_models = set()
            mdb_pvs = [pv["value"] for pv in cde_spec["permissibleValues"]]
            cadsr_pvs = self.fetch_cde_valueset(
                cde_id=cde_spec["CDECode"],
                cde_version=cde_spec.get("CDEVersion"),
            )
            if not cadsr_pvs:
                logging.exception(
                    "Error fetching PVs from caDSR for %sv%s",
                    cde_spec["CDECode"],
                    cde_spec.get("CDEVersion"),
                )
            annotation_spec: AnnotationSpec = {
                "entity": {},
                "annotation": {
                    "key": (cde_spec["CDEFullName"], cde_spec["CDEOrigin"]),
                    "attrs": {
                        "origin_id": cde_spec["CDECode"],
                        "origin_version": cde_spec.get("CDEVersion"),
                        "origin_name": cde_spec["CDEOrigin"],
                        "value": cde_spec["CDEFullName"],
                    },
                },
                "value_set": [],
            }
            update_annotation = False
            for pv in cadsr_pvs:
                if not pv:
                    logging.exception(
                        "PVs from caDSR for %sv%s are null",
                        cde_spec["CDECode"],
                        cde_spec.get("CDEVersion"),
                    )
                    continue
                if pv["value"] in mdb_pvs:
                    continue
                logging.info("New PV found: %s", pv["value"])
                update_annotation = True
                annotation_spec["value_set"].append(pv)
            if not update_annotation:
                continue
            for model_spec in cde_spec["models"]:
                affected_models.add((model_spec["model"], model_spec["version"]))
            result.append(annotation_spec)
        return result, affected_models


class NCItClient:
    """Client for NCIt API."""

    DEFAULT_NCIM_TSV = Path().cwd() / "data/source/NCIt/NCIt_Metathesaurus_Mapping.txt"
    DEFAULT_NCIM_README_URL = (
        "https://evs.nci.nih.gov/ftp1/Mappings/NCIt_Metathesaurus_Mapping.README.txt"
    )
    DEFAULT_NCIM_ZIP_URL = (
        "https://evs.nci.nih.gov/ftp1/Mappings/NCIt_Metathesaurus_Mapping.txt.zip"
    )
    SOURCE_KEY = "NCIt"
    DATE_FMT = "%Y%m"

    def __init__(
        self,
        ncim_tsv: Path | None = None,
        readme_url: str | None = None,
        zip_url: str | None = None,
    ) -> None:
        """Initialize client."""
        self.readme_url = readme_url or self.DEFAULT_NCIM_README_URL
        self.zip_url = zip_url or self.DEFAULT_NCIM_ZIP_URL
        if not ncim_tsv:
            ncim_tsv = self.DEFAULT_NCIM_TSV
        self.ncim_mapping: dict = self.load_ncim_tsv_to_dict(ncim_tsv)

    def get_readme_date(self) -> datetime.datetime | None:
        """Fetch README file at self.readme_url and return the latest update date."""
        if not self.readme_url:
            msg = "readme_url is not set"
            raise ValueError(msg)

        response = requests.get(self.readme_url, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()

        match = re.match(r"README (\d{6})", response.text.splitlines()[0].strip())
        return (
            datetime.datetime.strptime(
                match.group(1),
                self.DATE_FMT,
            ).replace(tzinfo=datetime.timezone.utc)
            if match
            else None
        )

    def download_and_extract_tsv(self, save_path: Path | None = None) -> dict:
        """Download and extract NCIt mappings TSV file from self.zip_url."""
        if not self.zip_url:
            msg = "zip_url is not set"
            raise ValueError(msg)
        response = requests.get(self.zip_url, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content), "r") as zip_ref:
            tsv_filename = self.DEFAULT_NCIM_TSV.name
            with zip_ref.open(tsv_filename) as f:
                tsv_content = f.read()
                if save_path:
                    with save_path.open("wb") as save_file:
                        save_file.write(tsv_content)

                return self.load_ncim_tsv_to_dict(f)

    def load_ncim_tsv_to_dict(
        self,
        ncim_tsv: Path | io.TextIOWrapper | None = None,
    ) -> dict:
        """Load NCIm TSV file to dict."""
        if not ncim_tsv:
            return {}
        ncim = {}
        if isinstance(ncim_tsv, Path):
            file = ncim_tsv.open(mode="r", encoding="utf-8")
        else:
            file = io.TextIOWrapper(ncim_tsv, encoding="utf-8")
        with file as f:
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

    def check_ncit_for_updated_mappings(self, *, force_update: bool = False) -> bool:
        """Check NCIt for new mappings."""
        latest = self.get_readme_date()
        last = get_last_sync_date(self.SOURCE_KEY)
        if not force_update and (not latest or latest <= last):
            logging.info("No new mappings to sync.")
            return False
        logging.info("New mappings with date %s found. Syncing...", latest)
        self.ncim_mapping = self.download_and_extract_tsv()
        return True

    def check_synonyms_against_mdb(
        self,
        mdb_cdes: list[MDBCDESpec],
    ) -> tuple[list[AnnotationSpec], set[tuple[str, str]]]:
        """For MDB CDEs with PVs, check NCIt for new PV synonyms."""
        result = []
        for cde_spec in tqdm(mdb_cdes, desc="Checking NCIt for new synonyms..."):
            affected_models = set()
            annotation_spec: AnnotationSpec = {
                "entity": {},
                "annotation": {
                    "key": (cde_spec["CDEFullName"], cde_spec["CDEOrigin"]),
                    "attrs": {
                        "origin_id": cde_spec["CDECode"],
                        "origin_version": cde_spec.get("CDEVersion"),
                        "origin_name": cde_spec["CDEOrigin"],
                        "value": cde_spec["CDEFullName"],
                    },
                },
                "value_set": [],
            }
            for pv in cde_spec["permissibleValues"]:
                mdb_synonyms = pv.get("synonyms", [])
                mdb_synonyms_frozen = {frozenset(syn.items()) for syn in mdb_synonyms}
                pv_ncit_codes = [
                    syn.get("origin_id")
                    for syn in mdb_synonyms
                    if syn.get("origin_name") in ["NCIt", "NCIm"]
                ]
                update_annotation = False
                synonyms_to_add = []
                for code in pv_ncit_codes:
                    if not code or code not in self.ncim_mapping:
                        continue
                    ncim_synonyms = self.ncim_mapping[code]
                    for ncim_syn in ncim_synonyms:
                        ncim_syn_frozen = frozenset(ncim_syn.items())
                        if ncim_syn_frozen in mdb_synonyms_frozen:
                            continue
                        logging.info("New synonym found: %s", ncim_syn["value"])
                        update_annotation = True
                        synonyms_to_add.append(ncim_syn)
                if not update_annotation:
                    continue
                pv["synonyms"].extend(synonyms_to_add)
                annotation_spec["value_set"].append(pv)
            if not annotation_spec["value_set"]:
                continue
            for model_spec in cde_spec["models"]:
                affected_models.add((model_spec["model"], model_spec["version"]))
            result.append(annotation_spec)
        return result, affected_models


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
        response = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
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
