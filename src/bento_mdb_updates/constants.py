"""Constants for MDB updates."""

import logging

VALID_MDB_IDS = [
    "fnl-mdb-dev",
    "cloud-one-mdb-dev",
    "cloud-one-mdb-qa",
    "cloud-one-mdb-stage",
    "cloud-one-mdb-prod",
    "og-mdb-dev",
    "og-mdb-nightly",
    "og-mdb-prod",
]
MDB_IDS_WITH_PRERELEASES = [
    "og-mdb-nightly",
]
VALID_TIERS = {
    "lower": ["dev", "dev2", "qa", "qa2"],
    "upper": ["stage", "prod"],
}
VALID_LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "severe": logging.CRITICAL,
    "off": logging.NOTSET,
}
GITHUB_TOKEN_SECRET = "mdb-updates-github-token"  # noqa: S105
GITHUB_TOKEN_SECRET_NWM = "nwm-github-token"  # noqa: S105
MDB_UPDATES_GH_REPO = "nelsonwmoore/bento-mdb-updates"
DH_TERMS_GH_REPO = "CBIIT/crdc-datahub-terms"
