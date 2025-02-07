"""Data types for CDE PVs and Synonyms."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from pathlib import Path


class ModelSpec(TypedDict):
    """CRDC model spec. Dict with 'handle', 'version', and 'yaml_file_names' keys."""

    handle: str
    version: str
    yaml_file_names: list[str | Path]


class PermissibleValue(TypedDict):
    """
    Permissible value dict w/ CDE PV term attributes and synonyms list.

    id & definition refer to ValueMeaning of the Permissible Value.
    synonyms: initiated with NCIt synonym(s) from 'Concepts' for the PV if available.
    """

    value: str
    origin_id: str
    origin_definition: str
    origin_version: str
    origin_name: str
    ncit_concept_codes: str
    synonyms: list[dict[str, str | None]]


class AnnotationSpec(TypedDict):
    """Annotation spec."""

    entity: dict
    annotation: dict
    value_set: list[PermissibleValue | None]


class ModelCDESpec(TypedDict):
    """CRDC model CDE spec."""

    handle: str
    version: str
    annotations: list[AnnotationSpec]
