"""Utilities for testing."""

import re

from bento_mdb_updates.datatypes import AnnotationSpec, ModelCDESpec


def remove_nanoids_from_str(statement: str) -> str:
    """Remove values for 'nanoid' attr from string if present."""
    return re.sub(r"nanoid:'[^']*'", "nanoid:''", statement)


def assert_actual_is_expected(actual, expected) -> None:
    """
    Custom assertion function to compare actual and expected results.
    Prints both values in case of failure for better debugging.
    """
    if actual != expected:
        print("\n=== ACTUAL ===\n", actual)
        print("\n=== EXPECTED ===\n", expected)
    assert actual == expected


TEST_ANNOTATION_SPEC = AnnotationSpec(
    entity={
        "key": ("study", "organism_species"),
        "attrs": {
            "handle": "organism_species",
            "model": "CDS",
            "value_domain": "value_set",
            "is_required": "Preferred",
            "is_key": "False",
            "is_nullable": "False",
            "is_strict": "True",
            "desc": "Species binomial of study participants",
        },
    },
    annotation={
        "key": ("sample_organism_type", "caDSR"),
        "attrs": {
            "handle": "sample_organism_type",
            "value": "Sample Organism Type",
            "origin_id": "6118266",
            "origin_version": "1.00",
            "origin_name": "caDSR",
        },
    },
    value_set=[
        {
            "value": "Mouse",
            "origin_version": "1",
            "origin_id": "2578400",
            "origin_definition": "Any of numerous species of small rodents belonging to the genus Mus and various related genera of the family Muridae.",
            "origin_name": "caDSR",
            "ncit_concept_codes": ["C14238"],
            "synonyms": [
                {
                    "value": "Mouse",
                    "origin_id": "C14238",
                    "origin_definition": "Any of numerous species of small rodents belonging to the genus Mus and various related genera of the family Muridae.",
                    "origin_name": "NCIt",
                },
                {
                    "origin_id": "447482001",
                    "origin_name": "SNOMEDCT_US",
                    "origin_version": "2024_03_01",
                    "value": "Mus",
                },
            ],
        },
        {
            "value": "Human",
            "origin_version": "1",
            "origin_id": "2620875",
            "origin_definition": "The bipedal primate mammal, Homo sapiens; belonging to man or mankind; pertaining to man or to the race of man; use of man as experimental subject or unit of analysis in research.",
            "origin_name": "caDSR",
            "ncit_concept_codes": ["C14225"],
            "synonyms": [
                {
                    "value": "Human",
                    "origin_id": "C14225",
                    "origin_definition": "The bipedal primate mammal, Homo sapiens; belonging to man or mankind; pertaining to man or to the race of man; use of man as experimental subject or unit of analysis in research.",
                    "origin_name": "NCIt",
                }
            ],
        },
        {
            "value": "Dog",
            "origin_version": "1",
            "origin_id": "5729587",
            "origin_definition": "The domestic dog, Canis familiaris.",
            "origin_name": "caDSR",
            "ncit_concept_codes": [],
            "synonyms": [],
        },
    ],
)

TEST_ANNOTATION_SPEC_MIN = AnnotationSpec(
    entity={},
    annotation={
        "key": ("subject_legal_adult_or_pediatric_participant_type", "caDSR"),
        "attrs": {
            "handle": "subject_legal_adult_or_pediatric_participant_type",
            "value": "Subject Legal Adult Or Pediatric Participant Type",
            "origin_id": "11524549",
            "origin_name": "caDSR",
        },
    },
    value_set=[
        {
            "value": "Pediatric",
            "origin_version": "1",
            "origin_id": "2597927",
            "origin_definition": "Having to do with children.",
            "origin_name": "caDSR",
            "ncit_concept_codes": [],
            "synonyms": [],
        },
        {
            "value": "Adult - legal age",
            "origin_version": "1",
            "origin_id": "11524542",
            "origin_definition": (
                "A person of legal age to consent to a procedure as specifed by local regulation."
            ),
            "origin_name": "caDSR",
            "ncit_concept_codes": [],
            "synonyms": [],
        },
    ],
)

TEST_ANNOTATION_SPEC_NO_VS = AnnotationSpec(
    entity={},
    annotation={
        "key": ("program_name_text", "caDSR - CRDC"),
        "attrs": {
            "handle": "program_name_text",
            "value": "Program Name Text",
            "origin_id": "11444542",
            "origin_version": "1",
            "origin_name": "caDSR - CRDC",
        },
    },
    value_set=[],
)

TEST_MODEL_CDE_SPEC = ModelCDESpec(
    handle="TEST",
    version="1.2.3",
    annotations=[
        TEST_ANNOTATION_SPEC,
        TEST_ANNOTATION_SPEC_MIN,
        TEST_ANNOTATION_SPEC_NO_VS,
    ],
)

TEST_MODEL_CDE_SPEC_NO_ANNOTATIONS = ModelCDESpec(
    handle="TEST",
    version="1.2.3",
    annotations=[],
)
