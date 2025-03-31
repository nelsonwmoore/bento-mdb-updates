import copy
from pathlib import Path

import pytest
import yaml
from bento_mdf.mdf import MDFReader

from bento_mdb_updates.model_cdes import (
    get_yaml_files_from_spec,
    load_model_specs_from_yaml,
    make_model_cde_spec,
    process_mdb_cdes,
)
from tests.test_utils import (
    TEST_MAKE_MODEL_CDE_SPEC_BASE,
    TEST_MDB_CDE_SPEC,
    TEST_MDB_CDE_SPEC_RAW,
    TEST_MODEL_SPEC,
    TEST_MODEL_SPEC_INVALID_YML,
    TEST_MODEL_SPEC_YML,
    assert_equal,
)


class TestLoadModelSpecsFromYaml:
    def test_load_model_specs_from_yaml_valid(self, tmp_path):
        valid_yaml = Path(tmp_path / "valid.yml")
        valid_yaml.write_text(TEST_MODEL_SPEC_YML, encoding="utf-8")
        actual = load_model_specs_from_yaml(valid_yaml)
        assert_equal(actual, TEST_MODEL_SPEC)

    def test_load_model_specs_from_yaml_invalid(self, tmp_path):
        invalid_yaml = Path(tmp_path / "invalid.yml")
        invalid_yaml.write_text(TEST_MODEL_SPEC_INVALID_YML, encoding="utf-8")
        with pytest.raises(yaml.YAMLError):
            load_model_specs_from_yaml(invalid_yaml)

    def test_load_model_specs_from_yaml_missing(self, tmp_path):
        valid_yaml = Path(tmp_path / "missing.yml")
        with pytest.raises(FileNotFoundError):
            load_model_specs_from_yaml(valid_yaml)


class TestGetYamlFilesFromSpec:
    def test_get_yaml_files_from_spec(self):
        test_model, test_version = "TCDS", "1.0.0"
        test_spec = TEST_MODEL_SPEC[test_model]
        actual = get_yaml_files_from_spec(test_spec, test_version)
        base_url = (
            "https://raw.githubusercontent.com/"
            "CBIIT/test-cds-model/1.0.0-release/model-desc/"
        )
        expected = [base_url + str(f) for f in test_spec["mdf_files"]]
        assert_equal(actual, expected)

    def test_get_yaml_files_from_spec_latest(self):
        test_model = "TCCDI"
        test_spec = TEST_MODEL_SPEC[test_model]
        actual = get_yaml_files_from_spec(test_spec)
        base_url = (
            "https://raw.githubusercontent.com/CBIIT/test-ccdi-model/2.0.0/model-desc/"
        )
        expected = [base_url + str(f) for f in test_spec["mdf_files"]]
        assert_equal(actual, expected)

    def test_get_yaml_files_from_spec_no_tag(self):
        test_model = "TCCDI"
        test_spec = TEST_MODEL_SPEC[test_model]
        actual = get_yaml_files_from_spec(test_spec, "0.1.0")
        base_url = (
            "https://raw.githubusercontent.com/CBIIT/test-ccdi-model/0.1.0/model-desc/"
        )
        expected = [base_url + str(f) for f in test_spec["mdf_files"]]
        assert_equal(actual, expected)


class TestModelCDESpec:
    TEST_MDF = Path(__file__).parent / "samples" / "test_mdf_cdes.yml"
    mdf = MDFReader(TEST_MDF)
    model = mdf.model

    def test_make_model_cde_spec(self):
        actual = make_model_cde_spec(self.model)
        assert_equal(actual, TEST_MAKE_MODEL_CDE_SPEC_BASE)

    def test_add_ncit_synonyms_to_model_cde_spec(self):
        pass

    def test_load_model_cde_spec(self):
        pass

    def test_compare_model_specs_to_mdb(self):
        pass


def test_process_mdb_cdes():
    raw_mdb_cdes = copy.deepcopy(TEST_MDB_CDE_SPEC_RAW)
    process_mdb_cdes([raw_mdb_cdes])  # type: ignore
    assert_equal(raw_mdb_cdes, TEST_MDB_CDE_SPEC)
