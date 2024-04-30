import os
import unittest

from wrabbit.tests.utils import process_nextflow_app
from wrabbit.exceptions import (
    InvalidEntrypoint,
    NoSchemaException,
    SchemaEmptyException
)

TESTS_DIR = "temp_tests_dir"
TEST_DATA_LOCATION = "tests/unit/test_data/sample_code_package_content"

# testovi
# test koji pravi nextflow app i proverava polja
#  - app_contents
#  - da li je input napravljen kako treba
#    - binding
#    - port
#  - da li je output napravljen kako treba
#  - da li ima hintove
#  - da li ima requirements
#    - listing
# test koji pakuje to u yaml i onda proverava


class TestNextflowProcessFile(unittest.TestCase):
    def setUp(self):
        self.root_path = get_walle_root_path()
        self.test_data_dir = os.path.join(self.root_path, TEST_DATA_LOCATION)

    def test_process_nextflow_app(self):
        destination_path = os.path.join(self.test_dir, "data")

        app_dict = process_nextflow_app()

        assert app_dict
        # TODO Validate app content

    def test_process_nextflow_app_renamed_nf_file(self):

        nf_file_name = "new_name.nf"

        app_dict = process_nextflow_app()

        assert app_dict
        # TODO Validate app content

    def test_process_nextflow_app_no_nf_file(self):

        with self.assertRaises(Exception) as context:
            process_nextflow_app()

        assert type(context.exception) is InvalidEntrypoint

    def test_process_nextflow_app_multiple_nf_files(self):

        new_file_name_1 = "new_name_1.nf"
        new_file_name_2 = "new_name_2.nf"

        with self.assertRaises(Exception) as context:
            process_nextflow_app()

        assert type(context.exception) is InvalidEntrypoint

    def test_process_nextflow_app_no_schema_file(self):

        with self.assertRaises(Exception) as context:
            process_nextflow_app()

        assert type(context.exception) is NoSchemaException

    def test_process_nextflow_app_schema_file_empty(self):

        with self.assertRaises(Exception) as context:
            process_nextflow_app()

        assert type(context.exception) is SchemaEmptyException

    def test_process_nextflow_app_no_config_file(self):

        app_dict = process_nextflow_app()

        assert app_dict

    def test_process_nextflow_app_only_schema(self):
        with self.assertRaises(Exception) as context:
            process_nextflow_app()

        assert type(context.exception) is InvalidEntrypoint
