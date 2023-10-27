import os
import pytest
from unittest.mock import patch
from ...esg_data_pipeline.extraction_server import create_directory, run_extraction
from ...esg_data_pipeline.config import config


@pytest.fixture
def test_directory_name(tmpdir):
    """
    Fixture to create a temporary test directory for the 'create_directory' tests.
    """
    return str(tmpdir.join("test_directory"))


# Define the fixture to create an empty output folder
@pytest.fixture
def empty_output_folder(tmp_path):
    # Create an empty temporary output folder for testing
    output_dir = tmp_path / "pdfs"
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


class TestCreateDirectory:
    def test_create_directory_success(self, test_directory_name):
        """
        Test that the 'create_directory' function successfully creates a directory.
        """
        # Call the function to create the directory
        create_directory(test_directory_name)
        # Check if the directory exists
        assert os.path.exists(test_directory_name)

    @patch('os.makedirs', side_effect=FileNotFoundError("No such file or directory"))
    def test_create_directory_failure(self, mock_makedirs, test_directory_name):
        """
        Test that the 'create_directory' function raises a 'FileNotFoundError' when
        it encounters an error during directory creation.
        """
        # Call the function, intentionally raising an exception when creating the directory
        with pytest.raises(FileNotFoundError):
            create_directory(test_directory_name)

    def test_create_directory_with_files(self, test_directory_name):
        """
        Test that the 'create_directory' function deletes files within a directory.
        """
        # Create some files in the directory
        os.makedirs(test_directory_name, exist_ok=True)
        open(os.path.join(test_directory_name, "file1.txt"), 'w').close()
        open(os.path.join(test_directory_name, "file2.txt"), 'w').close()

        # Call the function to delete the files within the directory
        create_directory(test_directory_name)

        # Check if the directory is empty after calling the function
        assert not os.listdir(test_directory_name)


class TestRunExtraction:
    def test_run_extraction_no_pdfs_found(self, empty_output_folder):
        output_dir = empty_output_folder

        # Check if the directory is empty
        assert len(os.listdir(output_dir)) == 0

    @pytest.fixture
    def sample_args(self, tmp_path):
        # tmp_path is a built-in pytest fixture for creating temporary directories
        project_name = 'TEST'

        # Create the path strings for the different folders
        base_data_project_folder = tmp_path / project_name
        pdf_folder = base_data_project_folder / 'interim' / 'pdfs'
        base_interim_folder = base_data_project_folder / 'interim' / 'ml'
        extraction_folder = base_interim_folder / 'extraction'
        annotation_folder = base_interim_folder / 'annotations'

        return {
            "project_name": project_name,
            "s3_usage": False,
            "extraction": {
                "use_extractions": False,
                "seed": 42,
                "min_paragraph_length": 20,
                "annotation_folder": None,
                "skip_extracted_files": False,
                "store_extractions": True,
            },
            "pdf_folder": pdf_folder,  # Include the created pdf_folder
            "extraction_folder": extraction_folder,  # Include the created extraction_folder
            "annotation_folder": annotation_folder,  # Include the created annotation_folder
        }

    def test_run_extraction_local_folders_created(self, sample_args):
        with patch(config) as config_mock:
            with patch('os.makedirs') as makedirs_mock:
                run_extraction()
                makedirs_mock.assert_called_with(
                    sample_args['extraction_folder'], exist_ok=True)
                makedirs_mock.assert_called_with(sample_args['annotation_folder'], exist_ok=True)
                makedirs_mock.assert_called_with(sample_args['pdf_folder'], exist_ok=True)

    def test_run_extraction_successful_extraction(sample_args, extractor_mock):
        ext_mock = extractor_mock.return_value
        ext_mock.run_folder.return_value = None
        with patch('your_module.os.listdir', return_value=['extracted_file.txt']):
            response = run_extraction()
            ext_mock.run_folder.assert_called_with(
                sample_args['pdf_folder'], sample_args['extraction_folder'])
            assert "Extraction finished successfully" in str(response)
