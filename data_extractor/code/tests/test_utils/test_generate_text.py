from pathlib import Path
from train_on_pdf import generate_text_3434
from tests.utils_test import write_to_file
import shutil
from unittest.mock import patch, Mock, call
import s3_communication
import train_on_pdf
import pytest

# types
import typing
from _pytest.capture import CaptureFixture


@pytest.fixture(autouse=True)
def prerequisites_generate_text(path_folder_temporary: Path) -> None:
    """Defines a fixture for mocking all required paths and creating required temporary folders

    :param path_folder_temporary: Requesting the path_folder_temporary fixture
    :type path_folder_temporary: Path
    :rtype: None
    """
    path_folder_relevance = path_folder_temporary / 'relevance'
    path_folder_text_3434 = path_folder_temporary / 'folder_test_3434'
    path_folder_relevance.mkdir(parents = True)
    path_folder_text_3434.mkdir(parents = True)
    
    # create multiple files in the folder_relevance with the same header
    for i in range(5):
        path_current_file = path_folder_relevance / f'{i}_test.csv'
        path_current_file.touch()
        write_to_file(path_current_file, f'That is a test {i}', 'HEADER')
        
    with (patch('train_on_pdf.folder_relevance', str(path_folder_relevance)),
          patch('train_on_pdf.folder_text_3434', str(path_folder_text_3434)),
          patch('train_on_pdf.os.getenv', lambda *args: args[0])):
        yield
        
        # cleanup
        for path in path_folder_temporary.glob("*"):
            shutil.rmtree(path)


def test_generate_text_with_s3(path_folder_temporary: Path):
    """Tests if the s3 connection objects are created and their methods are called
    Requesting prerequisites_generate_text automatically (autouse)

    :param path_folder_temporary: Requesting the path_folder_temporary fixture
    :type path_folder_temporary: Path
    """
    # get the path to the temporary folder
    path_folder_text_3434 = path_folder_temporary / 'folder_test_3434'
    project_name = 'test'
    
    mocked_s3_settings = {
        'prefix': 'test_prefix',
        'main_bucket': {
            's3_endpoint': 'S3_END_MAIN',
            's3_access_key': 'S3_ACCESS_MAIN',
            's3_secret_key': 'S3_SECRET_MAIN',
            's3_bucket_name': 'S3_NAME_MAIN'
        },
        'interim_bucket': {
            's3_endpoint': 'S3_END_INTERIM',
            's3_access_key': 'S3_ACCESS_INTERIM',
            's3_secret_key': 'S3_SECRET_INTERIM',
            's3_bucket_name': 'S3_NAME_INTERIM'
        }
    }
    
    with (patch('train_on_pdf.S3Communication', Mock(spec=s3_communication.S3Communication)) as mocked_s3):
        generate_text_3434(project_name, True, mocked_s3_settings)
        
    # check for calls
    mocked_s3.assert_any_call(s3_endpoint_url='S3_END_MAIN',
                                 aws_access_key_id='S3_ACCESS_MAIN',
                                 aws_secret_access_key='S3_SECRET_MAIN',
                                 s3_bucket='S3_NAME_MAIN')
    mocked_s3.assert_any_call(s3_endpoint_url='S3_END_INTERIM',
                                 aws_access_key_id='S3_ACCESS_INTERIM',
                                 aws_secret_access_key='S3_SECRET_INTERIM',
                                 s3_bucket='S3_NAME_INTERIM')
    
    call_list = [call[0] for call in mocked_s3.mock_calls]
    assert any([call for call in call_list if 'download_files_in_prefix_to_dir' in call])
    assert any([call for call in call_list if 'upload_file_to_s3' in call])


def test_generate_text_no_s3(path_folder_temporary: Path):
    """Tests if files are taken from the folder relevance,
    then read in and putting the content into the file text_3434.csv. Note that
    the header of text_3434.csv is taken from the first file read in
    Requesting prerequisites_generate_text automatically (autouse)

    :param path_folder_temporary: Requesting the path_folder_temporary fixture
    :type path_folder_temporary: Path
    """
    # get the path to the temporary folder
    path_folder_text_3434 = path_folder_temporary / 'folder_test_3434'
    project_name = 'test'
    s3_usage = False
    project_settings = None
    
    generate_text_3434(project_name, s3_usage, project_settings)
            
    # ensure that the header and the content form the first file is written to 
    # the file text_3434.csv in folder relevance and the the content of the other
    # files in folder relevance is appended without the header

    # check if file_3434 exists
    path_file_text_3434_csv = path_folder_text_3434 / 'text_3434.csv'
    assert path_file_text_3434_csv.exists()
    
    # check if header and content of files exist
    strings_expected = [
        f'That is a test {line_number}' for line_number in range(5)
        ]

    with open(str(path_file_text_3434_csv), 'r') as file_text_3434:
        for line_number, line_content in enumerate(file_text_3434, start = -1):
            if line_number == -1:
                assert line_content.rstrip() == 'HEADER'
            else:
                assert line_content.rstrip() in strings_expected


def test_generate_text_successful(path_folder_temporary: Path):
    """Tests if the function returns true
    Requesting prerequisites_generate_text automatically (autouse)

    :param path_folder_temporary: Requesting the path_folder_temporary fixture
    :type path_folder_temporary: Path
    """
    project_name = 'test'
    s3_usage = False
    project_settings = None
    
    return_value = generate_text_3434(project_name, s3_usage, project_settings)
    assert return_value == True

    
def test_generate_text_not_successful_empty_folder(path_folder_temporary: Path,
                                                   capsys: typing.Generator[CaptureFixture[str], None, None]):
    """Tests if the function returns false
    Requesting prerequisites_generate_text automatically (autouse)

    :param path_folder_temporary: Requesting the path_folder_temporary fixture
    :type path_folder_temporary: Path
    :param capsys: Requesting default fixture for capturing cmd output
    :type capsys: typing.Generator[CaptureFixture[str], None, None])
    """
    project_name = 'test'
    s3_usage = False
    project_settings = None
    
    # clear the relevance folder
    path_folder_relevance = path_folder_temporary / 'relevance'
    [file.unlink() for file in path_folder_relevance.glob("*") if file.is_file()] 
    
    # call the function
    return_value = generate_text_3434(project_name, s3_usage, project_settings)
    
    output_cmd, _ = capsys.readouterr()
    assert 'No relevance inference results found.' in output_cmd
    assert return_value == False

    
def test_generate_text_not_successful_exception(path_folder_temporary: Path):
    """Tests if the function returns false
    Requesting prerequisites_generate_text automatically (autouse)

    :param path_folder_temporary: Requesting the path_folder_temporary fixture
    :type path_folder_temporary: Path
    """
    project_name = 'test'
    s3_usage = False
    project_settings = None
    
    # clear the relevance folder
    path_folder_relevance = path_folder_temporary / 'relevance'
    [file.unlink() for file in path_folder_relevance.glob("*") if file.is_file()]
    
    # patch glob.iglob to force an exception...
    with patch('train_on_pdf.glob.iglob', side_effect=lambda *args: [None]):
        return_value = generate_text_3434(project_name, s3_usage, project_settings)
        
        assert return_value == False
