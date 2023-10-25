import pytest

class TestRunExtraction:

    #  Extraction finishes successfully with valid input files and settings.
    @pytest.fixture
    def mock_s3_communication(mocker):
        return mocker.patch('extraction_server.S3Communication')

    @pytest.fixture
    def mock_extractor(mocker):
        return mocker.patch('extraction_server.Extractor')

    def test_extraction_success_valid_input(self, mocker, mock_s3_communication, mock_extractor):
        # Mock the necessary dependencies
        mock_s3_communication.return_value = mocker.Mock()
        mock_extractor.return_value = mocker.Mock()

        # Invoke the function under test
        run_extraction()

        # Assert that the necessary methods were called
        mock_s3_communication.assert_called_once()
        mock_extractor.assert_called_once()

    def test_extraction_success_valid_input_s3_usage(self, mocker, mock_s3_communication, mock_extractor):
        # Mock the necessary dependencies
        mock_s3_communication.return_value = mocker.Mock()
        mock_extractor.return_value = mocker.Mock()

        # Invoke the function under test
        run_extraction()

        # Assert that the necessary methods were called
        mock_s3_communication.assert_called_once()
        mock_extractor.assert_called_once()

    def test_extraction_success_valid_input_s3_usage_train_mode(self, mocker, mock_s3_communication, mock_extractor):
        # Mock the necessary dependencies
        mock_s3_communication.return_value = mocker.Mock()
        mock_extractor.return_value = mocker.Mock()

        # Invoke the function under test
        run_extraction()

        # Assert that the necessary methods were called
        mock_s3_communication.assert_called_once()
        mock_extractor.assert_called_once()

    def test_extraction_fail_no_pdf_files(self, mocker, mock_s3_communication, mock_extractor):
        # Mock the necessary dependencies
        mock_s3_communication.return_value = mocker.Mock()
        mock_extractor.return_value = mocker.Mock()

        # Invoke the function under test
        response = run_extraction()

        # Assert that the response status is 500
        assert response.status_code == 500


    def test_extraction_fail_no_annotations_csv(self, mocker, mock_s3_communication, mock_extractor):
        # Mock the necessary dependencies
        mock_s3_communication.return_value = mocker.Mock()
        mock_extractor.return_value = mocker.Mock()

        # Invoke the function under test
        response = run_extraction()

        # Assert that the response status is 500
        assert response.status_code == 500


    def test_extraction_fail_multiple_annotations_csv(self, mocker, mock_s3_communication, mock_extractor):
        # Mock the necessary dependencies
        mock_s3_communication.return_value = mocker.Mock()
        mock_extractor.return_value = mocker.Mock()

        # Invoke the function under test
        response = run_extraction()

        # Assert that the response status is 500
        assert response.status_code == 500
