from unittest.mock import patch
import pytest
from ....esg_data_pipeline.components.pdf_text_extractor import PDFTextExtractor
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import os


@pytest.fixture
def pdf_text_extractor():
    """
    Create an instance of PDFTextExtractor for testing.
    """
    pdf_text_extractor = PDFTextExtractor(min_paragraph_length=3)
    return pdf_text_extractor


@pytest.fixture
def mock_clean_text():
    """
    Mock the BaseCurator.clean_text method to simulate its behavior.
    """
    with patch(
            'data_extractor.code.esg_data_pipeline.esg_data_pipeline.components.base_curator.BaseCurator.clean_text',
            side_effect=lambda x: x):
        yield


@pytest.fixture
def create_pdf(tmp_path):
    def _create_pdf(content):
        pdf_filename = tmp_path / "test.pdf"
        c = canvas.Canvas(str(pdf_filename), pagesize=letter)

        for page_num, page_content in enumerate(content, start=1):
            c.drawString(100, 750, f"{page_content}")
            if page_num < len(content):
                c.showPage()

        c.save()
        return pdf_filename

    return _create_pdf


@pytest.fixture
def output_folder(tmp_path):
    # Create a temporary output folder for testing
    return tmp_path


class TestProcessPage:

    def test_pdf_text_extractor_constructor_defaults(self, pdf_text_extractor):
        """
        Test the PDFTextExtractor constructor with default values.
        """
        assert pdf_text_extractor.min_paragraph_length == 3
        assert pdf_text_extractor.annotation_folder is None
        assert not pdf_text_extractor.skip_extracted_files
        assert pdf_text_extractor.name == 'PDFTextExtractor'

    def test_pdf_text_extractor_constructor_custom_values(self):
        """
        Test the PDFTextExtractor constructor with custom values.
        """
        annotation_folder = "/path/to/annotations"
        min_paragraph_length = 30
        skip_extracted_files = True
        name = "CustomPDFTextExtractor"

        pdf_text_extractor = PDFTextExtractor(
            annotation_folder=annotation_folder,
            min_paragraph_length=min_paragraph_length,
            skip_extracted_files=skip_extracted_files,
            name=name
        )

        assert pdf_text_extractor.min_paragraph_length == min_paragraph_length
        assert pdf_text_extractor.annotation_folder == annotation_folder
        assert pdf_text_extractor.skip_extracted_files == skip_extracted_files
        assert pdf_text_extractor.name == name

    def test_process_page(self, pdf_text_extractor, mock_clean_text):
        """
        Test the process_page method with standard input.
        """
        input_text = "Paragraph 1\n\nParagraph 2"
        expected_paragraphs = ["Paragraph 1", "Paragraph 2"]

        result = pdf_text_extractor.process_page(input_text)
        assert result == expected_paragraphs

    def test_process_page_custom_min_length(self, pdf_text_extractor, mock_clean_text):
        """
        Test the process_page method with a custom minimum paragraph length.
        """
        # Update the min_paragraph_length before processing the input_text
        pdf_text_extractor.min_paragraph_length = 15

        input_text = "Short\n\nThis is a long paragraph\n\nAnother short"
        expected_paragraphs = ["This is a long paragraph"]
        result = pdf_text_extractor.process_page(input_text)
        print(result)
        assert result == expected_paragraphs

    def test_process_page_empty_input(self, pdf_text_extractor, mock_clean_text):
        """
        Test the process_page method with empty input.
        """
        input_text = ""
        expected_paragraphs = []
        result = pdf_text_extractor.process_page(input_text)
        assert result == expected_paragraphs


class TestExtractPdfByPage:
    @pytest.mark.parametrize("input_content, expected_result", [
        (["Page 1 content", "Page 2 content", "Page 3 content"],
         {0: ["Page 1 content"], 1: ["Page 2 content"], 2: ["Page 3 content"]}),
        ([""], {})  # Empty input content
    ])
    def test_extract_pdf_by_page(self, pdf_text_extractor, create_pdf, input_content, expected_result):
        """
        Test extraction of text from a PDF with custom content and from a PDF with empty content.
        """
        pdf_file = create_pdf(input_content)
        result = pdf_text_extractor.extract_pdf_by_page(str(pdf_file))
        assert result == expected_result

    def test_extract_pdf_by_page_paragraph_length_equal_to_min_length(self, pdf_text_extractor, create_pdf):
        """
        Test extraction of text from a PDF with a paragraph length equal to the minimum length.
        """
        pdf_text_extractor.min_paragraph_length = 15
        pdf_file_content = ["Short", "This is a long paragraph", "Another short"]

        pdf_file = create_pdf(pdf_file_content)
        result = pdf_text_extractor.extract_pdf_by_page(str(pdf_file))

        expected_result = {1: ["This is a long paragraph"]}
        assert result == expected_result


class TestRun:
    def test_run_skip_existing_json(self, create_pdf, output_folder, pdf_text_extractor):
        """
        Test that skip_existing_json works when a JSON file already exists in the output folder.
        """
        pdf_text_extractor.skip_extracted_files = True
        content = ["Page 1 content", "Page 2 content", "Page 3 content"]
        pdf_file = create_pdf(content)
        with open(os.path.join(output_folder, "test.json"), 'w') as f:
            f.write("{}")

        result = pdf_text_extractor.run(pdf_file, output_folder)
        assert result is None

    @pytest.mark.parametrize("input_pdf_content, expected_result", [
        (["Page 1 content", "Page 2 content", "Page 3 content"], {0: ["Page 1 content"]}),
        ([], None),  # Empty input_pdf_content
    ])
    def test_run_successful_extraction(self, pdf_text_extractor, output_folder, create_pdf, mocker, input_pdf_content,
                                       expected_result):
        """
        Test the successful extraction of PDF content.
        """
        input_pdf = create_pdf(input_pdf_content)

        # Mock the extract_pdf_by_page method
        mocker.patch.object(pdf_text_extractor, "extract_pdf_by_page", return_value=expected_result)

        result = pdf_text_extractor.run(input_pdf, output_folder)

        assert result == expected_result


class TestRunFolder:
    def test_run_successful_extraction(self, pdf_text_extractor, output_folder, create_pdf, mocker):
        """
        Test the successful extraction of PDF content.
        """
        # Create two PDFs, one with content and one empty
        pdf_with_content = create_pdf(["Page 1 content", "Page 2 content", "Page 3 content"])
        pdf_empty = create_pdf([])

        # Mock the extract_pdf_by_page method
        mocker.patch.object(pdf_text_extractor, "extract_pdf_by_page", side_effect=[{0: ["Page 1 content"]}, None])

        # Run extraction for the PDF with content
        result_with_content = pdf_text_extractor.run(pdf_with_content, output_folder)

        # Check the result and verify the presence of JSON file
        assert result_with_content == {0: ["Page 1 content"]}
        json_with_content = output_folder / (pdf_with_content.stem + ".json")
        assert json_with_content.exists()

        # Run extraction for the empty PDF
        result_empty = pdf_text_extractor.run(pdf_empty, output_folder)

        # Check the result and verify the absence of JSON file
        assert result_empty is None
        json_empty = output_folder / (pdf_empty.stem + ".json")
        assert json_empty.exists()
