from .components import Extractor, PDFTextExtractor, TextCurator
from .extraction_server import run_extraction
import logging
from .config import logging_config, config

# Logger for this package
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging_config.get_console_handler())
logger.propagate = False
