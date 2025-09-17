from dotenv import load_dotenv
load_dotenv()

from .icon_2i import _ICON2IRetriever, _ICON2IIngestor
import importlib.util
if importlib.util.find_spec('pygeoapi') is not None:
    from .icon_2i import ICON2IRetrieverProcessor, ICON2IIngestorProcessor

from .main import run_icon2i_ingestor, run_icon2i_retriever
from .utils.strings import parse_event