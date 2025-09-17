from .icon_2i_ingestor import _ICON2IIngestor
from .icon_2i_retriever import _ICON2IRetriever

import importlib.util
if importlib.util.find_spec('pygeoapi') is not None:
    from .icon_2i_ingestor_processor import ICON2IIngestorProcessor
    from .icon_2i_retriever_processor import ICON2IRetrieverProcessor