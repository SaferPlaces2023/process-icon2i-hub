from .icon_2i_tp_retriever import _ARPAVWaterLevelRetriever

_ARPAV_RETRIEVERS = {
    'total_precipitation': icon_2i_tp_retriever,
}

import importlib.util
if importlib.util.find_spec('pygeoapi') is not None:
    from .icon_2i_retriever_processor import ARPAVRetrieverProcessor