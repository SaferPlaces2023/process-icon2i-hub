from dotenv import load_dotenv
load_dotenv()

from .icon_2i import _ARPAV_RETRIEVERS, _ARPAVPrecipitationRetriever, _ARPAVWaterLevelRetriever
import importlib.util
if importlib.util.find_spec('pygeoapi') is not None:
    from .icon_2i import ARPAVRetrieverProcessor

from .main import run_arpav_retriever
from .utils.strings import parse_event