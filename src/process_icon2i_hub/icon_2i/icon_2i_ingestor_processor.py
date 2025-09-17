# =================================================================
#
# Authors: Valerio Luzzi <valluzzi@gmail.com>
#
# Copyright (c) 2023 Valerio Luzzi
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import os
import json
import uuid
import datetime
import requests

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from ..cli.module_log import Logger, set_log_debug
from ..utils import filesystem, module_s3
from ..utils.status_exception import StatusException

from . import _ARPAV_RETRIEVERS
from .icon_2i_ingestor import _ICON2IIngestor

# -----------------------------------------------------------------------------


#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'safer-process',
    'title': {
        'en': 'ICON-2I Precipitation Ingestor Process',
    },
    'description': {
        'en': 'Collect Precipitations data from ICON-2I'
    },
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['safer process'],
    'inputs': {
        'token': {
            'title': 'secret token',
            'description': 'identify yourself',
            'schema': {
                'type': 'string'
            }
        },
        'variable': {
            'title': 'Variable',
            'description': 'The variable to retrieve. Possible values are "precipitation" or "water_level".',
            'schema': {
                'type': 'string',
                'enum': ['precipitation', 'water_level']
            }
        },
        'forecast_run': {
            'title': 'Forecast run',
            'description': 'ICON-2I forecast runs (optional). If not provided, all the available forecast runs from current date will be considered. The forecast run must be a valid ISO format date string at hour 00:00:00 or 12:00:00 related to at least two days ago',
            'schema': {
                'type': 'iso-string or list of iso-string',
                'format': 'YYYY-MM-DDTHH:MM:SS or [YYYY-MM-DDTHH:MM:SS, YYYY-MM-DDTHH:MM:SS, ...]'
            }
        },
        'out_dir': {
            'title': 'Output directory',
            'description': 'The output directory where the data will be stored. If not provided, the data will not be stored in a local directory.',
            'schema': {
                'type': 'string'
            }
        },
        'bucket_destination': {
            'title': 'Bucket destination',
            'description': 'The bucket destination where the data will be stored. If not provided, the data will not be stored in a bucket. If neither out nor bucket_destination are provided, the output will be returned as a feature collection.',
            'schema': {
                'type': 'string'
            }
        },
        'debug': {
            'title': 'Debug',
            'description': 'Enable Debug mode',
            'schema': {
            }
        },
    },
    'outputs': {
        'status': {
            'title': 'status',
            'description': 'Staus of the process execution [OK or KO]',
            'schema': {
                'type': 'string',
                'enum': ['OK', 'KO']
            }
        },
        'collected_data': {
            'title': 'Collected data',
            'description': 'Reference to the collected data. Each entry contains the date and the S3 URI of the collected data',
            'type': 'array',
            'schema': {
                'type': 'object',
                'properties': {
                    'date': {
                        'type': 'string'
                    },
                    'S3_uri': {
                        'type': 'string'
                    }
                }
            }
        }
    },
    'example': {
        "inputs": {
            "token": "ABC123XYZ666",
            "debug": True,
            "forecast_run": ["2025-02-26T00:00:00", "2025-02-26T12:00:00"]
        }
    }
}

# -----------------------------------------------------------------------------

class ICON2IIngestorProcessor(BaseProcessor):
    """
    ICON-2I Ingestor Processor.
    """

    _tmp_data_folder = os.path.join(os.getcwd(), 'ICON2IIngestorProcessor')

    def __init__(self, processor_def):
        """
        Initialize the ICON2I Retriever Process.
        """
        super().__init__(processor_def, PROCESS_METADATA)

        if not os.path.exists(self._tmp_data_folder):
            os.makedirs(self._tmp_data_folder, exist_ok=True)


    def argument_validation(self, data):
        """
        Validate the arguments passed to the processor.
        """

        token = data.get('token', None)
        debug = data.get('debug', False)

        if token is None or token != os.getenv("INT_API_TOKEN", "token"):
            raise StatusException(StatusException.DENIED, 'ACCESS DENIED: wrong token')
            
        if type(debug) is not bool:
            raise StatusException(StatusException.INVALID, 'debug must be a boolean')
        if debug:
            set_log_debug()

        self._tmp_data_folder = _ARPAV_RETRIEVERS[self.variable]._tmp_data_folder
        if not os.path.exists(self._tmp_data_folder):
            os.makedirs(self._tmp_data_folder)

    
    def execute(self, data):

        mimetype = 'application/json'

        outputs = {}

        try:
            
            # DOC: Args validation
            self.argument_validation(data)
            Logger.debug(f'Validated process parameters')

            ICON2IIngestor = _ICON2IIngestor()

            # DOC: Set up the ARPAV Retriever
            outputs = ICON2IIngestor.run(**data)
            
        except StatusException as err:
            outputs = {
                'status': err.status,
                'message': str(err)
            }
        except Exception as err:
            outputs = {
                'status': StatusException.ERROR,
                'error': str(err)
            }
            raise ProcessorExecuteError(str(err))
        
        filesystem.garbage_folders(self._tmp_data_folder)
        Logger.debug(f'Cleaned up temporary data folder: {self._tmp_data_folder}')
        
        return mimetype, outputs


    def __repr__(self):
        return f'<ICON2IIngestorProcessor> {self.name}'