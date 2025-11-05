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

from .icon_2i_retriever import _ICON2IRetriever

# -----------------------------------------------------------------------------

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'safer-process',
    'title': {
        'en': 'ICON-2I Precipitation Retirever Process',
    },
    'description': {
        'en': 'Retrieve Precipitations data collected from ICON-2I'
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
        'lat_range': {
            'title': 'Latitude range',
            'description': 'The latitude range in format [lat_min, lat_max]. Values must be in EPSG:4326 crs. If no latitude range is provided, all latitudes will be returned',
            'schema': {
            }
        },
        'long_range': {
            'title': 'Longitude range',
            'description': 'The longitude range in format [long_min, long_max]. Values must be in EPSG:4326 crs. If no longitude range is provided, all longitudes will be returned',
            'schema': {
            }
        },
        'time_range': {
            'title': 'Time range',
            'description': 'The time range in format [time_start, time_end]. Both time_start and time_end must be in ISO-Format and related to at least one week ago. If no time range is provided, all times will be returned',
            'schema': {
            }
        },
        'out': {
            'title': 'Output directory',
            'description': 'The output directory where the data will be stored. If not provided, the data will not be stored in a local directory.',
            'schema': {
                'type': 'string'
            }
        },
        'bucket_source': {
            'title': 'Bucket source',
            'description': 'The bucket source where the data will be retrieved from. If not provided, the data will be retrieved from the ICON-2I API.',
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
            }
        },
        's3_uri': {
            'title': 'S3 Uri',
            'description': 'S3 Uri of the merged timestamp multiband raster',
            'schema': {
            }
        },
        'data': {
            'title': 'Time series dataset',
            'description': 'Dataset with precipitation forecast data time series in requested "out_format"',
            'schema': {
            }
        }
    },
    'example': {
        "inputs": {
            "debug": True,
            "variable": "total_precipitation",
            "forecast_run": [
                "2025-09-18T00:00:00",
            ],
            "bucket_destination": "s3://saferplaces.co/SaferCastAPI/test/ICON2I",
            "token": "123ABC456XYZ",
        }

    }
}

# -----------------------------------------------------------------------------

class ICON2IRetrieverProcessor(BaseProcessor):
    """
    ICON-2I Retriever Processor.
    """

    name = 'ICON2IRetrieverProcessor'
    _tmp_data_folder = os.path.join(os.getcwd(), 'ICON2IRetrieverProcessor')

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

        self._tmp_data_folder = _ICON2IRetriever._tmp_data_folder
        if not os.path.exists(self._tmp_data_folder):
            os.makedirs(self._tmp_data_folder)

    
    def execute(self, data):

        mimetype = 'application/json'

        outputs = {}

        try:
            
            # DOC: Args validation
            self.argument_validation(data)
            Logger.debug(f'Validated process parameters')

            ICON2IRetriever = _ICON2IRetriever()

            # DOC: Set up the ARPAV Retriever
            outputs = ICON2IRetriever.run(**data)
            
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
        return f'<ICON2IRetrieverProcessor> {self.name}'