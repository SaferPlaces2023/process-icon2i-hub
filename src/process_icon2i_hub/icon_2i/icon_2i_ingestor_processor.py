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

from .icon_2i_ingestor import _ICON2IIngestor

# -----------------------------------------------------------------------------


#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'safer-process',
    'title': {
        'en': 'ICON-2I Ingestor Process',
    },
    'description': {
        'en': 'Collect data from ICON-2I'
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
            "debug": True,

            "variable": "total_precipitation",

            "lat_range": [
            44,
            44.5
            ],
            "long_range": [
            12.2,
            12.8
            ],
            
            "time_range": [
                "2025-09-18T00:00:00",
                "2025-09-18T08:00:00"
            ],

            "bucket_destination": "s3://saferplaces.co/SaferCastAPI/test/ICON2I",
        }
    }
}

# -----------------------------------------------------------------------------

class ICON2IIngestorProcessor(BaseProcessor):
    """
    ICON-2I Ingestor Processor.
    """
    name = 'ICON2IIngestorProcessor'

    def __init__(self, processor_def):
        """
        Initialize the ICON2I Ingestor Process.
        """
        super().__init__(processor_def, PROCESS_METADATA)
        
        # Dual-mode configuration
        self.processor_mode = os.getenv('ICON2I_PROCESSOR_MODE', 'local').lower()
        if self.processor_mode not in ['local', 'lambda']:
            self.processor_mode = 'local'
        Logger.debug(f'ICON2I Ingestor processor mode: {self.processor_mode}')
        
        # Lambda configuration (only loaded if mode is lambda)
        self._lambda_client = None
        self._lambda_function_name = None
        self._lambda_region = None
        if self.processor_mode == 'lambda':
            self._lambda_function_name = os.getenv('ICON2I_INGESTOR_LAMBDA_FUNCTION_NAME')
            self._lambda_region = os.getenv('AWS_REGION', 'us-east-1')
            if not self._lambda_function_name:
                raise StatusException(
                    StatusException.INVALID,
                    'ICON2I_INGESTOR_LAMBDA_FUNCTION_NAME environment variable is required when ICON2I_PROCESSOR_MODE=lambda'
                )


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

    def _get_lambda_client(self):
        """
        Get or create boto3 Lambda client (lazy initialization).
        """
        if self._lambda_client is None:
            try:
                import boto3
            except ImportError:
                raise StatusException(
                    StatusException.ERROR,
                    'boto3 is required for Lambda mode. Install it with: pip install boto3'
                )
            self._lambda_client = boto3.client('lambda', region_name=self._lambda_region)
        return self._lambda_client

    def _invoke_lambda(self, data):
        """
        Invoke Lambda function synchronously and return normalized response.
        """
        client = self._get_lambda_client()
        
        try:
            # Prepare payload
            payload = json.dumps(data)
            Logger.debug(f'Invoking Lambda function: {self._lambda_function_name}')
            
            # Invoke synchronously
            response = client.invoke(
                FunctionName=self._lambda_function_name,
                InvocationType='RequestResponse',
                Payload=payload
            )
            
            # Parse response
            if response['StatusCode'] != 200:
                raise StatusException(
                    StatusException.ERROR,
                    f'Lambda returned status code {response["StatusCode"]}'
                )
            
            # Extract FunctionResult
            result_payload = json.load(response['Payload'])
            Logger.debug(f'Lambda response: {result_payload}')
            
            # Handle Lambda response format: {statusCode, body: {result: ...}}
            if isinstance(result_payload, dict):
                if 'body' in result_payload and isinstance(result_payload['body'], dict):
                    return result_payload['body'].get('result', result_payload)
                elif 'result' in result_payload:
                    return result_payload['result']
                else:
                    return result_payload
            else:
                return result_payload
                
        except Exception as err:
            if isinstance(err, StatusException):
                raise
            raise StatusException(
                StatusException.ERROR,
                f'Lambda invocation failed: {str(err)}'
            )

    
    def execute(self, data):

        mimetype = 'application/json'

        outputs = {}
        cleanup_needed = False

        try:
            
            # DOC: Args validation
            self.argument_validation(data)
            Logger.debug(f'Validated process parameters')

            # Execute based on processor mode
            if self.processor_mode == 'lambda':
                # Lambda mode: invoke external Lambda function
                Logger.debug(f'Executing in Lambda mode')
                outputs = self._invoke_lambda(data)
            else:
                # Local mode: run ingestor locally (default, backward compatible)
                Logger.debug(f'Executing in local mode')
                cleanup_needed = True
                
                # DOC: Processor can be called in async mode, so we need to set up a temporary data folder different for each call → avoid one call to delete the data of another call
                ICON2IIngestor = _ICON2IIngestor()
                ICON2IIngestor._set_tmp_data_folder(os.path.join(ICON2IIngestor._tmp_data_folder, str(uuid.uuid4())))
                
                # DOC: Run the ICON2I Ingestor
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
        
        finally:
            # Clean up temporary data folder only in local mode
            if cleanup_needed:
                filesystem.rmdir(ICON2IIngestor._tmp_data_folder)
                Logger.debug(f'Removed temporary data folder: {ICON2IIngestor._tmp_data_folder}')
        
        return mimetype, outputs


    def __repr__(self):
        return f'<ICON2IIngestorProcessor> {self.name}'