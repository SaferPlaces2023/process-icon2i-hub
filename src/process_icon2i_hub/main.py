# -----------------------------------------------------------------------------
# License:
# Copyright (c) 2025 Gecosistema S.r.l.
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
#
# Name:        main.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     18/03/2021
# -----------------------------------------------------------------------------
import click
import pprint
import traceback
import json

from .cli.module_log import Logger
from .utils.status_exception import StatusException
from .utils.module_prologo import prologo, epilogo

from .icon_2i import _ICON2IIngestor, _ICON2IRetriever


# REGION: [ ICON2I INGESTOR ] ========================================================================================

class _ARG_NAMES_ICON2I_INGESTOR():
    VARIABLE = {
        'aliases': ['--variable', '--var'],
        'help': "Variable to retrieve, either 'precipitation' or 'water_level'.",
        'default': None,
        'example': '--variable precipitation',
    }
    FORECAST_RUN = {
        'aliases': ['--forecast_run', '--fr'],
        'help': "Forecast run to retrieve, either 'latest' or a specific date in ISO 8601 format.",
        'default': None,
        'example': '--forecast_run 2025-07-23T00:00:00',
    }
    OUT_DIR = {
        'aliases': ['--out_dir', '--output_dir', '--od'],
        'help': "Output directory for the retrieved data. If not provided, the output will be returned as a dictionary.",
        'default': None,
        'example': '--out_dir /path/to/output',
    }
    BUCKET_DESTINATION = {
        'aliases': ['--bucket_destination', '--bucket', '--s3'],
        'help': "Destination bucket for the output data.",
        'default': None,
        'example': '--bucket_destination s3://my-bucket/path/to/prefix',
    }

@click.command()
@click.option(
    *_ARG_NAMES_ICON2I_INGESTOR.VARIABLE['aliases'],
    type=click.Choice(['total_precipitation'], case_sensitive=True),
    default=_ARG_NAMES_ICON2I_INGESTOR.VARIABLE['default'],
    help=_ARG_NAMES_ICON2I_INGESTOR.VARIABLE['help'],
)
@click.option(
    *_ARG_NAMES_ICON2I_INGESTOR.FORECAST_RUN['aliases'],
    type=str, default=_ARG_NAMES_ICON2I_INGESTOR.FORECAST_RUN['default'],
    help=_ARG_NAMES_ICON2I_INGESTOR.FORECAST_RUN['help'],
)
@click.option(
    *_ARG_NAMES_ICON2I_INGESTOR.OUT_DIR['aliases'],
    type=str, default=_ARG_NAMES_ICON2I_INGESTOR.OUT_DIR['default'],
    help=_ARG_NAMES_ICON2I_INGESTOR.OUT_DIR['help'],
)
@click.option(
    *_ARG_NAMES_ICON2I_INGESTOR.BUCKET_DESTINATION['aliases'],
    type=str, default=_ARG_NAMES_ICON2I_INGESTOR.BUCKET_DESTINATION['default'],
    help=_ARG_NAMES_ICON2I_INGESTOR.BUCKET_DESTINATION['help'],
)
# -----------------------------------------------------------------------------
# Common options to all Gecosistema CLI applications
# -----------------------------------------------------------------------------
@click.option(
    '--backend', 
    type=click.STRING, required=False, default=None,
    help="The backend to use for sending back progress status updates to the backend server."
)
@click.option(
    '--jid',
    type=click.STRING, required=False, default=None,
    help="The job ID to use for sending back progress status updates to the backend server. If not provided, it will be generated automatically."
)
@click.option(
    '--version',
    is_flag=True, required=False, default=False,
    help="Show the version of the package."
)
@click.option(
    '--debug',
    is_flag=True, required=False, default=False,
    help="Debug mode."
)
@click.option(
    '--verbose',
    is_flag=True, required=False, default=False,
    help="Print some words more about what is doing."
)
def cli_run_icon2i_ingestor(**kwargs):
    """
    main_click - main function for the CLI application
    """
    output = run_icon2i_ingestor(**kwargs)
    
    Logger.debug(pprint.pformat(output))
    
    return output

def run_icon2i_ingestor(
    # --- Specific options ---
    variable = None,
    forecast_run = None,
    out_dir = None,
    bucket_destination = None,
    # --- Common options ---
    backend = None,
    jid = None,
    version = False,
    debug = False,
    verbose = False
):
    """
    main_python - main function
    """

    try:
        # DOC: -- Init logger + cli settings + handle version and debug -------
        t0, jid = prologo(backend, jid, version, verbose, debug)

        # DOC: -- Run the ICON-2I ingestor process -------------------------------
        ICON2IIngestor = _ICON2IIngestor()
        results = ICON2IIngestor.run(
            variable=variable,
            forecast_run=forecast_run,
            out_dir=out_dir,
            bucket_destination=bucket_destination,
        )

    except StatusException as err:
        results = {
            'status': err.status,
            'body': {
                'message': str(err),
                ** ({"traceback": traceback.format_exc()} if debug else dict())
            }
        }
    except Exception as e:
        results = {
            "status": StatusException.ERROR,
            "body": {
                "error": str(e),
                ** ({"traceback": traceback.format_exc()} if debug else dict())
            }
        }

    # DOC: -- Cleanup the temporary files if needed ---------------------------
    epilogo(t0, backend, jid)
    
    return results

# ENDREGION: [ ICON2I INGESTOR ] ======================================================================================



# REGION: [ ICON2I RETRIEVER ] ========================================================================================

class _ARG_NAMES_ICON2I_RETRIEVER():
    VARIABLE = {
        'aliases': ['--variable', '--var'],
        'help': "Variable to retrieve, either 'precipitation' or 'water_level'.",
        'default': None,
        'example': '--variable precipitation',
    }
    LAT_RANGE = {
        'aliases': ['--lat_range', '--lat', '--latitude_range', '--latitude', '--lt'],
        'help': "Latitude range as two floats (min, max).",
        'default': None,
        'example': '--lat_range 40.0 42.0',
    }
    LONG_RANGE = {
        'aliases': ['--long_range', '--long', '--longitude_range', '--longitude', '--lg'],
        'help': "Longitude range as two floats (min, max).",
        'default': None,
        'example': '--long_range 12.0 14.0',
    }
    TIME_RANGE = {
        'aliases': ['--time_range', '--time', '--datetime_range', '--datetime', '--t'],
        'help': "Time range as two ISO 8601 UTC0 strings (start, end).",
        'default': None,
        'example': '--time_range 2025-07-23T00:00:00 2025-07-24T00:00:00',
    }
    OUT = {
        'aliases': ['--out', '--output', '--o'],
        'help': "Output file path for the retrieved data. If not provided, the output will be returned as a dictionary.",
        'default': None,
        'example': '--out /path/to/output.json',
    }
    OUT_FORMAT = {
        'aliases': ['--out_format', '--output_format', '--of'],
        'help': "Output format of the retrieved data.",
        'default': None,
        'example': '--out_format geojson',
    }
    BUCKET_SOURCE = {
        'aliases': ['--bucket_source', '--bucket', '--s3'],
        'help': "Source bucket for the input data.",
        'default': None,
        'example': '--bucket_source s3://my-bucket/path/to/prefix',
    }
    BUCKET_DESTINATION = {
        'aliases': ['--bucket_destination', '--bucket', '--s3'],
        'help': "Destination bucket for the output data.",
        'default': None,
        'example': '--bucket_destination s3://my-bucket/path/to/prefix',
    }


@click.command()

# -----------------------------------------------------------------------------
# Specific options of your CLI application
# -----------------------------------------------------------------------------
@click.option(
    *_ARG_NAMES_ICON2I_RETRIEVER.LAT_RANGE['aliases'], 
    callback=lambda ctx, param, value: list(value) if value else None,
    type=float, nargs=2, default=_ARG_NAMES_ICON2I_RETRIEVER.LAT_RANGE['default'], 
    help=_ARG_NAMES_ICON2I_RETRIEVER.LAT_RANGE['help'],
)
@click.option(
    *_ARG_NAMES_ICON2I_RETRIEVER.LONG_RANGE['aliases'],
    callback=lambda ctx, param, value: list(value) if value else None,
    type=float, nargs=2, default=_ARG_NAMES_ICON2I_RETRIEVER.LONG_RANGE['default'],
    help=_ARG_NAMES_ICON2I_RETRIEVER.LONG_RANGE['help'],
)
@click.option(
    *_ARG_NAMES_ICON2I_RETRIEVER.TIME_RANGE['aliases'],
    callback=lambda ctx, param, value: list(value) if value else None,
    type=str, nargs=2, default=_ARG_NAMES_ICON2I_RETRIEVER.TIME_RANGE['default'],
    help=_ARG_NAMES_ICON2I_RETRIEVER.TIME_RANGE['help'],
)
@click.option(
    *_ARG_NAMES_ICON2I_RETRIEVER.VARIABLE['aliases'],
    type=click.Choice(['total_precipitation'], case_sensitive=True),
    default=_ARG_NAMES_ICON2I_RETRIEVER.VARIABLE['default'],
    help=_ARG_NAMES_ICON2I_RETRIEVER.VARIABLE['help'],
)
@click.option(
    *_ARG_NAMES_ICON2I_RETRIEVER.OUT['aliases'],
    type=str, default=_ARG_NAMES_ICON2I_RETRIEVER.OUT['default'],
    help=_ARG_NAMES_ICON2I_RETRIEVER.OUT['help'],
)
@click.option(
    *_ARG_NAMES_ICON2I_RETRIEVER.OUT_FORMAT['aliases'],
    type=click.Choice(['geojson'], case_sensitive=False), default=_ARG_NAMES_ICON2I_RETRIEVER.OUT_FORMAT['default'], 
    help=_ARG_NAMES_ICON2I_RETRIEVER.OUT_FORMAT['help'],
)
@click.option(
    *_ARG_NAMES_ICON2I_RETRIEVER.BUCKET_DESTINATION['aliases'],
    type=str, default=_ARG_NAMES_ICON2I_RETRIEVER.BUCKET_DESTINATION['default'], 
    help=_ARG_NAMES_ICON2I_RETRIEVER.BUCKET_DESTINATION['help'],
)
@click.option(
    *_ARG_NAMES_ICON2I_RETRIEVER.BUCKET_SOURCE['aliases'],
    type=str, default=_ARG_NAMES_ICON2I_RETRIEVER.BUCKET_SOURCE['default'],
    help=_ARG_NAMES_ICON2I_RETRIEVER.BUCKET_SOURCE['help'],
)

# -----------------------------------------------------------------------------
# Common options to all Gecosistema CLI applications
# -----------------------------------------------------------------------------
@click.option(
    '--backend', 
    type=click.STRING, required=False, default=None,
    help="The backend to use for sending back progress status updates to the backend server."
)
@click.option(
    '--jid',
    type=click.STRING, required=False, default=None,
    help="The job ID to use for sending back progress status updates to the backend server. If not provided, it will be generated automatically."
)
@click.option(
    '--version',
    is_flag=True, required=False, default=False,
    help="Show the version of the package."
)
@click.option(
    '--debug',
    is_flag=True, required=False, default=False,
    help="Debug mode."
)
@click.option(
    '--verbose',
    is_flag=True, required=False, default=False,
    help="Print some words more about what is doing."
)

def cli_run_icon2i_retriever(**kwargs):
    """
    main_click - main function for the CLI application
    """
    output = run_icon2i_retriever(**kwargs)
    
    Logger.debug(pprint.pformat(output))
    
    return output


def run_icon2i_retriever(
    # --- Specific options ---
    lat_range = None,
    long_range = None,
    time_range = None,
    variable = None,
    out = None,
    out_format = None,
    bucket_destination = None,
    bucket_source = None,

    # --- Common options ---
    backend = None,
    jid = None,
    version = False,
    debug = False,
    verbose = False
):
    """
    main_python - main function
    """

    try:

        # DOC: -- Init logger + cli settings + handle version and debug -------
        t0, jid = prologo(backend, jid, version, verbose, debug)

        # DOC: -- Run the ARPAV retriever process -------------------------------
        ICON2I_Retriever = _ICON2IRetriever()
        results = ICON2I_Retriever.run(
            lat_range=lat_range,
            long_range=long_range,
            time_range=time_range,
            variable=variable,
            out=out,
            out_format=out_format,
            bucket_destination=bucket_destination,
            bucket_source=bucket_source,
        )

    except StatusException as err:
        results = {
            'status': err.status,
            'body': {
                'message': str(err),
                ** ({"traceback": traceback.format_exc()} if debug else dict())
            }
        }
    except Exception as e:
        results = {
            "status": StatusException.ERROR,
            "body": {
                "error": str(e),
                ** ({"traceback": traceback.format_exc()} if debug else dict())
            }
        }

    # DOC: -- Cleanup the temporary files if needed ---------------------------
    epilogo(t0, backend, jid)
    
    return results

# ENDREGION: [ ICON2I RETRIEVER ] =====================================================================================