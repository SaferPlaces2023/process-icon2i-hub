import os
import json
import uuid
import datetime
import urllib3
import requests

import numpy as np
import pandas as pd
import pygrib
import xarray as xr
import gdal2numpy as g2n

from . import _consts
from .icon_2i_ingestor import _ICON2IIngestor
from ..cli.module_log import Logger
from ..utils import filesystem, module_s3
from ..utils.status_exception import StatusException


urllib3.disable_warnings()


class _ICON2IRetriever():

    name = f'{_consts._DATASET_NAME}__Retriever'
    
    _tmp_data_folder = os.path.join(os.getcwd(), name)

    def __init__(self):
        
        if not os.path.exists(self._tmp_data_folder):
            os.makedirs(self._tmp_data_folder)


    def argument_validation(self, **kwargs):
        """
        Validate the arguments passed to the processor.
        """

        print(f"Validating arguments: {kwargs}")

        variable = kwargs.get('variable', None)
        lat_range = kwargs.get('lat_range', None)
        long_range = kwargs.get('long_range', None)
        time_range = kwargs.get('time_range', None)
        time_start = time_range[0] if type(time_range) in [list, tuple] else time_range
        time_end = time_range[1] if type(time_range) in [list, tuple] else None
        out_format = kwargs.get('out_format', None)
        bucket_source = kwargs.get('bucket_source', None)
        bucket_destination = kwargs.get('bucket_destination', None)
        out = kwargs.get('out', None)

        if variable is None:
            variable = list(_consts._VARIABLES_DICT.keys())
            Logger.debug(f'No variable specified, collect all variables: {variable}')
        if not isinstance(variable, (str, list)):
            raise StatusException(StatusException.INVALID, 'variable must be a string or a list of strings')
        if isinstance(variable, str):
            variable = [variable]
        if not all(isinstance(v, str) for v in variable):
            raise StatusException(StatusException.INVALID, 'All variables must be strings')
        if not all(v in _consts._VARIABLES_DICT for v in variable):
            raise StatusException(StatusException.INVALID, f'Invalid variable "{variable}". Must be one of {_consts._VARIABLES_DICT.keys()}')

        if lat_range is not None:
            if type(lat_range) is not list or len(lat_range) != 2:
                raise StatusException(StatusException.INVALID, 'lat_range must be a list of 2 elements')
            if type(lat_range[0]) not in [int, float] or type(lat_range[1]) not in [int, float]:
                raise StatusException(StatusException.INVALID, 'lat_range elements must be float')
            if lat_range[0] < -90 or lat_range[0] > 90 or lat_range[1] < -90 or lat_range[1] > 90:
                raise StatusException(StatusException.INVALID, 'lat_range elements must be in the range [-90, 90]')
            if lat_range[0] > lat_range[1]:
                raise StatusException(StatusException.INVALID, 'lat_range[0] must be less than lat_range[1]')
        
        if long_range is not None:
            if type(long_range) is not list or len(long_range) != 2:
                raise StatusException(StatusException.INVALID, 'long_range must be a list of 2 elements')
            if type(long_range[0]) not in [int, float] or type(long_range[1]) not in [int, float]:
                raise StatusException(StatusException.INVALID, 'long_range elements must be float')
            if long_range[0] < -180 or long_range[0] > 180 or long_range[1] < -180 or long_range[1] > 180:
                raise StatusException(StatusException.INVALID, 'long_range elements must be in the range [-180, 180]')
            if long_range[0] > long_range[1]:
                raise StatusException(StatusException.INVALID, 'long_range[0] must be less than long_range[1]')
        
        if time_start is None:
            raise StatusException(StatusException.INVALID, 'Cannot process without a time valued')
        if type(time_start) is not str:
            raise StatusException(StatusException.INVALID, 'time_start must be a string')
        if type(time_start) is str:
            try:
                time_start = datetime.datetime.fromisoformat(time_start)
            except ValueError:
                raise StatusException(StatusException.INVALID, 'time_start must be a valid datetime iso-format string')
        
        if time_end is not None:
            if type(time_end) is not str:
                raise StatusException(StatusException.INVALID, 'time_end must be a string')
            if type(time_end) is str:
                try:
                    time_end = datetime.datetime.fromisoformat(time_end)
                except ValueError:
                    raise StatusException(StatusException.INVALID, 'time_end must be a valid datetime iso-format string')
            if time_start > time_end:
                raise StatusException(StatusException.INVALID, 'time_start must be less than time_end')
            
        time_start = time_start.replace(minute=(time_start.minute // 5) * 5, second=0, microsecond=0)
        time_end = time_end.replace(minute=(time_end.minute // 5) * 5, second=0, microsecond=0) if time_end is not None else time_start + datetime.timedelta(hours=1)
        if time_end < (datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(hours=48)).replace(tzinfo=None):
            raise StatusException(StatusException.INVALID, 'Time range must be within the last 48 hours')

        if out_format is not None:  
            if type(out_format) is not str:
                raise StatusException(StatusException.INVALID, 'out_format must be a string or null')
            if out_format not in ['tif']:
                raise StatusException(StatusException.INVALID, 'out_format must be one of ["tif"]')
        else:
            out_format = 'tif'
        
        if bucket_destination is not None:
            if type(bucket_destination) is not str:
                raise StatusException(StatusException.INVALID, 'bucket_destination must be a string')
            if not bucket_destination.startswith('s3://'):
                raise StatusException(StatusException.INVALID, 'bucket_destination must start with "s3://"')
            
        if bucket_source is not None:
            if type(bucket_source) is not str:
                raise StatusException(StatusException.INVALID, 'bucket_source must be a string')
            if not bucket_source.startswith('s3://'):
                raise StatusException(StatusException.INVALID, 'bucket_source must start with "s3://"')
        else:
            bucket_source = bucket_destination
            
        if out is not None:
            if type(out) is not str:
                raise StatusException(StatusException.INVALID, 'out must be a string')
            if not out.endswith('.tif'):
                raise StatusException(StatusException.INVALID, 'out must end with ".tif"')
            dirname, _ = os.path.split(out)
            if dirname != '' and not os.path.exists(dirname):
                os.makedirs(dirname)

        return {
            'variable': variable,
            'lat_range': lat_range,
            'long_range': long_range,
            'time_start': time_start,
            'time_end': time_end,
            'out_format': out_format,
            'bucket_source': bucket_source,
            'bucket_destination': bucket_destination,
            'out': out
        }
    

    def retrieve_icon2I_data(self, variable, lat_range, lon_range, time_start, time_end, bucket_source):

        # DOC: Check if the dataset is available in the source bucket
        def check_date_dataset_avaliability(variable, requested_dates, bucket_source):
            requested_source_uris = [f'{bucket_source}/{_consts._DATASET_NAME}__{variable}__{d}.nc' for d in requested_dates]
            bucket_source_filekeys = module_s3.s3_list(bucket_source, filename_prefix=f'{_consts._DATASET_NAME}__{variable}__')
            bucket_source_uris = [f'{bucket_source}/{filesystem.justfname(f)}' for f in bucket_source_filekeys]
            avaliable_uris = [ru for ru in requested_source_uris if ru in bucket_source_uris]
            if len(avaliable_uris) != len(requested_dates):
                return None
            return avaliable_uris
        

        requested_dates = pd.date_range(time_start, time_end, freq='1d').to_series().apply(lambda d: d.date()).to_list()
        
        variable_datasets = dict()
        for var in variable:
           
            data_source_uris = check_date_dataset_avaliability(var, requested_dates, bucket_source) if bucket_source is not None else None

            # DOC: If the dataset is not available in the source bucket, run the ingestor to retrieve it
            # DOC: It is convenient to ingest all the requested variables due to high probability that if one is missing then also the others are missing .. so let's do it once
            if data_source_uris is None:
                icon2i_ingestor = _ICON2IIngestor()
                icon2i_ingestor_out = icon2i_ingestor.run(
                    variable = variable,
                    forecast_run = list(map(lambda d: d.isoformat(), requested_dates)),
                    out_dir = self._tmp_data_folder,
                    bucket_destination = bucket_source
                )
                if icon2i_ingestor_out.get('status', 'ERROR') != 'OK':
                    raise StatusException(StatusException.ERROR, f'Error during ICON2I ingestor run: {icon2i_ingestor_out["message"]}')    
                data_source_uris = [cdi['ref'] for cdi in icon2i_ingestor_out['collected_data_info'] if cdi['variable'] == var]

            # DOC: Now we have the data source URIs, we can retrieve the data
            retrived_files = []
            for dsu in data_source_uris:
                if dsu.startswith('s3://'):
                    rf = os.path.join(self._tmp_data_folder, os.path.basename(dsu))
                    module_s3.s3_download(dsu, rf)
                    retrived_files.append(rf)
                else:
                    retrived_files.append(dsu)
                
            datasets = [xr.open_dataset(rf) for rf in retrived_files]
            dataset = xr.concat(datasets, dim='time')
            dataset = dataset.assign_coords(
                lat=np.round(dataset.lat.values, 6),
                lon=np.round(dataset.lon.values, 6),
            )
            dataset = dataset.sortby(['time', 'lat', 'lon'])

            # DOC: Filter the dataset by latitude, longitude, and time range
            def dataset_query(dataset, lat_range, lon_range, time_range):
                query_dataset = dataset.copy()
                if isinstance(lat_range, list) and len(lat_range) == 2:
                    query_dataset = query_dataset.sel(lat=slice(lat_range[0], lat_range[1]))
                elif isinstance(lat_range, (float, int)):
                    query_dataset = query_dataset.sel(lat=lat_range, method="nearest")

                if isinstance(lon_range, list) and len(lon_range) == 2:
                    query_dataset = query_dataset.sel(lon=slice(lon_range[0], lon_range[1]))
                elif isinstance(lon_range, (float, int)):
                    query_dataset = query_dataset.sel(lon=lon_range, method="nearest")

                if isinstance(time_range, list) and len(time_range) == 2:
                    query_dataset = query_dataset.sel(time=slice(time_range[0], time_range[1]))
                elif isinstance(time_range, str) or isinstance(time_range, datetime.datetime):
                    query_dataset = query_dataset.sel(time=time_range, method="nearest")

                return query_dataset
            dataset = dataset_query(dataset, lat_range, lon_range, [time_start, time_end])       
            
            variable_datasets[var] = dataset

            # return dataset
        
        return variable_datasets
    

    def create_timestamp_raster(self, variable, dataset, out):
        timestamps = [datetime.datetime.fromisoformat(str(ts).replace('.000000000','')).isoformat(timespec='seconds') for ts in dataset.time.values]
        
        if out is None:
            multiband_raster_filename = f'{_consts._DATASET_NAME}/{variable}/{_consts._DATASET_NAME}__{variable}__{timestamps[0]}.tif'
            multiband_raster_filepath = os.path.join(self._tmp_data_folder, multiband_raster_filename)
        else:
            multiband_raster_filepath = out
        
        xmin, xmax = dataset.lon.min().item(), dataset.lon.max().item()
        ymin, ymax = dataset.lat.min().item(), dataset.lat.max().item()
        nx, ny = dataset.dims['lon'], dataset.dims['lat']
        pixel_size_x = (xmax - xmin) / nx
        pixel_size_y = (ymax - ymin) / ny

        data = dataset.sortby('lat', ascending=False)[variable].values
        geotransform = (xmin, pixel_size_x, 0, ymax, 0, -pixel_size_y)
        projection = dataset.attrs.get('crs', 'EPSG:4326')
        
        g2n.Numpy2GTiffMultiBanda(
            data,
            geotransform,
            projection,
            multiband_raster_filepath,
            format="COG",
            save_nodata_as=-9999.0,
            metadata={
                'band_names': [ts.isoformat() for ts in timestamps],
                'type': '', # product.measure_type,  # !!!: To be defined
                'unit': '' # product.measure_unit   # !!!: To be defined
            }
        )
    
        return multiband_raster_filepath

    
    def run(
        self,
        variable: str,
        lat_range: tuple | list | None = None,
        long_range: tuple | list | None = None,
        time_range: tuple | list | None = None,
        out_format: str | None = None,
        bucket_source: str | None = None,
        bucket_destination: str | None = None,
        out: str | None = None,
        **kwargs
    ):
        
        """
        Run the retrieval process for ICON2I data.
        
        :param variable: The code of the variable to retrieve.
        :param lat_range: Latitude range as a tuple (min_lat, max_lat).
        :param lon_range: Longitude range as a tuple (min_lon, max_lon).
        :param time_range: Time range as a tuple (start_time, end_time).
        :param out_format: Output format (e.g., 'netcdf', 'json').
        :param bucket_source: Source S3 bucket for input data.
        :param bucket_destination: Destination S3 bucket for output data.
        :param out: Local output directory.
        :return: Dictionary with status and collected data information.
        """
        
        try:
            # DOC: Validate the arguments
            validated_args = self.argument_validation(
                variable=variable,
                lat_range=lat_range,
                long_range=long_range,
                time_range=time_range,
                out_format=out_format,
                bucket_source=bucket_source,
                bucket_destination=bucket_destination,
                out=out
            )
            variable = validated_args['variable']
            lat_range = validated_args['lat_range']
            long_range = validated_args['long_range']
            time_start = validated_args['time_start']
            time_end = validated_args['time_end']
            out_format = validated_args['out_format']
            bucket_source = validated_args['bucket_source']
            bucket_destination = validated_args['bucket_destination']
            out = validated_args['out']

            # DOC: Retrieve the ICON2I data
            variable_datasets = self.retrieve_icon2I_data(
                variable=variable,
                lat_range=lat_range,
                lon_range=long_range,
                time_start=time_start,
                time_end=time_end,
                bucket_source=bucket_source
            )

            # DOC: Retrieve the data and create timestamp rasters for each variable
            variables_timestamp_rasters_refs = dict()
            for var, dataset in variable_datasets.items():
                
                # DOC: Create timestamp raster
                timestamp_raster = self.create_timestamp_raster(
                    variable = var,
                    dataset = dataset,
                    out = out
                )
                variables_timestamp_rasters_refs[var] = timestamp_raster

                # DOC: If out is provided, store the data in the specified location
                if bucket_destination is not None:
                    bucket_uri = f"{bucket_destination}/{filesystem.justfname(timestamp_raster)}"
                    upload_status = module_s3.s3_upload(timestamp_raster, bucket_uri)
                    if not upload_status:
                        raise StatusException(StatusException.ERROR, f"Failed to upload data to bucket {bucket_destination}")
                    Logger.debug(f"Data stored in bucket: {bucket_uri}")
                    variables_timestamp_rasters_refs[var] = bucket_uri


            # DOC: Prepare outputs
            if bucket_destination is not None or out is not None:
                outputs = { 
                    'status': 'OK',
                    'collected_data_info': [
                        {
                            'variable': var,
                            'ref': ref,
                        }
                        for var, ref in variables_timestamp_rasters_refs.items()
                    ]
                }
            else:
                outputs = timestamp_raster
            
            return outputs

        except Exception as e:
            raise StatusException(StatusException.ERROR, f'Error during ICON2I retriever run: {str(e)}')
        
        finally:
            filesystem.garbage_folders(self._tmp_data_folder)
            Logger.debug(f'Cleaned up temporary data folder: {self._tmp_data_folder}')
            