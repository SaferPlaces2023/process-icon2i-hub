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

from . import _consts
from ..cli.module_log import Logger
from ..utils import filesystem, module_s3
from ..utils.status_exception import StatusException


urllib3.disable_warnings()


class _ICON2IIngestor():
    """
    Class to retrieve data from ARPAV Precipitation sensors.
    """

    name = f'{_consts._DATASET_NAME}__Ingestor'

    # REF: https://meteohub.mistralportal.it/api/datasets/ICON_2I_SURFACE_PRESSURE_LEVELS/opendata
    
    _tmp_data_folder = os.path.join(os.getcwd(), name)

    def __init__(self):
        
        if not os.path.exists(self._tmp_data_folder):
            os.makedirs(self._tmp_data_folder)


    def get_avaliable_forecast_runs(self):
        
        def parse_avaliable_data(avaliable_data_response):
            avaliable_data = pd.DataFrame(avaliable_data_response.json())
            avaliable_data['forecast_run'] = avaliable_data.apply(lambda row: datetime.datetime.fromisoformat(f'{row.date}T{row.run}'), axis=1)
            return avaliable_data

        avaliable_data_response = requests.get(_consts._AVALIABLE_DATA_URL)
        if avaliable_data_response.status_code == 200:
            avaliable_data = parse_avaliable_data(avaliable_data_response)
        else:
            print('Error while requesting avaliable data endpoint')

        return avaliable_data
    
    
    def ping_avaliable_runs(self, forecast_datetime_runs):
        avaliable_data = self.get_avaliable_forecast_runs()
        avaliable_runs = avaliable_data.forecast_run.tolist()
        return all(fdr in avaliable_runs for fdr in forecast_datetime_runs)
    

    def argument_validation(self, **kwargs):
        """
        Validate the arguments passed to the processor.
        """

        print(f"Validating arguments: {kwargs}")

        variable = kwargs.get('variable', None)
        forecast_run = kwargs.get('forecast_run', None)
        bucket_destination = kwargs.get('bucket_destination', None)
        out_dir = kwargs.get('out_dir', None)

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
        
        if forecast_run is not None:
            if type(forecast_run) not in [str, list]:
                raise StatusException(StatusException.INVALID, 'Invalid input format for forecast_run parameter')
            if type(forecast_run) is str:
                forecast_run = [forecast_run]
            for irfr, rfr in enumerate(forecast_run):
                try:
                    rfr = datetime.datetime.fromisoformat(rfr)
                    if rfr.hour not in [0, 12] or rfr.minute != 0 or rfr.second != 0 or rfr.microsecond != 0:
                        raise StatusException(StatusException.INVALID, f'Invalid forecast run "{rfr.isoformat()}". Must be a valid 12h interval')
                    forecast_run[irfr] = rfr
                except Exception as err:
                    raise StatusException(StatusException.INVALID, f'Invalid forecast run "{rfr}. Must be a valid ISO format date string')
            forecast_run = [fr for fr in forecast_run if self.ping_avaliable_runs([fr])]
        else:
            avaliable_data = self.get_avaliable_forecast_runs()
            forecast_run = avaliable_data.forecast_run.tolist()

        if bucket_destination is not None:
            if type(bucket_destination) is not str:
                raise StatusException(StatusException.INVALID, 'bucket_destination must be a string')
            if not bucket_destination.startswith('s3://'):
                raise StatusException(StatusException.INVALID, 'bucket_destination must start with "s3://"')
            
        if out_dir is not None:
            if type(out_dir) is not str:
                raise StatusException(StatusException.INVALID, 'out must be a string')
            os.makedirs(out_dir, exist_ok=True)
        else:
            out_dir = self._tmp_data_folder

        return {
            'variable': variable,
            'requested_forecast_run': forecast_run,
            'bucket_destination': bucket_destination,
            'out': out_dir
        }
    

    def get_icon2I_data_filenames(self, forecast_datetime_runs):
        avaliable_data = self.get_avaliable_forecast_runs()
        forecast_runs_filenames = avaliable_data[avaliable_data.forecast_run.isin(forecast_datetime_runs)].filename.to_list()
        return forecast_runs_filenames
    

    def download_icon2I_data(self, forecast_datetime_runs):
        request_file_names = self.get_icon2I_data_filenames(forecast_datetime_runs)
        icon2I_file_paths = []
        for rf in request_file_names:
            response = requests.get(_consts._RETRIEVE_DATA_URL(rf), stream=True)
            if response.status_code == 200:
                rf_filename = os.path.join(self._tmp_data_folder, rf)
                with open(rf_filename, "wb") as grib_file:
                    for chunk in response.iter_content(chunk_size=8192):
                        grib_file.write(chunk)
                icon2I_file_paths.append(rf_filename)
            else:
                print(f'Request error {response.status_code} with file "{rf}"')
        return icon2I_file_paths


    def icon_2I_time_concat(self, grib_dss, variable):
        varaible_name = _consts._VARIABLES_DICT[variable]

        dss = []
        for ids, grib_ds in enumerate(grib_dss):
            
            # DOC: se ci sono altri dataset sucessivi prendo solo prime 12 h altrimenti tutto il forecast disponibile 72h (12 files)
            gmsg = [msg for msg in list(grib_ds) if msg.name==varaible_name][: 12 if ids < len(grib_dss)-1 else 72]

            grib_data = []

            ts = gmsg[0].validDate
            lat_range = gmsg[0].data()[1][:,0]
            lon_range = gmsg[0].data()[2][0,:]
            times_range = []

            for i,msg in enumerate(gmsg):
                if msg.name == varaible_name:
                    
                    # !!!: This was tested only with Total Precipitation messages
                    values, lats, lons = msg.data()
                    times_range.append(ts + datetime.timedelta(hours=i))

                    data = np.stack([np.where(d.data==9999.0, np.nan, d.data) for d in values])
                    grib_data.append(data)

            np_dataset = np.stack(grib_data)
            np_dataset = np.concatenate(([np_dataset[0]], np.diff(np_dataset, axis=0)), axis=0) # DOC: og data is cumulative
            ds = xr.Dataset(
                {
                    variable: (["time", "lat", "lon"], np_dataset.astype(np.float32))
                },
                coords={
                    "time": times_range,
                    "lat": lat_range,
                    "lon": lon_range
                }
            )
            dss.append(ds)

        ds = xr.concat(dss, dim='time')
        ds = ds.assign_coords(
            lat=np.round(ds.lat.values, 6).astype(np.float32),
            lon=np.round(ds.lon.values, 6).astype(np.float32),
        )
        ds = ds.sortby(['time', 'lat', 'lon'])
        ds[variable] = xr.where(ds[variable] < 0, 0, ds[variable])
        return ds

    def save_date_datasets(self, date_datasets, variable, out_dir, bucket_destination):
        """
        Save the date datasets to the specified output directory and upload to S3 if bucket_destination is provided.
        """
        date_dataset_refs = []
        for dt, ds in date_datasets:
            fn = f'{_consts._DATASET_NAME}__{variable}__{dt}.nc'
            fp = os.path.join(out_dir, fn)
            ds.to_netcdf(fp)
            if bucket_destination:
                uri = os.path.join(bucket_destination, fn)
                module_s3.s3_upload(fp, uri)
                date_dataset_refs.append((variable, dt, uri))
            else:
                date_dataset_refs.append((variable, dt, fp))
        return date_dataset_refs


    def get_single_date_dataset(self, dataset):
        dates = sorted(list(set(dataset.time.dt.date.values)))
        date_datasets = []
        for date in dates:
            subset = dataset.sel(time=dataset.time.dt.date == date)
            date_datasets.append((date, subset))
        
        # DOC: Discard datasets with only 12 values that refers to date before current date
        date_datasets = [(dt,ds) for dt,ds in date_datasets if not (dt < datetime.datetime.today().date() and len(ds.time) == 12)]
        return date_datasets

    def run(
        self,
        variable: str = None,
        forecast_run: str | None = None,
        out_dir: str = None,
        bucket_destination: str = None,
        **kwargs
    ):
        """
        Run the ingestor to retrieve and process data.
        
        :param forecast_run: The forecast run to retrieve data for.
        :param out: Output file path.
        :param bucket_destination: S3 bucket destination for the output file.
        """

        try:

            # DOC: Validate the arguments
            validated_args = self.argument_validation(
                variable=variable,
                forecast_run=forecast_run,
                out=out_dir,
                bucket_destination=bucket_destination
            )
            variable = validated_args['variable']
            forecast_run = validated_args['requested_forecast_run']
            bucket_destination = validated_args['bucket_destination']
            out_dir = validated_args['out']

            # DOC: Get the available forecast runs
            icon2I_file_paths = self.download_icon2I_data(forecast_run)

            # DOC: Extract each variable from the gribs
            variables_date_datasets_refs = []
            for var in variable:
                # DOC: Open gribs
                gribs = [pygrib.open(gf) for gf in icon2I_file_paths]

                # DOC: Concatenate the gribs into a single xarray dataset
                timeserie_dataset = self.icon_2I_time_concat(gribs, var)

                # DOC: Split the dataset into individual date datasets
                date_datasets = self.get_single_date_dataset(timeserie_dataset)

                # DOC: Save the date datasets to the output directory and upload to S3 if specified
                variable_date_datasets_refs = self.save_date_datasets(date_datasets, var, out_dir, bucket_destination)

                # DOC: Collect all variables+date datasets references
                variables_date_datasets_refs.extend(variable_date_datasets_refs)

            # DOC: Prepare the output
            outputs = {
                'status': 'OK',
                'collected_data_info': [
                    {
                        'variable': var,
                        'date': dt.isoformat(), 
                        'ref': ref
                    }
                    for var,dt,ref in variables_date_datasets_refs
                ]
            }

            return outputs

        except Exception as e:
            raise StatusException(StatusException.ERROR, f'Error during ICON2I ingestor run: {str(e)}')
        
        finally:
            filesystem.garbage_folders(self._tmp_data_folder)
            Logger.debug(f'Cleaned up temporary data folder: {self._tmp_data_folder}')