import numpy as np
import xarray as xr

_DATASET_NAME = 'ICON_2I_SURFACE_PRESSURE_LEVELS'

_BASE_URL = 'https://meteohub.agenziaitaliameteo.it/api'
_AVALIABLE_DATA_URL = f'{_BASE_URL}/datasets/{_DATASET_NAME}/opendata'
_RETRIEVE_DATA_URL = lambda data_filename: f'{_BASE_URL}/opendata/{data_filename}'


class _VARIABLES:
    """
    Class to hold the constants for the ICON2I variables.
    """
    TEMPERATURE = '2 metre temperature'
    DEWPOINT_TEMPERATURE = '2 metre dewpoint temperature'
    U_WIND_COMPONENT = '10 metre U wind component'
    V_WIND_COMPONENT = '10 metre V wind component'
    TOTAL_CLOUD_COVER = 'Total Cloud Cover'
    TEMPERATURE_G = 'Temperature (G)'
    SNOW_DEPTH_WATER_EQUIVALENT = 'Snow depth water equivalent'
    PRESSURE_REDUCED_TO_MSL = 'Pressure Reduced to MSL'
    TOTAL_PRECIPITATION = 'Total Precipitation'


_VARIABLES_LIST = [attr for attr in dir(_VARIABLES) if not attr.startswith('_')]

_VARIABLE_CODE = lambda variable: variable.replace(' ', '_').lower()
_VARIABLES_DICT =  { _VARIABLE_CODE(variable): _VARIABLES.__dict__[variable] for variable in _VARIABLES_LIST }

_DATA_CUBE_PROCESSING = {
    _VARIABLE_CODE("TOTAL_PRECIPITATION"): lambda data_cube: np.concatenate(([data_cube[0]], np.diff(data_cube, axis=0)), axis=0)
}

class _DERIVED_VARIABLES:
    """
    Class to hold some variables that can be computed from _VARIABLES
    """
    WIND_SPEED = list(map(_VARIABLE_CODE, ["U_WIND_COMPONENT", "V_WIND_COMPONENT"]))
    WIND_DIRECTION = list(map(_VARIABLE_CODE, ["U_WIND_COMPONENT", "V_WIND_COMPONENT"]))

_DERIVED_VARIABLES_LIST = [attr for attr in dir(_DERIVED_VARIABLES) if not attr.startswith('_')]
_DERIVED_VARIABLES_DICT =  { _VARIABLE_CODE(variable): _DERIVED_VARIABLES.__dict__[variable] for variable in _DERIVED_VARIABLES_LIST }

def compute_wind_speed(wind_u, wind_v):
    ds_wu = xr.open_dataset(wind_u)
    ds_wv = xr.open_dataset(wind_v)
    ds_wind_speed = xr.Dataset(
        data_vars=dict(
            wind_speed = (["time", "lat", "lon"], np.sqrt(ds_wu.u_wind_component**2 + ds_wv.v_wind_component**2))
        ),
        coords = dict(
            time = ds_wu.time,
            lat = ds_wu.lat,
            lon = ds_wu.lon,
        )
    )
    return ds_wind_speed

def compute_wind_direction(wind_u, wind_v):
    ds_wu = xr.open_dataset(wind_u)
    ds_wv = xr.open_dataset(wind_v)
    ds_wind_direction = xr.Dataset(
        data_vars=dict(
            wind_direction = (["time", "lat", "lon"], np.arctan2(ds_wv.v_wind_component, ds_wu.u_wind_component) * (180 / np.pi))
        ),
        coords = dict(
            time = ds_wu.time,
            lat = ds_wu.lat,
            lon = ds_wu.lon,
        )
    )
    return ds_wind_direction

#ds['wind_direction'] = np.arctan2(ds.wind_v, ds.wind_u) * (180 / np.pi)

_DERIVED_VARIABLES_COMPUTE = {
    _VARIABLE_CODE("WIND_SPEED"): compute_wind_speed,
    _VARIABLE_CODE("WIND_DIRECTION"): compute_wind_direction,
}