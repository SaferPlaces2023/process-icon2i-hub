import numpy as np

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

    _VARIABLE_CODE(_VARIABLES.TOTAL_PRECIPITATION): lambda data_cube: np.concatenate(([data_cube[0]], np.diff(data_cube, axis=0)), axis=0)

}