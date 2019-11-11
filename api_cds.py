#import CMIP5 daily data on single levels
import cdsapi

c = cdsapi.Client()

c.retrieve(
    'projections-cmip5-daily-single-levels',
    {
        'ensemble_member':'r1i1p1',
        'format':'zip',
        'variable':[
            '2m_temperature','maximum_2m_temperature_in_the_last_24_hours','mean_precipitation_flux',
            'minimum_2m_temperature_in_the_last_24_hours'
        ],
        'model':'hadgem2_cc',
        'experiment':'pi_control',
        'period':'20091201-20141130'
    },
    'download.zip')

