#import CMIP5 daily data on single levels
import cdsapi
import time
from datetime import datetime, timedelta
import logging

logger = logging.Logger('catch_all')

list_var = ['total_precipitation','2m_temperature']

def api_approach(day, var, loc):
    d = datetime.strptime(str(day), '%Y%m%d')
    #print(str(d))
    d2 = d + timedelta(days = 1)
    day2 = d2.strftime('%Y%m%d')
    days = [d.strftime('%d'), d2.strftime('%d')]
    year = d.strftime('%Y')
    month = d.strftime('%m')
    if int(day2) - int(day) > 1000: #across year
        year = [d.strftime('%Y'), d2.strftime('%Y')]
    if int(day2) - int(day) > 1: #across month
        month = [d.strftime('%m'), d2.strftime('%m')]
    c = cdsapi.Client()
    while(True):
        try:
            r = c.retrieve(
                'reanalysis-era5-single-levels', {
                        'variable'    :  var,
                        'product_type': 'reanalysis',
                        'year'        : year,
                        'month'       : month,
                        'day'         : days, #has to be two days 
                        'time'        : [
                            '00:00','01:00','02:00',
                            '03:00','04:00','05:00',
                            '06:00','07:00','08:00',
                            '09:00','10:00','11:00',
                            '12:00','13:00','14:00',
                            '15:00','16:00','17:00',
                            '18:00','19:00','20:00',
                            '21:00','22:00','23:00'
                        ],
                        'format'      : 'netcdf'
                })
            if var == '2m_temperature':
                filename = loc+'tt_'+str(day)+'-'+day2+'.nc'
                r.download(filename)
            else:
                filename = loc+'tp_'+str(day)+'-'+day2+'.nc'
                r.download(filename)
            break
        except Exception as e:
            time.sleep(5)
            logger.error("ERROR in API_CDS: " + str(e))
            #print("ERROR in API_CDS")






#     r = c.retrieve(
#     'reanalysis-era5-single-levels', {
#             'variable'    : 'total_precipitation',
#             'product_type': 'reanalysis',
#             'year'        : '2017',
#             'month'       : '01',
#             'day'         : ['01', '02'],
#             'time'        : [
#                 '00:00','01:00','02:00',
#                 '03:00','04:00','05:00',
#                 '06:00','07:00','08:00',
#                 '09:00','10:00','11:00',
#                 '12:00','13:00','14:00',
#                 '15:00','16:00','17:00',
#                 '18:00','19:00','20:00',
#                 '21:00','22:00','23:00'
#             ],
#             'format'      : 'netcdf'
#     })
# r.download('tp_20170101-20170102.nc')