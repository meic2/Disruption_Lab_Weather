import cdsapi
from coords import coords

c = cdsapi.Client()

for key in coords:
    c.retrieve(
    'seasonal-monthly-single-levels',
    {
        'originating_centre':'ecmwf',
        'system':[
            '4','5'
        ],
        'variable':'total_precipitation',
        'product_type':'monthly_mean',
        'year':[
            '2013','2014','2015',
            '2016'
        ],
        'month':[
            '01','02','03',
            '04','05','06',
            '07','08','09',
            '10','11','12'
        ],
        'leadtime_month':'1',
        'format':'grib',
        'grid'    : key + '/' + coords[key],

    },
    'precipitation_' + key + '.grib')
    # c.retrieve(
    #     'reanalysis-era5-land',
    #     {
    #         'variable':[
    #             'total_precipitation'
    #         ],
    #         'year':[
    #             "2019"
    #         ],
    #         'month':[
    #             '01','02','03',
    #             '04','05','06',
    #             '07','08','09',
    #             '10','11','12'
    #         ],
    #         'day':[
    #             '01','02','03','04','05','06',
    #             '07','08','09','10','11','12',
    #             '13','14','15','16','17','18',
    #             '19','20','21','22','23','24',
    #             '25','26','27','28','29','30',
    #             '31'
    #         ],
    #         'time':[
    #             "00:00"
    #         ],
    #         'format':'grib',
    #         'grid'    : key + '/' + coords[key],
    #         # 'latitude': key,
    #         # 'longitude': coords[key],
    #     },
    #     'download.grib')

#  "2001", "2002", "2003","2004","2005","2006","2007","2008",
#                 "2009", "2010", "2011","2012","2013","2014","2015","2016",
#                 "2017", "2018", "2019" 

# '2m_temperature',

# ,"01:00","02:00","03:00","04:00","05:00","06:00",
#                 "07:00","08:00","09:00","10:00","11:00","12:00","13:00",
#                 "14:00","15:00","16:00","17:00","18:00","19:00","20:00",
#                 "21:00","22:00","23:00"