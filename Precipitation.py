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



###### unzip all the files
import zipfile
archive = zipfile.ZipFile('download.zip', 'r')

archive.extractall()




######### process the file to csv format
import datetime as dt  # Python standard library datetime  module
import numpy as np
import netCDF4  
import matplotlib.pyplot as plt
import pandas as pd

nc1 = netCDF4.Dataset("pr_day_HadGEM2-CC_piControl_r1i1p1_20091201-20141130.nc")


# find closest index to specified value
def near(array,value):
    idx=(abs(array-value)).argmin()
    return idx

from collections import OrderedDict
hhid = pd.read_csv("HICPS coordinates.csv")
res_final = pd.DataFrame(columns=['date_mmddyy', 'month', 'year', 'HHID', 'Lattitude', 'Longtitude','rainfall'])
res = []

ix_set = set([])
iy_set = set([])


lat = nc1.variables['lat'][:]
lon = nc1.variables['lon'][:]
time_var = nc1.variables['time']
dtime = netCDF4.num2date(time_var[:],time_var.units)

for row in range(len(hhid)):
    print(row)
    hhid_lat = hhid["latitude"][row]
    hhid_lon = hhid["longitude"][row]
    hhid_hhid = hhid['HHID'][row]
    #print(hhid_lat, hhid_lon)
    
    # specify some location to extract time series
    lati = hhid_lat; loni = hhid_lon;

    # Find nearest point to desired location (could also interpolate, but more work)
    ix = near(lon, loni)
    iy = near(lat, lati)
    #print(ix, iy)
    if ix in ix_set or iy in iy_set:
        print(lon[ix], lat[iy], hhid_hhid)
        continue

    #Specify the exact time period you want:
    #start = dt.datetime(2009,12,1,0,0,0)
    #stop = dt.datetime(2014,11,3,0,0,0)
    #20091201-20141130 -- current time span
    
    #istart = netCDF4.date2index(start,time_var,select='nearest')# -10
    #istop = netCDF4.date2index(stop,time_var,select='nearest')
#     print (istart,istop)
    
    # Get all time records of variable [vname] at indices [iy,ix]
    vname = 'pr'
    #vname = 'surf_el'
    var = nc1.variables[vname]
    #print(var.dimesions)
    
    hs = var[:,iy,ix]
    #var[istart:istop,iy,ix]
    tim = dtime[:,]
    #dtime[istart:istop,]
    # Create Pandas time series object
    ts = pd.Series(hs,index=tim,name=vname)
    #print(ts)
    
    rain = 0 
    for s in range(len(ts)):
        if ts[s] > 0:
            rain += ts[s]
        else:
            #print()
            rain = 0
        year = tim[s].strftime("%Y")
        month = tim[s].strftime("%m")
        datetime = tim[s].strftime("%m%d%Y")
        #temp = [datetime, month, year, hhid_hhid, lon[ix], lat[iy], rain]
        dict1 = OrderedDict()
        dict1.update({"date_mmddyy": datetime, "month": month, "year": year, "Lattitude": lon[ix],"Longtitude":lat[iy], "HHID": hhid_hhid, "rainfall": rain})
        res.append(dict1)


res_final.to_csv('2007-2012_precipitation.csv')
