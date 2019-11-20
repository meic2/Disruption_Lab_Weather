######### process the file to csv format
import datetime as dt  # Python standard library datetime  module
import numpy as np
import netCDF4  
#import matplotlib.pyplot as plt
import pandas as pd

from concurrent import futures

import warnings
from collections import deque 
from itertools import zip_longest

def grouper(iterable, n, fillvalue=None):
    """Collect data into fixed-length chunks or blocks.

        >>> list(grouper('ABCDEFG', 3, 'x'))
        [('A', 'B', 'C'), ('D', 'E', 'F'), ('G', 'x', 'x')]

    """
    if isinstance(iterable, int):
        warnings.warn(
            "grouper expects iterable as first parameter",
            DeprecationWarning,
        )
        n, iterable = iterable, n
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)


# find closest index to specified value
def near(array,value):
    idx=(abs(array-value)).argmin()
    return idx

from collections import OrderedDict

#function read from local netCDF raw data file and return the formatted csv file
#@path_input: local raw data file format
#@path_output: the path to write the csv 
def transform_csv_from_netCDF(path_input, path_output):
    nc1 = netCDF4.Dataset(path_input, index_col = False)
    print("START TRANSFORMING ")
    hhid = pd.read_csv("/Users/meichen/Desktop/uiuc/FA19/Disruption Lab/HICPS coordinates.csv", index_col = False)
    length = range(len(hhid))
    #res_final = pd.DataFrame(columns=['date_mmddyy', 'month', 'year', 'HHID', 'Lattitude', 'Longtitude','avg_temperature','min_temperature','max_temperature'])
    res = []

    ix_set = set([])
    iy_set = set([])

    lat = nc1.variables['latitude'][:]
    lon = nc1.variables['latitude'][:]
    time_var = nc1.variables['time']
    dtime = netCDF4.num2date(time_var[:],time_var.units)
    tim = dtime[:,]

    vname1 = 't2m_max'
    vname2 = 't2m_min'
    vname3 = 't2m_avg'
    var1 = nc1.variables[vname1]
    var2 = nc1.variables[vname2]
    var3 = nc1.variables[vname3]
   

    for row in length:
        print(row)
        hhid_lat = hhid["latitude"][row]
        hhid_lon = hhid["longitude"][row]
        hhid_hhid = hhid['HHID'][row]
        #print(hhid_lat, hhid_lon)
        
        # specify some location to extract time series
        lati = hhid_lat; loni = hhid_lon

        # Find nearest point to desired location (could also interpolate, but more work)
        ix = near(lon, loni)
        iy = near(lat, lati)
        
        ### if there are overlap between different coords, print the value and continue
        if ix in ix_set or iy in iy_set:
            print(lon[ix], lat[iy], hhid_hhid)
            continue

        #Specify the exact time period you want:
        #start = dt.datetime(2009,12,1,0,0,0)
        #stop = dt.datetime(2014,11,3,0,0,0)
        #20091201-20141130 -- current time span
        
        #istart = netCDF4.date2index(start,time_var,select='nearest')# -10
        #istop = netCDF4.date2index(stop,time_var,select='nearest')

        # Get all time records of variable [vname] at indices [iy,ix]
        #print(var.dimesions)
        
        hs1 = var1[:,iy,ix]
        hs2 = var2[:,iy,ix]
        hs3 = var3[:,iy,ix]
        #var[istart:istop,iy,ix]

        #dtime[istart:istop,]
        # Create Pandas time series object
        ts1 = pd.Series(hs1,index=tim,name=vname1)
        ts2 = pd.Series(hs2,index=tim,name=vname2)
        ts3 = pd.Series(hs3,index=tim,name=vname3)
        #print(ts)
        
        rain = 0 
        for s in range(len(ts1)):
            max_temp = ts1[s]
            min_temp = ts2[s]
            avg_temp = ts3[s]
            # if ts[s] > 0:
            #     rain += ts[s]
            # else:
            #     #print()
            #     rain = 0
            year = tim[s].strftime("%Y")
            month = tim[s].strftime("%m")
            datetime = tim[s].strftime("%m%d%Y")
            #temp = [datetime, month, year, hhid_hhid, lon[ix], lat[iy], rain]
            dict1 = {}
            dict1.update({"date_mmddyy": datetime, "month": month, "year": year, "Lattitude": lon[ix],"Longtitude":lat[iy], "HHID": hhid_hhid, \
                'avg_temperature': avg_temp,'min_temperature': min_temp,'max_temperature': max_temp})
            res.append(dict1)
            #break
        #break
    
    res_final = pd.DataFrame(res)
    res_final.to_csv(path_output, index = False)


if __name__ == "__main__":
    path_tt = '/Users/meichen/Box/Disruption_Lab/Temperature/daily_temp/'   

    path_input = "/Users/meichen/Box/Disruption_Lab/Temperature/daily_temp/1998_tt.nc"
    path_output = "/Users/meichen/Box/Disruption_Lab/Temperature/1998_tt.csv"
    hhid = pd.read_csv("HICPS coordinates.csv", index_col = False)
    length = range(len(hhid))
    #transform_csv_from_netCDF(path_input, path_output, length)

    n = 10
    executor = futures.ThreadPoolExecutor(n)
    f1 = [executor.submit(transform_csv_from_netCDF, path_input, path_output, group) 
           for group in grouper(length, n)]
    futures.wait(f1)
    data = []
    for f in futures.as_completed(f1):
        temp = f.result()
        print(temp)
        data.append(temp)
    
    res_final = pd.DataFrame(data)
    res_final.to_csv(path_output, index = False)





    

