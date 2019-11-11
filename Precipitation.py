
######### process the file to csv format
import datetime as dt  # Python standard library datetime  module
import numpy as np
import netCDF4  
import matplotlib.pyplot as plt
import pandas as pd


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

    hhid = pd.read_csv("HICPS coordinates.csv", index_col = False)
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


    res_final.to_csv(path_output, index = False)


if __name__ == "__main__":
    path_input = "pr_day_HadGEM2-CC_piControl_r1i1p1_20091201-20141130.nc"
    path_output = "2010-2015_precipitation.csv"
    transform_csv_from_netCDF(path_input, path_output)


    

