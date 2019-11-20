import api_cds
import total_precipitation
import rainseason_dummy
import os
import min_max_avg_temp
from datetime import datetime, timedelta
from concurrent import futures
import warnings
from collections import deque
from itertools import zip_longest
from Precipitation import transform_csv_from_netCDF as transform_tp
from temperature_csv import transform_csv_from_netCDF as transform_tt, grouper

path_tp2 = '/Users/meichen/Box/Disruption_Lab/Precipitation/'
# list_year1 = [x for x in os.listdir(path_tp) if len(x) == 4]
path_tt2 = '/Users/meichen/Box/Disruption_Lab/Temperature/'
# list_year2 = [x for x in os.listdir(path_tt) if len(x) == 4]
list_year = ['1998']
path_tp = '/Users/meichen/Box/Disruption_Lab/Precipitation/daily_precipitation/'
path_tt = '/Users/meichen/Box/Disruption_Lab/Temperature/daily_temp/'

months = range(1, 13)
def concTemp(months):
    year = '1998'
    for month in months:
        if month!= 1: break
        if len(str(month)) == 1:
            month = '0'+str(month)
        else: 
            month = str(month)
        if year == None:
            break
        
        command = "bash /Users/meichen/Desktop/uiuc/FA19/Disruption\ Lab/concate.sh " + year + " " + month + " 2m_temperature"
        print(command)
        #os.system(command)
        filename1 = path_tt+year+"/"+year+month+"_tt.nc"
        #filename2 = path_tt+year+"_tp.nc"
        #os.system("mv "+ filename1 + " " + filename2) #move to outer space
        transform_tt(filename1, path_tt2+year+month+"_tt.csv")
        
concTemp(months)
# n = 1
# os.system('export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES')
# executor = futures.ProcessPoolExecutor(n)
# f = [executor.submit(concTemp, group) 
#            for group in grouper(months, n)]