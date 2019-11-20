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



def concat_and_transform1(list_year):
    for year in list_year:
        if year == None:
            break
        for month in range(1, 13):
            if len(str(month)) == 1:
                month = '0'+str(month)
            else: 
                month = str(month)
            if year == None:
                break
            #print('total_precipitation', year)
            command = "bash /Users/meichen/Desktop/uiuc/FA19/Disruption\ Lab/concate.sh " + year + " " + month + " total_precipitation"
            print(command)
            os.system(command)
            filename1 = path_tp+year+"/"+year+month+"_tp.nc"
            #filename2 = path_tt+year+"_tp.nc"
            #os.system("mv "+ filename1 + " " + filename2) #move to outer space
            transform_tp(filename1, path_tp2+year+month+"_tp.csv")


def concat_and_transform2(list_year):
    for year in list_year:
        if year == None:
            break
        for month in range(1, 13):
            if len(str(month)) == 1:
                month = '0'+str(month)
            else: 
                month = str(month)
            if year == None:
                break
            
            command = "bash /Users/meichen/Desktop/uiuc/FA19/Disruption\ Lab/concate.sh " + year + " " + month + " 2m_temperature"
            print(command)
            os.system(command)
            filename1 = path_tt+year+"/"+year+month+"_tt.nc"
            #filename2 = path_tt+year+"_tp.nc"
            #os.system("mv "+ filename1 + " " + filename2) #move to outer space
            transform_tt(filename1, path_tt2+year+month+"_tt.csv")

        # path_tt2 = '/Users/meichen/Box/Disruption_Lab/Temperature/'
        # print("2m_temperature", year)
        # os.system("bash concate_tp.sh " + str(year) + " " + "2m_temperature")
        # filename1 = path_tt+year+"/"+year+"_tt.nc"
        # #filename2 = path_tt+year+"_tt.nc"
        # #os.system("mv "+ filename1 + " " + filename2) #move to outer folder
        # transform_tt(filename1, path_tt2+year+"_tt.csv")


# executor2 = futures.ThreadPoolExecutor(n)
# f2 = [executor2.submit(concat_and_transform2, group) 
#            for group in grouper(list_year2, n)]






#list_var = ['total_precipitation','2m_temperature']
#from 19980101 to 20091231



def download_daily_data_tt(list_days, path_tt = '/Users/meichen/Box/Disruption_Lab/Temperature/daily_temp/'):
    var = '2m_temperature'       
    #if var != '2m_temperature': continue
    for day in list_days:
        #pre_filename = 'tt_20010112-20010113.nc'
        if day == None: 
            break
    
        #pass
        api_cds.api_approach(day, var, path_tt)
        print("start calculate daily temperature")
        min_max_avg_temp.calculateTem(day, path_tt)
        d = datetime.strptime(str(day), '%Y%m%d')
        print(d)
    # except:   
    #     print("MultiProcess Issue: ERROR With Ite")

def download_daily_data_tp(list_days, path_tp = '/Users/meichen/Box/Disruption_Lab/Precipitation/daily_precipitation/'):
    var = 'total_precipitation'       
    for day in list_days: 
        if day == None: 
            print("whaaaaat")
            break
        try:
            #pass,
            #print("start api")
            api_cds.api_approach(day, var, path_tp)
            total_precipitation.calculateTP(day, path_tp)
            d = datetime.strptime(str(day), '%Y%m%d')
            print(d)
        except:
            print("MultiProcess Issue: ERROR With Ite")
#0507
#0618
#0701
#0915


if __name__ == "__main__":
    
    IS_TP = 0
    day = 19980823#20030529


    list_days = []
    
    while(day < 20081231):
        list_days.append(day)
        d = datetime.strptime(str(day), '%Y%m%d')
        d2 = d + timedelta(days = 1)
        day = int(d2.strftime('%Y%m%d')) 
    print(list_days) 
    # list_days = []
    # for file in os.listdir(path_tt):
    #     if '.nc' not in file: continue
    #     list_days.append(file[3:11])

    #download_daily_data_tt(list_days)
    path_tp = '/Users/meichen/Box/Disruption_Lab/Precipitation/daily_precipitation/'
    path_tt = '/Users/meichen/Box/Disruption_Lab/Temperature/daily_temp/'

    path_tp2 = '/Users/meichen/Box/Disruption_Lab/Precipitation/'
    list_year1 = [x for x in os.listdir(path_tp) if len(x) == 4]
    path_tt2 = '/Users/meichen/Box/Disruption_Lab/Temperature/'
    list_year2 = [x for x in os.listdir(path_tt) if len(x) == 4]
    print(list_year2)


    n = 5
    if IS_TP:
        os.system('export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES')
        executor = futures.ThreadPoolExecutor(n)#ProcessPoolExecutor(n)
        f = [executor.submit(download_daily_data_tp, group) 
                for group in grouper(list_days, n)]
        futures.wait(f)

        print("TIME TO CONCATENATE FILES ")
        ####### concatenate all tp file 

        executor = futures.ThreadPoolExecutor(n)
        f1 = [executor.submit(concat_and_transform1, group) 
                for group in grouper(list_year1, n)]

    else:
        executor2 = futures.ThreadPoolExecutor(n)
        os.system('export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES')
        f2 = [executor2.submit(download_daily_data_tt, group) 
                for group in grouper(list_days, n)]

        futures.wait(f2)

        print("TIME TO CONCATENATE FILES ")
        ####### concatenate all tp file 

        executor2 = futures.ThreadPoolExecutor(n)
        f2 = [executor2.submit(concat_and_transform2, group) 
                for group in grouper(list_year2, n)]



    
                



