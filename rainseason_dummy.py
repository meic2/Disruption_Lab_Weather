import pandas as pd
import numpy as np

#read from local csv file, create firstday, lastday, and dummy varaibles rain_season and write file back to the local 
#input: input csv file
#output: output path to write file 
#threashold: default at 0.0001, the threshold to distinguish if it is a valid rainfall 
def rain_season(path_input, path_output, threashold = 0.0001):
    file = pd.read_csv(path_input, index_col = False)
    if 'Unnamed: 0' in file.columns:
        del file['Unnamed: 0']
    
    #iterate through the rainday list, create a dummy list to indicate whether the rainfalls 
    list_raindays = []
    prev_rain = 0
    rainfall_list  = list(file['rainfall'])
    for i in range(len(rainfall_list)):
        if rainfall_list[i] > 0.00001: #threshold 
            prev_rain +=1
            list_raindays.append(prev_rain)
        else: 
            prev_rain = 0
            list_raindays.append(prev_rain)

    #initialize all zero to the list
    list_firstday = [0 for i in range(len(list_raindays))]
    list_lastday = [0 for i in range(len(list_raindays))]
    list_rainseason = [0 for i in range(len(list_raindays))]

    #find the firstday and lastday of the rainseason 
    list_file_HHID = list(file['HHID'])
    for i in range(len(list_raindays)-4):
        if list_raindays[i+4] == 5:
            if list_file_HHID[i] != list_file_HHID[i+4]:
                continue 
            list_firstday[i] = 1
            firstday_index = i

            #assumption: rainseason happens if the rainfall last for over five days and stay at the same address
            while i < len(list_raindays) and list_raindays[i] > 0 \
            and list_file_HHID[i] == list_file_HHID[firstday_index]:
                list_rainseason[i] = 1
                i+= 1
            list_lastday[i -1] = 1

    #concatenate the list to the dataframe
    file['firstday'] = list_firstday
    file['lastday'] = list_lastday
    file['rainy_season'] = list_rainseason

    file.to_csv(path_output,index = False)
    return file



if __name__ == "__main__":
    path_input = '2001-2005_precipitation.csv'
    path_output = '2001-2005_precipitation_rainseason.csv'
    rain_season(path_input, path_output)

