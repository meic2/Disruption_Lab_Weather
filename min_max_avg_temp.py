#!/usr/bin/env python
"""
Save as file calculate-daily-tp.py and run "python calculate-daily-tp.py".
  
Input file : tt_20170101-20170102.nc
Output file: daily-tt_20170101.nc
"""
import time, sys
from datetime import datetime, timedelta

from netCDF4 import Dataset, date2num, num2date
import numpy as np
import os 
 
#day = 20170101
def calculateTem(day, loc = '/Users/meichen/Box/ML Food Security/Raw Data/Temperature/daily_temp/'):
    d = datetime.strptime(str(day), '%Y%m%d')
    y = d.strftime('%Y')
    f_in = loc+'tt_%d-%s.nc' % (day, (d + timedelta(days = 1)).strftime('%Y%m%d'))
    f_out = loc+y+'/daily-tt_%d.nc' % day

    f_in_after = loc+y+'/tt_%d-%s.nc' % (day, (d + timedelta(days = 1)).strftime('%Y%m%d'))
    if y not in os.listdir(loc):
        print('make directory '+y)
        os.mkdir(loc+y)
    
    time_needed = []
    for i in range(1, 25):
        time_needed.append(d + timedelta(hours = i))
    
    with Dataset(f_in) as ds_src:
        var_time = ds_src.variables['time']
        time_avail = num2date(var_time[:], var_time.units,
                calendar = var_time.calendar)
    
        indices = []
        for tm in time_needed:
            a = np.where(time_avail == tm)[0]
            if len(a) == 0:
                sys.stderr.write('Error: precipitation data is missing/incomplete - %s!\n'
                        % tm.strftime('%Y%m%d %H:%M:%S'))
                sys.exit(200)
            else:
                #print('Found %s' % tm.strftime('%Y%m%d %H:%M:%S'))
                indices.append(a[0])
                #print(a[0])
    
        var_tt = ds_src.variables['t2m']
        #tt_values_set = False
        # for idx in indices:
        #     if not tt_values_set:
        #         data2 = []
        #         avgd = var_tt[idx, :, :]
        #         tt_values_set = True
        #     else:
        #         data2.append(var_tt[idx, :, :])
        #         avgd += var_tt[idx, :, :]
        #         #print(var_tt[idx, :, :])
        #print(data2)
        #traverse through all sublist to find max, min, avg value
        maxd = np.amax(var_tt[indices, :,:], axis = 0)
        mind = np.amin(var_tt[indices, :,:], axis = 0)
        avgd = np.mean(var_tt[indices, :,:], axis = 0)
        #print('Done with max, min, ', maxd)

        data_max = maxd
        data_min = mind
        data_avg = avgd
            
        with Dataset(f_out, mode = 'w', format = 'NETCDF3_64BIT_OFFSET') as ds_dest:
            # Dimensions
            for name in ['latitude', 'longitude']:
                dim_src = ds_src.dimensions[name]
                ds_dest.createDimension(name, dim_src.size)
                var_src = ds_src.variables[name]
                var_dest = ds_dest.createVariable(name, var_src.datatype, (name,))
                var_dest[:] = var_src[:]
                var_dest.setncattr('units', var_src.units)
                var_dest.setncattr('long_name', var_src.long_name)
    
            ds_dest.createDimension('time', None)
            var = ds_dest.createVariable('time', np.int32, ('time',))
            time_units = 'hours since 1900-01-01 00:00:00'
            time_cal = 'gregorian'
            var[:] = date2num([d], units = time_units, calendar = time_cal)
            var.setncattr('units', time_units)
            var.setncattr('long_name', 'time')
            var.setncattr('calendar', time_cal)
    
            # Variables temperature min
            var = ds_dest.createVariable('t2m_min', np.double, var_tt.dimensions)
            var[0, :, :] = data_min
            var.setncattr('units', var_tt.units)
            var.setncattr('long_name', var_tt.long_name)

            # Variables temperature max
            var2 = ds_dest.createVariable('t2m_max', np.double, var_tt.dimensions)
            var2[0, :, :] = data_max
            var2.setncattr('units', var_tt.units)
            var2.setncattr('long_name', var_tt.long_name)

            # Variables temperature avg
            var3 = ds_dest.createVariable('t2m_avg', np.double, var_tt.dimensions)
            var3[0, :, :] = data_avg
            var3.setncattr('units', var_tt.units)
            var3.setncattr('long_name', var_tt.long_name)
    
            # Attributes
            ds_dest.setncattr('Conventions', 'CF-1.6')
            ds_dest.setncattr('history', '%s %s'
                    % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    ' '.join(time.tzname)))
    
            print('Done! Daily temperature saved in %s' % f_out)

    
    os.system('mv '+f_in+' '+ f_in_after)
    print(f_in, "File Removed!")