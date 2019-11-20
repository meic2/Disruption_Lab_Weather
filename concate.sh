
#!/bin/bash
set -eux
pwd
year=$1
month=$2
var=$3
tt="2m_temperature"

list=""
final=""

if [ "$var" == "$tt" ]; then
    final="/Users/meichen/Box/Disruption_Lab/Temperature/daily_temp/${year}/${year}${month}_tt.nc"
    list=$(ls /Users/meichen/Box/Disruption_Lab/Temperature/daily_temp/${year}/daily-tt_${year}${month}*.nc)
else
    final="/Users/meichen/Box/Disruption_Lab/Precipitation/daily_precipitation/${year}/${year}${month}_tp.nc"
    list=$(ls /Users/meichen/Box/Disruption_Lab/Precipitation/daily_precipitation/${year}/daily-tp_${year}${month}*.nc)
fi
#cd /Users/meichen/Box/Disruption_Lab/Temperature/daily_temp/${year}/

#echo list
#final="${year}_tp.nc"
echo ${final}
ncrcat ${list} ${final}



##need to average 
# ncks --mk_rec_dmn time tt_19980101-19980102.nc temp_19980101.nc
# ncra -d time,0,23,1 -v t2m temp_19980101.nc tt_19980101.nc
# ncra -y max -d time,0,23,1 -v t2m temp.nc out3.nc
# ncra -y min -d time,0,23,1 -v t2m temp.nc out4.nc
# rm temp.nc