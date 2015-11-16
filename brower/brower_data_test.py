"""
@author Soazig Kaam <soazig.kaam@berkeley.edu>

"""

from smap.archiver.client import SmapClient
from smap.util import periodicSequentialCall
from smap.contrib import dtutil
from smap.util import find
from datetime import timedelta, date

import numpy as np
import pandas as pd
import pdb
import csv
import shutil
import time
import pprint as pp
import datetime

def daterange(startDate, endDate):
  for n in range(int ((endDate - startDate).days)):
    yield startDate + timedelta(n)

def daterangelist(daterange):
  datelist = []
  for d in daterange:
    datelist.append(d.strftime("%m-%d-%Y"))
  return datelist 
  
c = SmapClient(base='http://new.openbms.org/backend',\
                               key=['WE4iJWG7k575AluJ9RJyAZs25UO72Xu0b4RA',\
                                    'SA2nYWuHrJxmPNK96pdLKhnSSYQSPdALkvnA'])
t = time.time()
#source = "Metadata/SourceName = 'Sutardja Dai Hall BACnet'"
source = 'Brower BACnet'
path_list_and = ['Brower', 'Field_Bus1']
path_list_or=[
             'BrowerAHU2/DA-T',
             'BrowerAHU2/OA-T'
#             'BrowerAHU2/SF-value',
#             'BrowerAHU2/SF-speed',
#             'Plant/Condenser.CWP7-speed',
#             'Plant/Condenser.CWP8-speed',
#             'Plant/Condenser.CWS-T',
#             'Plant/Condenser.CWR-T',
#             'Plant/Condenser.HXR-T',
#             'Plant/Condenser.HXS-T',
#             'CW_Pump_1/Analog_Values.AV-8',
#             'CW_Pump_7/Analog_Values.AV-8',
#             'CW_Pump_2/Analog_Values.AV-8',
#             'CW_Pump_8/Analog_Values.AV-8',
#             'Cool_Tower_Fan/Analog_Values.AV-8',
#             'HW_Pump_7/Analog_Values.AV-8',
#             'HW_Pump_5/Analog_Values.AV-8'
             ]

restrict2 = " Metadata/SourceName = '%s' and\n"%source\
                 + ' and\n '.join(["Path ~ '%s'"] * len(path_list_and))\
                 %tuple(path_list_and) + "and ("\
                 + ' or\n '.join(["Path ~ '%s'"] * len(path_list_or)) \
                 %tuple(path_list_or) + ")"
restrict = " Metadata/SourceName = '%s' and "%source\
                 + ' and '.join(["Path ~ '%s'"] * len(path_list_and))\
                 %tuple(path_list_and) + " and ("\
                 + ' or '.join(["Path ~ '%s'"] * len(path_list_or)) \
                 %tuple(path_list_or) + ")"
#path_list_or = ['ROOM_TEMP','CTL_FLOW_MIN']
#restrict = source + " and Path ~ 'S3-14' and (Path ~ 'ROOM_TEMP' or Path ~ 'CTL_FLOW_MIN')"
tags = c.tags(restrict)

#startDate = date(2014,1,1)
#endDate = date(2015,1,1)
#datelist = daterangelist(daterange(startDate, endDate))
#startDatelist = datelist[::3]
#startDatelist.append(datelist[-1])
#name = 'Brower_data_' + str(startDate) + '_' + str(endDate)+ '.csv' 
name = 'Brower_data.csv' 
#name = 'Test_S314.csv'

f = open(name ,'w')
headers = ['index', 'datetime'] + path_list_or
csv.writer(f).writerow(headers)
dt_format = '%Y-%m-%d %H:%M:%S'

#for n in range(len(startDatelist)-1):
startDate = "01/01/2014"
endDate = "01/06/2014"
print "Start date: ", startDate, 
print "End date: ", endDate
query_data = 'apply window(first, field="hour") to data in ("' + startDate + '" , "' + str(endDate) + '") limit 10000000 where' + restrict
data = c.query(query_data)
#start = dtutil.dt2ts(dtutil.strptime_tz(startDate, "%m/%d/%Y"))
#end = dtutil.dt2ts(dtutil.strptime_tz(endDate, "%m/%d/%Y"))
#data2 = c.data(restrict2, start, end)
#print data2[1][0][:,1]
#print data2[0:5]
N=len(data)
df = pd.DataFrame()
for i in range(N):
  print data[i]['Path']
  d = np.array(data[i]['Readings'])
  if d.any():
    df['datetime'] = d[:,0]
    new_date = [datetime.datetime.fromtimestamp(d[:,0]/1000).strftime(dt_format)]
    print new_date
    for p in path_list_or:
      if p in data[i]['Path']:
        try:
          df[p] = d[:,1]
        except:
         pdb.set_trace()

with open(name, 'a') as f:
  df.to_csv(f, header = False)

## converting utc to locatime
#new_date = [datetime.datetime.fromtimestamp(row[1]/1000).strftime(dt_format)]
#with open(name, 'wb') as f:
#csv_f = csv.wroter(f)
#new_date = []
#for row in csv_f:



