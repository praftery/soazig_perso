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

c = SmapClient(base='http://new.openbms.org/backend',\
                               key=['WE4iJWG7k575AluJ9RJyAZs25UO72Xu0b4RA',\
                                    'SA2nYWuHrJxmPNK96pdLKhnSSYQSPdALkvnA'])
t = time.time()
source = 'Brower BACnet'
path_list_and = ['Brower', 'Field_Bus1']
path_list_or=[
             'BrowerAHU2/DA-T',
             'BrowerAHU2/OA-T',
             'BrowerAHU2/SF-value',
             'BrowerAHU2/SF-speed',
             'Plant/Condenser.CWP7-speed',
             'Plant/Condenser.CWP8-speed',
             'Plant/Condenser.CWS-T',
             'Plant/Condenser.CWR-T',
             'Plant/Condenser.HXR-T',
             'Plant/Condenser.HXS-T',
             'CW_Pump_1/Analog_Values.AV-8',
             'CW_Pump_7/Analog_Values.AV-8',
             'CW_Pump_2/Analog_Values.AV-8',
             'CW_Pump_8/Analog_Values.AV-8',
             'Cool_Tower_Fan/Analog_Values.AV-8',
             'HW_Pump_7/Analog_Values.AV-8',
             'HW_Pump_5/Analog_Values.AV-8'
             ]

restrict = " Metadata/SourceName = '%s' and "%source\
                 + ' and '.join(["Path ~ '%s'"] * len(path_list_and))\
                 %tuple(path_list_and) + " and ("\
                 + ' or '.join(["Path ~ '%s'"] * len(path_list_or)) \
                 %tuple(path_list_or) + ")"
tags = c.tags(restrict)

startDate = "01/01/2014"
endDate = "01/02/2014"
print "Start date: ", str(startDate), 
print "End date: ", str(endDate)
name = 'Brower_data_V3.csv' 
dt_format = '%Y-%m-%d %H:%M:%S'
query_data = 'apply window(first, field="hour") to data in ("' + startDate + '" , "' + str(endDate) + '") limit 10000000 where' + restrict
data = c.query(query_data)
N=len(data)
df = pd.DataFrame()
for i in range(N):
  d = np.array(data[i]['Readings'])
  if d.any():
    df['timestamp'] = d[:,0]
    df['datetime'] = [datetime.datetime.fromtimestamp(x/1000).strftime(dt_format) for x in d[:,0]]
    for p in path_list_or:
      if p in data[i]['Path']:
        try:
          df[p] = d[:,1]
        except:
          pdb.set_trace()
        print "Data for " + p + " downloaded."

df.to_csv(name)
