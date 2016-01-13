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
               key=['XuETaff882hB6li0dP3XWdiGYJ9SSsFGj0N8'])
t = time.time()
source_energy = 'Sutardja Dai Hall Energy Data'
path_and = 'energy_data/variable_elec_cost/'
#path_list_energy = [
#                'total_cost',
#                'AH2_panel_fan_power',
#                'AH2_panel_fan_power_cost',
#                'chilled_water_AH2',
#                'chilled_water_AH2_cost',
#                'hot_water_AH2',
#                'hot_water_AH2_cost'
#                ]
path_list_energy = [
                'total_zone_load_AH2',
                'instantaneous_zone_load_S4-03'
                'instantaneous_zone_load_S4-05'
                'instantaneous_zone_load_S4-02'
                'instantaneous_zone_load_S4-18'
                'hot_water_S4-03'
                'hot_water_S4-05'
                'hot_water_S4-02'
                'hot_water_S4-18'
                ]

restrict = " Metadata/SourceName = '%s' and Path ~ '%s' and ("\
             %(source_energy, path_and) \
             + ' or '.join(["Path ~ '%s'"] * len(path_list_energy)) \
             %tuple(path_list_energy) + ")"

tags = c.tags(restrict)
frames = []
startDateF = date(2015, 11, 9)
startDate = startDateF
endDateF = date(2015, 12, 26)
delta = datetime.timedelta(days=2)
endDate = startDate + delta
while endDate <= endDateF:
  print "Start date: ", startDate
  print "End date: ", endDate

  #startDate = "11/09/2015 00:00"
  #endDate = "12/26/2015 00:00"
  #start = dtutil.dt2ts(dtutil.strptime_tz("08-28-2015", "%m-%d-%Y"))
  #end   = dtutil.dt2ts(dtutil.strptime_tz("08-29-2015", "%m-%d-%Y"))
  #data = c.data(restrictall, start, end)
  #pp.pprint(data)
  #pdb.set_trace()
  
  dt_format = '%Y-%m-%d %H:%M:%S'
  query_data = 'apply window(first, field=\"minute\", width=3) to data in ("' + startDate.strftime("%m/%d/%Y") + '" , "' + endDate.strftime("%m/%d/%Y") + '") where' + restrict
  data = c.query(query_data)
  #data = c.data(restrictall, start, end, limit=1000)
  
  N=len(data)
  df = pd.DataFrame()
  d = np.array(data[0]['Readings'])
  df['timestamp'] = d[:,0]
  dt_format = '%Y-%m-%d %H:%M:%S'
  df['datetime'] = [datetime.datetime.fromtimestamp(x/1000).strftime(dt_format)
                    for x in d[:,0]]
  for i in range(N):
    u = data[i]['uuid']
    d = np.array(data[i]['Readings'])
    if d.any():
      tag_path = [tag['Path'] for tag in tags if tag['uuid'] == u][0]
      try:
        for p in path_list_energy:
          if p in tag_path:
            df[p] = d[:,1]
            print " Dwnl data for " + p
      except:
        print "Could not dwn data for : ", p
  df.to_csv('../csv_output/Energy/Energy2_TAV_' + startDate.strftime("%Y%m%d")\
    + '-' + endDate.strftime("%Y%m%d") + '.csv')
  frames.append(df)
  startDate = endDate
  endDate += delta
  dffull = pd.concat(frames)
name = '../csv_output/Energy/All_energy2_data_%s.csv' %(startDate.strftime("%Y%m%d"))
dffull.to_csv(name)
pdb.set_trace()
