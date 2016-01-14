"""
@author Soazig Kaam <soazig.kaam@berkeley.edu>

"""
from smap.archiver.client import SmapClient
from smap.util import periodicSequentialCall
from smap.contrib import dtutil
from smap.util import find
from datetime import timedelta, date
from functools import reduce

import sys, os, pdb
import numpy as np
import pandas as pd
import csv
import shutil
import time
import pprint as pp
import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), "../functions/"))
import tav_graphs_functions

c = SmapClient(base='http://new.openbms.org/backend',\
               key=['XuETaff882hB6li0dP3XWdiGYJ9SSsFGj0N8'])
t = time.time()
source = 'Sutardja Dai Hall BACnet'
points = ['CTL_FLOW_MAX', 'CTL_FLOW_MIN', 'AIR_VOLUME'] 
#zones_names = ['S4-18', 'S4-03', 'S4-05', 'S4-09']
full_frames = []
not_zones = ['S1-11', 'S1-12', 'S1-21', 'S6-21', 'S7-17', 'S7-18','S7-19', 'S7-20', 'S7-21']
for f in [str(f) for f in [3,4,5,6,7,2,1]]:
#for f in [str(f) for f in [1]]:
  floor_frames = []
  print "\nStarted process for Floor: ", f
  zones_names = ['S%s-%s' %(f, str(z).zfill(2)) for z in range(1,22)]
  zones_names = [z for z in zones_names if z not in not_zones]
  #pdb.set_trace()
  for zone_name in zones_names:
    print "\nStarted process for zone: ", zone_name
    zone_frames = []
    startF = date(2015,10,15)
    start = startF
    delta = datetime.timedelta(days=3)
    end = start + delta
    endF = date(2015, 11, 10)
    while start != end: 
      startDate = start.strftime("%m/%d/%Y %H:%M")
      endDate = end.strftime("%m/%d/%Y %H:%M")
      print "Start date: ", startDate
      print "End date: ", endDate
      restrict = tav_graphs_functions.restrict(source, [zone_name], points) 
      window='apply window(first, field=\"minute\", width=3) to' 
      query_data = tav_graphs_functions.query_data(c, restrict, startDate, endDate, win=window)
      tags = query_data[0]
      data = query_data[1]
      df = tav_graphs_functions.data_frame(data, tags, points, mode='Path')
      zone_frames.append(df)
      start = end
      end += min(delta, (endF - end))
    #pdb.set_trace()
    df_zone = pd.concat(zone_frames)
    floor_frames.append(df_zone)
  #pdb.set_trace()
  df_floor = reduce(lambda x,y: \
             pd.merge(x, y, on=['timestamp','datetime']),floor_frames)
  floor_path = '../csv_output/Floor2014/' + 'Floor%s_airflow' %(str(f).zfill(2))  \
      + '%s-%s'%(startF.strftime("%Y%m%d"), endF.strftime("%Y%m%d"))\
      + '.csv' 
  df_floor.to_csv(floor_path)
  df_aug = tav_graphs_functions.data_ratios(df_floor, points) 
  floor_ratio_path = '../csv_output/Floor2014/' + 'Floor%s_ratios' %(str(f).zfill(2))  \
      + '%s-%s'%(startF.strftime("%Y%m%d"), endF.strftime("%Y%m%d"))\
      + '.csv' 
  df_aug.to_csv(floor_ratio_path)
  full_frames.append(df_floor)
pdb.set_trace()
df_full = reduce(lambda x,y: \
             pd.merge(x, y, on=['timestamp','datetime']),full_frames)
full_path = '../csv_output/Full2014/TAV_airflow' \
      + '%s-%s'%(startF.strftime("%Y%m%d"), endF.strftime("%Y%m%d"))\
      + '.csv'
df_full.to_csv(full_path)
df_full_aug = tav_graphs_functions.data_ratios(df_full, points) 
full_ratio_path = '../csv_output/Full2014/TAV_ratios' \
      + '%s-%s'%(startF.strftime("%Y%m%d"), endF.strftime("%Y%m%d"))\
      + '.csv'
df_full_aug.to_csv(full_ratio_path)
pdb.set_trace()

