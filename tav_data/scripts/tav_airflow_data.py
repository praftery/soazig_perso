"""
@author Soazig Kaam <soazig.kaam@berkeley.edu>

"""
from smap.archiver.client import SmapClient
from functools import reduce
from datetime import date, timedelta

import sys, os, pdb
import numpy as np
import pandas as pd
import csv
import time
import pprint as pp
import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), "../functions/"))
import tav_graphs_functions

def tav_data(c, source, points, floors, not_zones, floor_frames, startF, endF, delta, window, source1=None):
  for f in floors:
    zones_names = ['S%s-%s' %(f, str(z).zfill(2)) for z in range(1,22)]
    #zones_names = ['S%s-%s' %(f, str(z).zfill(2)) for z in range(1,3)]
    #zones_names = ['S4-18', 'S4-03', 'S4-05', 'S4-09']
    zones_names = [z for z in zones_names if z not in not_zones]
    print "\nStarted process for Floor: ", f
    #pdb.set_trace()
    for zone_name in zones_names:
      print "\nStarted process for zone: ", zone_name
      zone_frames = []
      start = startF
      end = start + delta
      while start != end: 
        startDate = start.strftime("%m/%d/%Y %H:%M")
        endDate = end.strftime("%m/%d/%Y %H:%M")
        print "\nStart date: ", startDate
        print "End date: ", endDate
        restrict = tav_graphs_functions.restrict(source, [zone_name], points, path_and=source1) 
        query_data = tav_graphs_functions.query_data(c, restrict, startDate, endDate, win=window)
        #pdb.set_trace()
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
    #floor_path = '../csv_output/TEST/' + 'Floor%s_airflow' %(str(f).zfill(2))  \
    floor_path = '../csv_output/Floor%s_3min/'%(startF.year) + 'Floor%s_tav' %(str(f).zfill(2))  \
        + '%s-%s'%(startF.strftime("%Y%m%d"), endF.strftime("%Y%m%d"))\
        + '.csv' 
    df_floor.to_csv(floor_path)
#    df_aug = tav_graphs_functions.data_ratios(df_floor, points) 
#    #floor_ratio_path = '../csv_output/TEST/' + 'Floor%s_ratios' %(str(f).zfill(2))  \
#    floor_ratio_path = '../csv_output/Floor%s/'%(startF.year) + 'Floor%s_ratios' %(str(f).zfill(2))  \
#        + '%s-%s'%(startF.strftime("%Y%m%d"), endF.strftime("%Y%m%d"))\
#        + '.csv' 
#    df_aug.to_csv(floor_ratio_path)

c = SmapClient(base='http://new.openbms.org/backend',\
                 key=['XuETaff882hB6li0dP3XWdiGYJ9SSsFGj0N8'])
#source = 'Sutardja Dai Hall BACnet'
source = 'Sutardja Dai Hall TAV'
#source1 = None
source1 = 'tav_whole_bldg' 
#points = ['CTL_FLOW_MAX', 'CTL_FLOW_MIN', 'AIR_VOLUME'] 
points = ['average_airflow_in_cycle', 'average_airflow_in_hour', 'tav_active'] 
floors = [str(f) for f in [4,5,6,7,3,2,1]]
#floors = [str(f) for f in [5]]
not_zones = ['S1-11', 'S1-12', 'S1-21', 'S6-21', 'S7-17', 'S7-18','S7-19', 'S7-20', 'S7-21']
floor_frames = []
startF = date(2015,11,9)
endF = date(2015, 12, 26)
delta = datetime.timedelta(days=3) #try with 3 days - 1min resolution next time
window='apply window(first, field=\"minute\", width=3) to' 

tav_data(c, source, points, floors, not_zones, floor_frames, startF, endF, delta, window, source1=source1)
pdb.set_trace()

