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

def tav_data(c, source, restrict_root, points, floors, not_zones, floor_frames, startF, endF, delta, window, ts_step, path_and=None):
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
      end = start + min(delta, (endF - startF))
      while start != end: 
        startDate = start.strftime("%m/%d/%Y %H:%M")
        endDate = end.strftime("%m/%d/%Y %H:%M")
        print "\nStart date: ", startDate
        print "End date: ", endDate
        restrict = tav_graphs_functions.restrict_zones(restrict_root, [zone_name], path_and) 
        query_data = tav_graphs_functions.query_data(c, restrict, startDate, endDate, win=window)
        #pdb.set_trace()
        tags = query_data[0]
        data = query_data[1]
        dff = tav_graphs_functions.dff_ts(start, end, ts_step)
        df = tav_graphs_functions.data_frame(dff, data, tags, points, tag_mode='Path') #, zones = True, headers=int(1))
        #pdb.set_trace()
        zone_frames.append(df)
        start = end
        end += min(delta, (endF - end))
      df_zone = pd.concat(zone_frames)
      floor_frames.append(df_zone)
    #pdb.set_trace()
    df_floor = reduce(lambda x,y: \
               pd.merge(x, y, on=['timestamp','datetime']),floor_frames)
#    floor_path = '../csv_output/TEST/' + 'Floor%s_airflow' %(str(f).zfill(2))  \
    floor_path = '../csv_output/Floor%s_3min/'%(startF.year) + 'Floor%s_temp' %(str(f).zfill(2))  \
        + '%s-%s'%(startF.strftime("%Y%m%d"), endF.strftime("%Y%m%d"))\
        + '.csv' 
    df_floor.to_csv(floor_path)
#    df_aug = tav_graphs_functions.data_ratios(df_floor, points) 
#    floor_ratio_path = '../csv_output/Floor%s/'%(startF.year) + 'Floor%s_ratios' %(str(f).zfill(2))  \
#    floor_ratio_path = '../csv_output/TEST/' + 'Floor%s_ratios' %(str(f).zfill(2))  \
#        + '%s-%s'%(startF.strftime("%Y%m%d"), endF.strftime("%Y%m%d"))\
#        + '.csv' 
#    df_aug.to_csv(floor_ratio_path)
#    pdb.set_trace()

c = SmapClient(base='http://new.openbms.org/backend',\
                 key=['XuETaff882hB6li0dP3XWdiGYJ9SSsFGj0N8'])
source = 'Sutardja Dai Hall BACnet'
source_tav = 'Sutardja Dai Hall TAV'
source_energy = 'Sutardja Dai Hall Energy Data'
path_and_tav = ['tav_whole_bldg/']
#points = ['CTL_FLOW_MAX', 'CTL_FLOW_MIN', 'AIR_VOLUME'] 
points = ['ROOM_TEMP', 'CTL_STPT']
points_tav = ['average_airflow_in_cycle', 'average_airflow_in_hour', 'tav_active'] 
startF = date(2015,10, 15)
endF = date(2015, 12, 26)
delta = datetime.timedelta(days=5) #try with 3 days - 1min resolution next time
ts_step = 3*60 #timestamp fq
window='apply window(first, field=\"minute\", width=3) to' 
restrict = tav_graphs_functions.restrict(source, points) 
restrict_tav = tav_graphs_functions.restrict(source_tav, points_tav, path_and_tav)
restrict_root = '( ' + restrict + ') or (' + restrict_tav + ')'

floors = [str(f) for f in [4,5,6,7,3,2,1]]
#floors = [str(f) for f in [4, 5]]
not_zones = ['S1-11', 'S1-12', 'S1-21', 'S6-21', 'S7-17', 'S7-18','S7-19', 'S7-20', 'S7-21']
floor_frames = []

tav_data(c, source, restrict_root, points + points_tav, floors, not_zones, floor_frames, startF, endF, delta, window, ts_step, path_and=None)
pdb.set_trace()

