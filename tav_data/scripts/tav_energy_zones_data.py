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
    floor_frames = []
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
        df = tav_graphs_functions.data_frame_energy_zones(dff, data, tags, points, zone_name, tag_mode='Path') #, zones = True, headers=int(1))
        #pdb.set_trace()
        zone_frames.append(df)
        start = end
        end += min(delta, (endF - end))
      df_zone = pd.concat(zone_frames)
      floor_frames.append(df_zone)
      df_zone.to_csv('../csv_output_temp/Floor%s_load%s' \
        %(str(f).zfill(2), str(zone_name))
        + '%s-%s_tempo.csv'%(start.strftime("%Y%m%d"), end.strftime("%Y%m%d")))
    #pdb.set_trace()
    df_floor = reduce(lambda x,y: \
               pd.merge(x, y, on=['timestamp','datetime']),floor_frames)
#    floor_path = '../csv_output/TEST/' + 'Floor%s_airflow' %(str(f).zfill(2))  \
    floor_path = '../csv_output/Floor%s_3min/'%(startF.year) + 'Floor%s_load' %(str(f).zfill(2))  \
        + '%s-%s'%(startF.strftime("%Y%m%d"), endF.strftime("%Y%m%d"))\
        + '.csv' 
    df_floor.to_csv(floor_path)
    floor_frames = []
    pdb.set_trace()
#    df_aug = tav_graphs_functions.data_ratios(df_floor, points) 
#    floor_ratio_path = '../csv_output/Floor%s/'%(startF.year) + 'Floor%s_ratios' %(str(f).zfill(2))  \
#    floor_ratio_path = '../csv_output/TEST/' + 'Floor%s_ratios' %(str(f).zfill(2))  \
#        + '%s-%s'%(startF.strftime("%Y%m%d"), endF.strftime("%Y%m%d"))\
#        + '.csv' 
#    df_aug.to_csv(floor_ratio_path)
#    pdb.set_trace()

c = SmapClient(base='http://new.openbms.org/backend',\
                 key=['XuETaff882hB6li0dP3XWdiGYJ9SSsFGj0N8'])
source_energy = 'Sutardja Dai Hall Energy Data'
path_and_energy = ['energy_data/', 'variable_elec_cost/']
points_energy = ['zone_load']

startF = date(2014,10, 15)
endF = date(2014, 12, 26)
delta = datetime.timedelta(days=80) #try with 3 days - 1min resolution next time
ts_step = 3*60 #timestamp fq
window='apply window(first, field=\"minute\", width=3) to' 
restrict_energy_root = tav_graphs_functions.restrict(source_energy, points_energy, path_and_energy)

floors = [str(f) for f in [7, 2,1]]
#floors = [str(f) for f in [4, 5]]
not_zones = ['S1-11', 'S1-12', 'S1-21', 'S6-21', 'S7-17', 'S7-18','S7-19', 'S7-20', 'S7-21']
floor_frames = []

tav_data(c, source_energy, restrict_energy_root, points_energy, floors, not_zones, floor_frames, startF, endF, delta, window, ts_step, path_and=None)
#tav_data(c, source_energy, restrict_energy, points_energy, floors, not_zones, floor_frames, startF, endF, delta, window, ts_step, path_and=None)
pdb.set_trace()

