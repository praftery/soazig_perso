"""
Use this script to dwn data in from openbms in a specific time frame.
Use the type of data (tav, energy related to zones (load) or BACnet) at the zone level.
For data at a higher level, use the tav_energy.py script.

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

def tav_data(c, source, restrict_root, points, floors, not_zones, floor_frames, startF, endF, delta, floor_dir, window, ts_step, path_and=None):
  endFp = endF - datetime.timedelta(days=1)
  for f in floors:
    floor_frames = []
    #TODO: Choose zones HERE
    #zones_names = ['S%s-%s' %(f, str(z).zfill(2)) for z in range(1,22)]
    zones_names = ['S%s-%s' %(f, str(z).zfill(2)) for z in range(11,12)]
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
        df = tav_graphs_functions.data_frame(dff, data, tags, points, tag_mode='Path', zone=True, zone_name=zone_name) 
        #pdb.set_trace()
        zone_frames.append(df)
        start = end
        end += min(delta, (endF - end))
      df_zone = pd.concat(zone_frames) #concat the data fram of for this zone to cover the entire time period
      floor_frames.append(df_zone)
      df_zone.to_csv('../csv_output_temp/Floor%s-%s_' \
        %(str(f).zfill(2), str(zone_name))
        + '%s-%s_demo_temp.csv'%(start.strftime("%Y%m%d"), end.strftime("%Y%m%d")))
    #pdb.set_trace()
    #Combine all zones (columns) in a dataframe
    #TODO: create a temp file with aggregated floors
    df_floor = reduce(lambda x,y: \
               pd.merge(x, y, on=['timestamp','datetime']),floor_frames)
        #+ 'Floor%s_airflow_temp_load' %(str(f).zfill(2))  \
    #TODO: change file name HERE
    floor_path = floor_dir \
        + 'Floor%s_demo' %(str(f).zfill(2))\
        + '%s-%s'%(startF.strftime("%Y%m%d"), endFp.strftime("%Y%m%d"))\
        + '.csv' 
    df_floor.to_csv(floor_path)

c = SmapClient(base='http://new.openbms.org/backend',\
                 key=['XuETaff882hB6li0dP3XWdiGYJ9SSsFGj0N8'])
source = 'Sutardja Dai Hall BACnet'
source_tav = 'Sutardja Dai Hall TAV'
source_energy = 'Sutardja Dai Hall Energy Data'
path_and_tav = ['tav_whole_bldg/']
path_and_energy = ['energy_data/', 'variable_elec_cost/']
#TODO : include points we want
#points = ['CTL_FLOW_MAX', 'CTL_FLOW_MIN', 'AIR_VOLUME'] #, 'ROOM_TEMP', 'CTL_STPT']
points = ['AIR_VOLUME', 'CTL_FLOW_MIN','CLG_LOOPOUT', 'HTG_LOOPOUT', 'DMPR_POS', 'DMPR_CMD']
#points_tav = ['/cycle']
points_tav = ['average_airflow_in_cycle', 'average_airflow_in_hour', 'tav_active'] 
points_energy = ['hot_water', 'zone_load']

startF = date(2016, 04, 12)
endF = date(2016, 04, 27)
delta = datetime.timedelta(days=80) #try with 3 days - 1min resolution next time
ts_step = 1*60 #timestamp fq
window='apply window(first, field=\"second\", width=60) to' 
#window='apply window(first, field=\"minute\", width=1) to' 
restrict = tav_graphs_functions.restrict(source, points) 
restrict_tav = tav_graphs_functions.restrict(source_tav, points_tav, path_and_tav)
restrict_energy = tav_graphs_functions.restrict(source_energy, points_energy, path_and_energy)

##TODO Choose here what data to include in the request
#restrict_root = '( ' + restrict + ') or (' + restrict_energy + ')'
#restrict_root = '( ' + restrict + ') or (' + restrict_tav + ')'
restrict_root = restrict
points_root = points #+ points_energy
#points_root = points_tav
#points_root = points_energy

#TODO Choose flooors HERE
floors = [str(f) for f in [4]]
#floors = [str(f) for f in [4,5,6,7,3,2,1]]
#floors = [str(f) for f in [4]]
#floors = [str(f) for f in [3,2,1]]
not_zones = ['S1-11', 'S1-12', 'S1-21', 'S6-21', 'S7-17', 'S7-18','S7-19', 'S7-20', 'S7-21']
floor_frames = []

floor_dir = '../csv_output%s/Demo%s_1min/'%(startF.year, startF.year)
print floor_dir
if not os.path.exists(floor_dir):
  os.makedirs(floor_dir)
#pdb.set_trace()

tav_data(c, source, restrict_root, points_root, floors, not_zones, floor_frames, startF, endF, delta, floor_dir, window, ts_step, path_and=None)

print "\n\n======================== Dwn Complete =========================="
#pdb.set_trace()

