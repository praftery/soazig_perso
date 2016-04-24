"""
Use this script to dwnl data at a higher level - not at the zone level.
Consider cost and energy seperately.
@author Soazig Kaam <soazig.kaam@berkeley.edu>

"""
from smap.archiver.client import SmapClient
from datetime import timedelta, date

import sys, os, pdb
import numpy as np
import pandas as pd
import csv
import time
import pprint as pp
import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), "../functions/"))
import tav_graphs_functions

def energy_data(path_list, restrict, startF, endF, delta, ts, window, folder_name, file_name):
  start = startF
  end = start + min(delta, (endF - start))
  time_frames = []
  while start != end:
    startDate = start.strftime("%m/%d/%Y %H:%M")
    endDate = end.strftime("%m/%d/%Y %H:%M")
    print "Start date: ", startDate
    print "End date: ", endDate
    #start = dtutil.dt2ts(dtutil.strptime_tz("08-28-2015", "%m-%d-%Y"))
    #end   = dtutil.dt2ts(dtutil.strptime_tz("08-29-2015", "%m-%d-%Y"))
    #data = c.data(restrictall, start, end)
    #data = c.data(restrict, start, end, limit=1000)
    query_data = tav_graphs_functions.query_data(c, restrict, startDate, endDate, win=window)
    data = query_data[1]
    tags = query_data[0]
    dff = tav_graphs_functions.dff_ts(start, end, ts)
    #pdb.set_trace()
    #dt_format = '%Y-%m-%d %H:%M:%S'
    #dff = pd.DataFrame()
    #dff['datetime'] = ts
    #dff['timestamp'] = pd.DatetimeIndex(dff['datetime']).astype(np.int64)/1000
    #[datetime.datetime.fromtimestamp(x/1000).strftime(dt_format)
    #                   for x in dff['timestamp']]
    df = tav_graphs_functions.data_frame(dff, data, tags, path_list, tag_mode='Path')
    time_frames.append(df) 
    start = end
    end += min(delta, (endF - end))
  df_full_period = pd.concat(time_frames)
  df_full_period.to_csv('../csv_output_V2/' + folder_name + '%s/'%(startF.year) \
    + file_name + '%s-%s' \
    %(startF.strftime("%Y%m%d"), endF.strftime("%Y%m%d")) \
    + '.csv')

c = SmapClient(base='http://new.openbms.org/backend',\
               key=['XuETaff882hB6li0dP3XWdiGYJ9SSsFGj0N8'])
source_energy = 'Sutardja Dai Hall Energy Data'
path_and_energy = 'energy_data/variable_elec_cost/'
path_list_energy = [
                    'total_cost',
                    'AH2_panel_fan_power',
                    #'AH2_panel_fan_power_cost',
                    'AH2_total_supply_fan_power',
                    #'AH2_total_supply_fan_power_cost',
                    'chilled_water_AH2',
                    #'chilled_water_AH2_cost',
                    'hot_water_AH2',
                    #'hot_water_AH2_cost'
                    'total_zone_load_AH2'
                    'measured_chilled_water'
                   ]
path_not_energy = [
                   'chilled_water_AH2A', 
                   'chilled_water_AH2B'
                  ]

source_airflow = 'Sutardja Dai Hall BACnet'
path_list_airflow = ['AH2A/SF_CFM', 'AH2B/SF_CFM']

source_tav = 'Sutardja Dai Hall TAV'
path_and_tav = 'tav_whole_bldg/'
path_list_tav = [
                 'zones_active',
                 'zones_inactive',
                 'zones_in_low_cycle',
                 'zones_in_high_cycle',
                 'zones_waiting_for_low_cycle',
                 'zone_limit',
                 'zones_entering_low_cycle',
                 'zones_entering_high_cycle',
                 'zones_being_activated',
                 'zones_being_deactivated',
                ]
restrict_tav = " Metadata/SourceName = '%s' and Path ~ '%s' and ("\
             %(source_tav, path_and_tav)\
             + ' or '.join(["Path ~ '%s'"] * len(path_list_tav)) \
             %tuple(path_list_tav) + ")" 

restrict_energy = " Metadata/SourceName = '%s' and Path ~ '%s' and ("\
             %(source_energy, path_and_energy) \
             + ' or '.join(["Path ~ '%s'"] * len(path_list_energy)) \
             %tuple(path_list_energy) + ")  and not ("\
             + ' or '.join(["Path ~ '%s'"] * len(path_not_energy)) \
             %tuple(path_not_energy) + ")"

restrict_airflow = " Metadata/SourceName = '%s' and ("\
             %(source_airflow)\
             + ' or '.join(["Path ~ '%s'"] * len(path_list_airflow)) \
             %tuple(path_list_airflow) + ")" 
#folder_name='Energy'
#file_name='Energy_TAV'
#restrict = "(" + restrict_energy + ") or (" + restrict_airflow + ")"                  
#path_list = path_list_energy + path_list_airflow

folder_name='TAV'
file_name='TAV_trends'
restrict = restrict_tav
path_list = path_list_tav

#pdb.set_trace()
startF = datetime.datetime(2016, 04, 01, 0, 0, 0)
endF = datetime.datetime(2016, 04, 23, 0, 0, 0)
delta = datetime.timedelta(days=80)
ts = 3*60 #window * 60s
window = 'apply window(first, field=\"minute\", width=3) to'
energy_data(path_list, restrict, startF, endF, delta, ts, window, folder_name, file_name)
pdb.set_trace()
