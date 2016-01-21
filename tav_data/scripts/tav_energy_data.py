"""
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

def energy_data(c, source, path_and, path_list_energy, path_not, startF, endF, delta, window):
  start = startF
  end = start + delta
  time_frames = []
  while start != end:
    startDate = start.strftime("%m/%d/%Y %H:%M")
    endDate = end.strftime("%m/%d/%Y %H:%M")
    print "Start date: ", startDate
    print "End date: ", endDate
    restrict = " Metadata/SourceName = '%s' and Path ~ '%s' and ("\
             %(source, path_and) \
             + ' or '.join(["Path ~ '%s'"] * len(path_list_energy)) \
             %tuple(path_list_energy) + ") and not ("\
             + ' or '.join(["Path ~ '%s'"] * len(path_not)) \
             %tuple(path_not) + ")"
    #start = dtutil.dt2ts(dtutil.strptime_tz("08-28-2015", "%m-%d-%Y"))
    #end   = dtutil.dt2ts(dtutil.strptime_tz("08-29-2015", "%m-%d-%Y"))
    #data = c.data(restrictall, start, end)
    #data = c.data(restrict, start, end, limit=1000)
    query_data = tav_graphs_functions.query_data(c, restrict, startDate, endDate, win=window)
    #query_data = 'apply window(mean, field=\"minute\", width=15) to data in ("' \
    #             + str(startDate) + '" , "' + endDate + '") where' + restrict
    data = query_data[1]
    tags = query_data[0]
    df = tav_graphs_functions.data_frame(data, tags, path_list_energy, mode='Path')
    time_frames.append(df)
    start = end
    end += delta
 
#    N=len(data)
#    df = pd.DataFrame()
#    d = np.array(data[0]['Readings'])
#    dt_format = '%Y-%m-%d %H:%M:%S'
#    if d.any():
#      df['timestamp'] = d[:,0]
#      df['datetime'] = [datetime.datetime.fromtimestamp(x/1000).strftime(dt_format)
#                        for x in d[:,0]]
#      for i in range(N):
#        u = data[i]['uuid']
#        d = np.array(data[i]['Readings'])
#        if d.any():
#          tag_path = [tag['Path'] for tag in tags if tag['uuid'] == u][0]
#          try:
#            for p in path_list_energy:
#              if p in tag_path:
#                df[p] = d[:,1]
#                print " Dwnl data for " + p
#          except:
#            print "Could not dwn data for : ", p

  df_period = reduce(lambda x,y:\
              pd.merge(x, y, on=['timestamp','datetime']), time_frames)
  df_period.to_csv('../csv_output/Energy%s/'%(startF.year) + 'Energy_tav%s-%s_' \
    %(startDate.strftime("%Y%m%d"), endDate.strftime("%Y%m%d")) \
    + '.csv')

c = SmapClient(base='http://new.openbms.org/backend',\
               key=['XuETaff882hB6li0dP3XWdiGYJ9SSsFGj0N8'])
source_energy = 'Sutardja Dai Hall Energy Data'
path_and = 'energy_data/variable_elec_cost/'
path_list_energy = [
                'total_cost',
                'AH2_panel_fan_power',
                'AH2_panel_fan_power_cost',
                'chilled_water_AH2',
                'chilled_water_AH2_cost',
                'hot_water_AH2',
                'hot_water_AH2_cost'
                ]
#path_list_energy = [
#                'total_zone_load_AH2',
#                'instantaneous_zone_load_S4-03'
#                'instantaneous_zone_load_S4-05'
#                'instantaneous_zone_load_S4-02'
#                'instantaneous_zone_load_S4-18'
#                'hot_water_S4-03'
#                'hot_water_S4-05'
#                'hot_water_S4-02'
#                'hot_water_S4-18'
#                ]
path_not = ['chilled_water_AH2A', 'chilled_water_AH2B']
startF = date(2015, 10, 15)
endF = date(2015, 12, 26)
delta = datetime.timedelta(days=3)
window = 'apply window(mean, field=\"minute\", width=15) to'
energy_data(c, source_energy, path_and, path_list_energy, path_not, startF, endF, delta, window)

pdb.set_trace()
