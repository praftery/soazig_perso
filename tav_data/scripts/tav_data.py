"""
@author Soazig Kaam <soazig.kaam@berkeley.edu>

"""
from smap.archiver.client import SmapClient
from smap.util import periodicSequentialCall
from smap.contrib import dtutil
from smap.util import find
from datetime import timedelta, date

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
#path_list = [
#             'HEAT.COOL',
#             'VLV_POS',
#             'CTL_STPT',
#             'ROOM_TEMP',
#             'AI_3',
#             'CTL_FLOW_MIN',
#             'DMPR_POS',
#             'AIR_VOLUME'
#             ] 
path_list = [
             'CTL_FLOW_MAX',
             'CTL_FLOW_MIN',
             'AIR_VOLUME'
             ] 
source_tav = 'Sutardja Dai Hall TAV'
path_and = 'tav_whole_bldg'
path_list_tav = [
                'tav_active',
                '/cycle'
                ]
zone_list = ['S4-18', 'S4-03', 'S4-05', 'S4-09']
for zone_name in zone_list:
  dffull = pd.DataFrame() 
  #for path_list in points:
  if True:
    restrict = " Metadata/SourceName = '%s' and Path ~ '%s' and ("\
                 %(source, zone_name)\
                 + ' or '.join(["Path ~ '%s'"] * len(path_list)) \
                 %tuple(path_list) + ")"
    
    restrict2 = " Metadata/SourceName = '%s' and Path ~ '%s' and Path ~ '%s' and ("\
                 %(source_tav, zone_name, path_and) \
                 + ' or '.join(["Path ~ '%s'"] * len(path_list_tav)) \
                 %tuple(path_list_tav) + ")"
    
    restrictall = "(" + restrict + ") or (" + restrict2 + ")"
    tags = c.tags(restrict)
    
    start = date(2015,11,9)
    delta = datetime.timedelta(days=2)
    end = start + delta
    endDateF = date(2016, 12, 26)
    #data = c.data(restrictall, start, end)
    while end <= endDateF: 
      startDate = start.strftime("%m/%d/%Y %H:%M")
      endDate = end.strftime("%m/%d/%Y %H:%M")
      #Start = dtutil.dt2ts(dtutil.strptime_tz(start.strftime("%m-%d-%Y"), "%m-%d-%Y"))
      #End = dtutil.dt2ts(dtutil.strptime_tz(end.strftime("%m-%d-%Y"), "%m-%d-%Y"))
      print "Start date: ", startDate
      print "End date: ", endDate
      name = '../csv_output/' + zone_name + '_airflow' +  start.strftime("%Y%m%d")+'.csv' 
      dt_format = '%Y-%m-%d %H:%M:%S'
      query_data = 'select data in ("' + startDate + '" , "' + endDate + '") where' + restrict
      data = c.query(query_data)
      #data = c.data(restrict, Start, End, limit=1000000000)
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
          print tag_path
          for p in path_list:
            if p in tag_path:
              h = '_'.join(tag_path.split('/')[-2:])
              df[h] = d[:,1]
              print " Dwnl data for " + h
      dffull.append(df)
      start = end
      end += delta
    df_aug = tav_graphs_functions.data_ratios(dffull, path_list) 
    dffull.to_csv(name)
    pdb.set_trace()
