"""
@author Soazig Kaam <soazig.kaam@berkeley.edu>
"""
from smap.archiver.client import SmapClient
from datetime import timedelta, date

import numpy as np
import pandas as pd
import pdb
import csv
import shutil
import time
import pprint as pp
import datetime


def restrict(source, points, path_and=None):
  var = "Metadata/SourceName = '%s' and\n(" %(source)\
         + ' or\n '.join(["Path ~ '%s'"] * len(points)) \
         %tuple(points) + ")"
  if path_and != None:
    path_and = " and\n( " \
         + ' and\n '.join(["Path ~ '%s'"] * len(path_and)) \
         %tuple(path_and) + ")"
    return var + path_and
  else:
    return var

def restrict_zones(restrict_root, zones_names, path_and=None):
  if path_and != None:
    zones_names=[str(path_and) + '/' + str(z) for z in zones_names]
  var = restrict_root + " and\n(" \
         + ' or\n '.join(["Path ~ '%s'"] * len(zones_names)) \
         %tuple(zones_names) + ")"
  return var

def query_data(c, restrict, startDate, endDate, win='select'):
  query_data = win + ' data in ("' + \
                 str(startDate) + '" , "' + str(endDate) + \
                 '") where ' + restrict
  data = c.query(query_data)
  tags = c.tags(restrict)
#  start = dtutil.dt2ts(dtutil.strptime_tz(str(startDate), "%m/%d/%Y"))
#  end = dtutil.dt2ts(dtutil.strptime_tz(str(endDate), "%m/%d/%Y"))
#  data = c.data(restrict, start, end)
  return tags, data

def dff_ts(startF, endF, step_in_seconds):
  ts_start = time.mktime(startF.timetuple())
  ts_end = time.mktime(endF.timetuple())
  dff = pd.DataFrame()
  dff['timestamp'] = np.arange(ts_start, ts_end, step_in_seconds)
  dff['datetime'] = [datetime.datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S")
                                for x in dff['timestamp']]
  return dff

def data_frame(dff, data, tags, points, tag_mode, zone=None, zone_name=None):
  '''
  '''
  N = len(data)
  if True:
    for i in range(N):
      u = data[i][tag_mode]
      d = np.array(data[i]['Readings'])
      #pdb.set_trace()
      if d.any():
        tag_path = [tag['Path'] for tag in tags if tag[tag_mode] == u][0]
        try:
          #pdb.set_trace()
          for p in points:
            match = False
            if zone == True:
              if p in tag_path and zone_name in tag_path:
                head = '_'.join([zone_name, p]) 
                match = True
            elif p == tag_path.split('/')[-1] or p == '/'.join(tag_path.split('/')[-2:]):
              head = p
              match = True
            if match == True:
              df = pd.DataFrame(d, columns=['timestamp', head]) 
              df['timestamp'] = df['timestamp']/1000.0
              dff = pd.merge(dff, df, how='outer', on=['timestamp']) 
              print " Dwnl data for " + head
        except:
          print "Could not dwn data for " + '_'.join(tag_path.split('/')[-2:])
          pdb.set_trace()
  return dff

def data_ratios(df, points):
  df3 = df.replace(0.0,0.1)
  df2 = pd.DataFrame()
  df2['datetime'] = df['datetime']
  df2['timestamp'] = df['timestamp']
  for column_name, column in df.transpose()[2:].iterrows():
    #if column_name in ['datetime', 'timestamp']:
    #  break
    #else:
    if True:
      zone = column_name[:5]
      ratio = zone + '_ratio'
      at_min = zone + '_at_min'
      at_max = zone + '_at_max'
      if not ratio in df2.transpose().iterrows():
        try:
          df2[ratio] = df[zone + '_CTL_FLOW_MIN']/df[zone + '_CTL_FLOW_MAX']
        except:
          print "Could not perform calcs for: ", ratio
      if not at_min in df2.transpose().iterrows():
        try:
          df2[at_min] = df3[zone + '_AIR_VOLUME']/df3[zone + '_CTL_FLOW_MIN']
        except:
          print "Could not perform calcs for: ", at_min
      if not at_max in df2.transpose().iterrows():
        try:
          df2[at_max] = df3[zone + '_AIR_VOLUME']/df3[zone + '_CTL_FLOW_MAX']
        except:
          print "Could not perform calcs for: ", at_max
  return df2

def df_replace(csv_path, rep_in, rep_out):
    df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
    #df = df.where(df==rep_in,rep_out)
    csv_path_out = csv_path[:-4] + '_rep.csv'
    pdb.set_trace()
    #df.to_csv(csv_path)

