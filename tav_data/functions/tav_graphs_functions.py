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


def restrict(source, zones_names, points):
  var = "Metadata/SourceName = '%s' and\n(" %source\
         + ' or\n '.join(["Path ~ '%s'"]*len(zones_names))\
         %tuple(zones_names) \
         + ")\n and (" \
         + ' or\n '.join(["Path ~ '%s'"] * len(points)) \
         %tuple(points) + ")"
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

def data_frame(data, tags, points, mode):
  N = len(data)
  dt_format = '%Y-%m-%d %H:%M:%S'
  df = pd.DataFrame()
  d = np.array(data[0]['Readings'])
  df['timestamp'] = d[:,0]
  df['datetime'] = [datetime.datetime.fromtimestamp(x/1000).strftime(dt_format)
                    for x in d[:,0]]
  for i in range(N):
    u = data[i][mode]
    d = np.array(data[i]['Readings'])
    if d.any():
      tag_path = [tag['Path'] for tag in tags if tag[mode] == u][0]
      try:
        for p in points:
          if p in tag_path: 
            head = '_'.join(tag_path.split('/')[-2:])
            #head = p
            df[head] = d[:,1]
            print " Dwnl data for " + head
      except:
        print "Could not dwn data for " + '_'.join(tag_path.split('/')[-2:])
#        pdb.set_trace()
  return df

def data_ratios(df, points):
  df2 = pd.DataFrame()
  df2['timestamp'] = df['timestamp']
  df2['datetime'] = df['datetime']
  for column_name, column in df.transpose()[2:].iterrows():
    zone = column_name[:5]
    ratio = zone + '_ratio'
    at_min = zone + '_at_min'
    if not ratio in df2.transpose().iterrows():
      try:
        df2[ratio] = df[zone + '_CTL_FLOW_MIN']/df[zone + '_CTL_FLOW_MAX']
      except:
        print "Could not perform calcs for: ", ratio
    if not at_min in df2.transpose().iterrows():
      try:
        df2[at_min] = df[zone + '_AIR_VOLUME']/df[zone + '_CTL_FLOW_MIN']
      except:
        print "Could not perform calcs for: ", at_min
  return df2

if __name__ == "__main__":
  points = ['AIR_VOLUME', 'CTL_FLOW_MIN', 'CTL_FLOW_MAX']
  df_path = '../csv_output/SDH_TAV_20151124-20151126.csv'
  df = pd.read_csv(df_path,  index_col=0, parse_dates=True) 
  df_aug = data_ratios(df, points)
  df_path_aug = '../csv_output/SDH_TAV_20151124-20151126_aug.csv'
  df_aug.to_csv(df_path_aug)
  pdb.set_trace()

