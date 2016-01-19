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


def restrict(source, zones_names, points, path_and=None):
  if path_and != None:
    zones_names=[str(path_and) + '/' + str(z) for z in zones_names]
  var= "Metadata/SourceName = '%s' and\n(" %(source)\
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
  if d.any():
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
  return df2

def df_replace(csv_path, rep_in, rep_out):
    df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
    #df = df.where(df==rep_in,rep_out)
    csv_path_out = csv_path[:-4] + '_rep.csv'
    pdb.set_trace()
    #df.to_csv(csv_path)

if __name__ == "__main__":
  points = ['AIR_VOLUME', 'CTL_FLOW_MIN', 'CTL_FLOW_MAX']
  df3_path = '../csv_output/Floor2015/Floor05_airflow20151109-20151210.csv'
  df3 = pd.read_csv(df3_path,  index_col=0, parse_dates=True) 
  df3_aug = data_ratios(df3, points)
  pdb.set_trace()
  df3_aug.to_csv('../csv_output/Floor2015/Floor05_ratios20151109-20151210.csv')
  pdb.set_trace()

  #df2_path = '../csv_output/TEST/Floor04_ratios20151110-20151111.csv'
  #df_replace(df2_path,np.inf, 1)  
  #pdb.set_trace()
  
  #points = ['AIR_VOLUME', 'CTL_FLOW_MIN', 'CTL_FLOW_MAX']
  #df_path = '../csv_output/SDH_TAV_20151124-20151126.csv'
  #df = pd.read_csv(df_path,  index_col=0, parse_dates=True) 
  #df_aug = data_ratios(df, points)
  #df_path_aug = '../csv_output/SDH_TAV_20151124-20151126_aug.csv'
  #df_aug.to_csv(df_path_aug)
  #pdb.set_trace()

