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
         + ' or\n '.join(["Path ~ '%s'"] * len(path_and)) \
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

def data_frame(dff, data, tags, points, tag_mode):
  '''
  '''
  #pdb.set_trace()
  N = len(data)
  if True:
    for i in range(N):
      u = data[i][tag_mode]
      d = np.array(data[i]['Readings'])
      if d.any():
        tag_path = [tag['Path'] for tag in tags if tag[tag_mode] == u][0]
        try:
          for p in points:
            #pdb.set_trace()
            #if p in tag_path: 
            if p == tag_path.split('/')[-1]:
            #if p == '/'.join(tag_path.split('/')[-2:]): # for tav_whole_bldg/... 
              head = '_'.join(tag_path.split('/')[-2:])
              #head = p
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

if __name__ == "__main__":
  points = ['ROOM_TEMP', 'CTL_STPT']
  source = 'Sutardja Dai Hall BACnet'
  points_tav = ['tav_active']
  path_and_tav = ['tav_whole_bldg/']
  source_tav = 'Sutardja Dai Hall TAV'
  zones_names = [ 'S1-01', 'S2-09']
  restrict_root = '(' + restrict(source, points) + ') or (' + \
                        restrict(source_tav, points_tav, path_and_tav) + ')'
  restrict = restrict_zones(restrict_root, zones_names, path_and=None)
  pp.pprint(restrict)
  pdb.set_trace()
  c = SmapClient(base='http://new.openbms.org/backend',\
                 key=['XuETaff882hB6li0dP3XWdiGYJ9SSsFGj0N8'])
  startDate = date(2015,10,15).strftime("%m/%d/%Y %H:%M")
  endDate = date(2015, 10, 16).strftime("%m/%d/%Y %H:%M")
  window='apply window(first, field=\"minute\", width=3) to'
  query_data = query_data(c, restrict, startDate, endDate, win=window)
  pdb.set_trace()

  # Test to repopulate the ration airflow/max with existing datasets
  points = ['AIR_VOLUME', 'CTL_FLOW_MIN', 'CTL_FLOW_MAX']
  floors = [str(f) for f in [6,5,4,7,3,2,1]]
  for f in floors:
    df4_path = '../csv_output/Floor2015_3min/Floor%s_airflow20151015-20151226.csv'%(str(f).zfill(2))
    df4 = pd.read_csv(df4_path,  index_col=0) 
    df4_aug = data_ratios(df4, points)
    df4_aug.to_csv('../csv_output/Floor2015_3min/Floor%s_ratios20151015-20151226_new.csv'%(str(f).zfill(2)))
  pdb.set_trace()
    
#  points = ['AIR_VOLUME', 'CTL_FLOW_MIN', 'CTL_FLOW_MAX']
#  floors = [str(f) for f in [4,5,6,7,3,2,1]]
#  for f in floors:
#    df3_path = '../csv_output/Floor2015_3min/Floor%s_airflow20151015-20151226.csv'%(str(f).zfill(2))
#    df3 = pd.read_csv(df3_path,  index_col=0) 
#    df3_aug = data_ratios(df3, points)
#    df3_aug.to_csv('../csv_output/Floor2015_3min/Floor%s_ratios20151015-20151226.csv'%(str(f).zfill(2)))
#  pdb.set_trace()

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

