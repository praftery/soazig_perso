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
                head = '_'.join([p, zone_name]) 
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

if __name__ == "__main__":
  pdb.set_trace()

  df_path = '../csv_output/Floor2014_3min/Floor04_load20141015-20141226.csv'
  df = pd.read_csv(df_path, index_col=0)
  #df1_path = '../csv_output_temp/Floor04_loadS4-0120141226-20141226_tempo.csv'
  df1_path = '../csv_output_temp/TEMP_Floor04_loadS4-20141226-20141226_tempo.csv'
  df1 = pd.read_csv(df1_path, index_col=0)
  pdb.set_trace()
  for z in range(10,11):
    dfz_path = '../csv_output_temp/Floor04_loadS4-%s20141226-20141226_tempo.csv' %(str(z).zfill(2))
    dfz = pd.read_csv(dfz_path, index_col=0) 
    #df1 = df1.join(dfz) 
    #pdb.set_trace()
    df1 = pd.merge(df1, dfz, how='outer', on=['timestamp', 'datetime'], 
                   right_index=False, left_index=False) 
  pdb.set_trace()
  df1.to_csv('../csv_output_temp/TEMP_Floor04_loadS4-20141226-20141226_tempo.csv')
  
  pdb.set_trace()
  df = pd.merge(df, df1, how='outer', on=['timestamp', 'datetime'], 
                left_index=False, right_index=False) 
  pdb.set_trace() 
  df.to_csv('../csv_output_temp/TEMP_Floor04_loadS4-20141226-20141226_tempo.csv')
#  for z in range(4,6):
#    dfz_path = '../csv_output_temp/Floor07_temp_tav_zoneS7-%s20151226-20151226_tempo.csv' %(str(z).zfill(2))
#    dfz = pd.read_csv(dfz_path, index_) 
#    #df1 = df1.join(dfz) 
#    #pdb.set_trace()
#    df1 = pd.merge(df1, dfz, how='outer', on=['timestamp', 'datetime'], right_index=False) 
  pdb.set_trace()
#  df = pd.merge(df, df1, how='outer', on=['timestamp', 'datetime'], right_index=False) 
#  df.to_csv('../csv_output_temp/TEMP_Floor07_temp_tav_zoneS7-20151226-20151226_tempo.csv')
#  pdb.set_trace()
#  for z in range(6,8):
#    dfz_path = '../csv_output_temp/Floor07_temp_tav_zoneS7-%s20151226-20151226_tempo.csv' %(str(z).zfill(2))
#    dfz = pd.read_csv(dfz_path) 
#    #df1 = df1.join(dfz) 
#    #pdb.set_trace()
#    df1 = pd.merge(df1, dfz, how='outer', on=['timestamp', 'datetime'], right_index=False) 
#    pdb.set_trace()
#  df = pd.merge(df, df1, how='outer', on=['timestamp', 'datetime'], right_index=False) 
#  df.to_csv('../csv_output_temp/TEMP_Floor07_temp_tav_zoneS7-20151226-20151226_tempo.csv')
#  pdb.set_trace()
#  for z in range(8,9):
#    dfz_path = '../csv_output_temp/Floor07_temp_tav_zoneS7-%s20151226-20151226_tempo.csv' %(str(z).zfill(2))
#    dfz = pd.read_csv(dfz_path) 
#    #df1 = df1.join(dfz) 
#    #pdb.set_trace()
#    df1 = pd.merge(df1, dfz, how='outer', on=['timestamp', 'datetime'], right_index=False) 
#    pdb.set_trace()
#  df = pd.merge(df, df1, how='outer', on=['timestamp', 'datetime'], right_index=False) 

#  source_energy = 'Sutardja Dai Hall Energy Data'
#  points_energy = ['zone_load']
#  path_and_energy = ['energy_data/', 'variable_elec_cost/']
#  zones_names = ['S1-16', 'S1-18']
#  restrict_root_energy = restrict(source_energy, points_energy, path_and_energy)
#  restrict_all_energy = restrict_zones(restrict_root_energy, zones_names, path_and=None)
#  pp.pprint(restrict_all_energy)
#  pdb.set_trace()
#
#  points = ['ROOM_TEMP', 'CTL_STPT']
#  source = 'Sutardja Dai Hall BACnet'
#  points_tav = ['tav_active']
#  path_and_tav = ['tav_whole_bldg/']
#  source_tav = 'Sutardja Dai Hall TAV'
#  zones_names = [ 'S1-01', 'S2-09']
#  restrict_root = '(' + restrict(source, points) + ') or (' + \
#                        restrict(source_tav, points_tav, path_and_tav) + ')'
#  restrict = restrict_zones(restrict_root, zones_names, path_and=None)
#  pp.pprint(restrict)
#  pdb.set_trace()
#  c = SmapClient(base='http://new.openbms.org/backend',\
#                 key=['XuETaff882hB6li0dP3XWdiGYJ9SSsFGj0N8'])
#  startDate = date(2015,10,15).strftime("%m/%d/%Y %H:%M")
#  endDate = date(2015, 10, 16).strftime("%m/%d/%Y %H:%M")
#  window='apply window(first, field=\"minute\", width=3) to'
#  query_data = query_data(c, restrict, startDate, endDate, win=window)
#  pdb.set_trace()
#
#  # Test to repopulate the ration airflow/max with existing datasets
#  points = ['AIR_VOLUME', 'CTL_FLOW_MIN', 'CTL_FLOW_MAX']
#  floors = [str(f) for f in [6,5,4,7,3,2,1]]
#  for f in floors:
#    df4_path = '../csv_output/Floor2015_3min/Floor%s_airflow20151015-20151226.csv'%(str(f).zfill(2))
#    df4 = pd.read_csv(df4_path,  index_col=0) 
#    df4_aug = data_ratios(df4, points)
#    df4_aug.to_csv('../csv_output/Floor2015_3min/Floor%s_ratios20151015-20151226_new.csv'%(str(f).zfill(2)))
#  pdb.set_trace()
    
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

