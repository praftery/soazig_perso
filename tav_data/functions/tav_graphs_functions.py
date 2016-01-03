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

def query_data(c, restrict, startDate, endDate):
#  if mode == 'hour':
  query_data = 'apply window(first, field="hour") to data in ("' + \
                 str(startDate) + '" , "' + str(endDate) + \
                 '") limit 10000000 where ' + restrict
#  else:
#  query_data = 'select data in ("' + str(startDate) + '" , "' + str(endDate) + '") where ' + restrict
  data = c.query(query_data)
  tags = c.tags(restrict)
#  start = dtutil.dt2ts(dtutil.strptime_tz(str(startDate), "%m/%d/%Y"))
#  end = dtutil.dt2ts(dtutil.strptime_tz(str(endDate), "%m/%d/%Y"))
#  data = c.data(restrict, start, end)
  return tags, data

def data_frame(data, tags, points):
  N = len(data)
  dt_format = '%Y-%m-%d %H:%M:%S'
  df = pd.DataFrame()
  d = np.array(data[0]['Readings'])
  df['timestamp'] = d[:,0]
  df['datetime'] = [datetime.datetime.fromtimestamp(x/1000).strftime(dt_format)
                    for x in d[:,0]]
  for i in range(N):
    d = np.array(data[i]['Readings'])
    if d.any():
      try:
        for p in points:
          if p in data[i]['Path']:
            try:
              z_name = str(data[i]['Path'].split('/')[-2] + '_') + p
              df[z_name] = d[:,1]
              print "Dwld data for: ", z_name          
            except:
              pdb.set_trace()
      except:
        pdb.set_trace()
  return df

#def pd_stats(data_frame):
  




#def zones_dict_init(data, points):
#  dico = {}
#  dico['timestamp']= []
#  dico['datetime'] = []
#  for d in data:
#    z_name = str(d['Path'].split('/')[-2])
#    p = str(d['Path'].split('/')[-1])
#    uuid = str(d['uuid'])
#    if z_name not in dico:
#      dico[z_name] = {}
#    if p in points:
#      if p not in dico[z_name]:
#        dico[z_name][p]={}
#    if uuid not in dico[z_name][p]:
#      dico[z_name][p]['uuid'] = uuid
#      dico[z_name][p]['Readings'] = []
#  return dico
#
#def fill_zones_dict(data, dico, points):
#  N = len(data)
#  dt_format = '%Y-%m-%d %H:%M:%S'
#  d = np.array(data[0]['Readings'])
#  dico['timestamp'] = d[:,0]
#  dico['datetime'] = [datetime.datetime.fromtimestamp(x/1000).strftime(dt_format)
#                    for x in d[:,0]]
#  for z in [z in dico.keys()
#
#  for d in data:
#    d_r = np.array(d['Readings'])
#    if d_r.any():
#      try:
#        for p in points:
#          if p in d['Path']:
#            try:
#              z_name = str(d['Path'].split('/')[-2])
#              dico[z_name] = d_r[:,1]
#              print "Dwld data for: ", z_name          
#            except:
#              pdb.set_trace()
#      except:
#        pdb.set_trace()
#  return df













  
