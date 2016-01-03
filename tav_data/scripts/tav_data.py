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

c = SmapClient(base='http://new.openbms.org/backend',\
               key=['XuETaff882hB6li0dP3XWdiGYJ9SSsFGj0N8'])
t = time.time()
source = 'Sutardja Dai Hall BACnet'
path_list = [
             'HEAT.COOL',
             'VLV_POS',
             'CTL_STPT',
             'ROOM_TEMP',
             'AI_3',
             'CTL_FLOW_MIN',
             'DMPR_POS',
             'AIR_VOLUME'
             ] 
source_tav = 'Sutardja Dai Hall TAV'
path_and = 'tav_whole_bldg'
path_list_tav = [
                'tav_active',
                '/cycle'
                ]
zone_name = 'S5-03'
restrict = " Metadata/SourceName = '%s' and Path ~ '%s' and ("\
             %(source, zone_name)\
             + ' or '.join(["Path ~ '%s'"] * len(path_list)) \
             %tuple(path_list) + ")"

restrict2 = " Metadata/SourceName = '%s' and Path ~ '%s' and Path ~ '%s' and ("\
             %(source_tav, zone_name, path_and) \
             + ' or '.join(["Path ~ '%s'"] * len(path_list_tav)) \
             %tuple(path_list_tav) + ")"

restrictall = "(" + restrict + ") or (" + restrict2 + ")"
tags = c.tags(restrictall)

print "Tags len: ", len(tags)
startDate = "10/01/2015 14:00"
endDate = "10/01/2015 21:01"
#start = dtutil.dt2ts(dtutil.strptime_tz("08-28-2015", "%m-%d-%Y"))
#end   = dtutil.dt2ts(dtutil.strptime_tz("08-29-2015", "%m-%d-%Y"))
#data = c.data(restrictall, start, end)
#pp.pprint(data)
#pdb.set_trace()

name = zone_name + '_tav_cycle.csv' 
dt_format = '%Y-%m-%d %H:%M:%S'
query_data = 'select data in ("' + startDate + '" , "' + endDate + '") where' + restrictall
data = c.query(query_data)
#data = c.data(restrictall, start, end, limiit=1000)

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
    for p in path_list_tav + path_list:
      if p in tag_path:
        df[p] = d[:,1]
      print " Dwnl data for " + p

df.to_csv(name)
