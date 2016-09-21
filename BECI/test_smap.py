from smap.archiver.client import SmapClient
from datetime import timedelta, date

import sys, os, pdb
import time
import pandas as pd
import numpy as np
import datetime
import math

c = SmapClient(base='http://new.openbms.org/backend')
#               key=['XuETaff882hB6li0dP3XWdiGYJ9SSsFGj0N8'])
uuid_dict = { 
             'uuid1': {'u':"b7051656-d8d5-53dd-9221-15de4ce84e43",
                       'name':"MSA.MAIN.PWR_REAL_3_P"},
             'uuid2': {'u':"cc1dfe56-3abc-544e-add6-1bc88712fc90",
                       'name':"MSB.MAIN.PWR_REAL_3_P"}}
restrict = 'Metadata/SourceName = "Sutardja Dai Hall BACnet" and (uuid ="%s" or uuid = "%s")'\
            %(str(uuid_dict['uuid1']['u']), str(uuid_dict['uuid2']['u']))            
#pdb.set_trace()
#TODO: Change Date range here
startF=date(2015,7,17)
endF=date(2016,7,17)

#Create an empty dataframe to put the data in
dts_startF = time.mktime(startF.timetuple())
dts_endF = time.mktime(endF.timetuple())
step_in_seconds = 1*60
#dff = pd.DataFrame()
#dff['timestamp'] = np.arange(dts_startF, dts_endF, step_in_seconds)
#dff['datetime'] = [datetime.datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S")
#                                for x in dff['timestamp']]
#pdb.set_trace()

## Limited by the number of days to query (~8)
delta = datetime.timedelta(days=366)
endFp = endF - datetime.timedelta(days=1)
start = startF
end = start + min(delta, (endF - start))
time_frames = []
while start != end:
  startDate = start.strftime("%m/%d/%Y %H:%M")
  endDate = end.strftime("%m/%d/%Y %H:%M")
  print "Start date: ", startDate
  print "End date: ", endDate
  query_data = 'select data in ("%s","%s") limit 10000000 where ' \
             %(str(startF.strftime("%m/%d/%Y")), \
               str(endF.strftime("%m/%d/%Y"))) + restrict
  data = c.query(query_data)
  tags = c.tags(restrict)
  N = len(data)
  df = pd.DataFrame()
  for i in range(N):
    u = data[i]['uuid']
    d = np.array(data[i]['Readings'])
  #  pdb.set_trace()
    if d.any():
      tag_path = [tag['Path'] for tag in tags if tag['uuid'] == u][0]
  #    pdb.set_trace()
      try:
  #      pdb.set_trace()
        for name in [uuid_dict['uuid1']['name'], uuid_dict['uuid2']['name']]:
          if name in tag_path:
            header = name 
            df2 = pd.DataFrame(d, columns=['timestamp', header])
            df2['timestamp'] = df2['timestamp']/1000.0
            if i < 1:
              df = df2
            else:
              df = pd.merge(df, df2, how='outer', on=['timestamp']) 
              #df = pd.concat([df, df2]) 
            #pdb.set_trace()
            #dff = pd.merge(dff, df, how='outer', on=['timestamp']) 
            print " Dwnl data for " + header
      except:
        print "Could not dwn data for " + '_'.join(tag_path.split('/')[-1:])
        pdb.set_trace()
  pdb.set_trace()
#  dff['datetime'] = [datetime.datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S")
#                                for x in dff['timestamp']]
  df.sort(['timestamp'], ascending=[True], inplace=True)
  #df.dropna(axis=0)
  df.index = range(0,len(df))
  #df.interpolate()
  pdb.set_trace()
  df.to_csv("temp/test_smap-%s-%s.csv" \
               %(str(start.strftime("%Y%m%d")),str(end.strftime("%Y%m%d"))))
  start = end
  end += min(delta, (endF - end))

#List of timesteps 
#Find the nearest multiple of 60 of df[0] and df[-1]
dts_start = math.floor(np.array(df['timestamp'])[0]/60.0)*60.0
dts_end = math.ceil(np.array(df['timestamp'])[-1]/60.0)*60.0
dts_target = np.arange(dts_start, dts_end + 60.0, step_in_seconds)
#pdb.set_trace()
#Interpolate the data_ts
interp_data = {}
#interp_data['timestamp'] = dts_target
for header in [h for h in df if h not in ['timestamp']]:
  print header
  df[header] = df[header].interpolate() 
  interp_data[header] = np.interp(dts_target, \
                        np.array(df['timestamp']),\
                        np.array(df[header]))
dff1 = pd.DataFrame(interp_data) 

pdb.set_trace()
date_extremes = [datetime.datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S")
                 for x in [dts_start, dts_end]] 
index_range = pd.date_range(date_extremes[0], date_extremes[-1], freq='1min') 
dff1.index = index_range
pdb.set_trace()

#15 min average data
dff15 = dff1.resample('15min', how=np.mean)

dff1.to_csv("test_smap-%s-%s_1min.csv" %(str(startF.strftime("%Y%m%d")),str(endF.strftime("%Y%m%d"))))
dff15.to_csv("test_smap-%s-%s_15min.csv" %(str(startF.strftime("%Y%m%d")),str(endF.strftime("%Y%m%d"))))

#pdb.set_trace()

print "\n============================= DATA DOWNLOAD COMPLETE ==================================\n"
#pdb.set_trace()

