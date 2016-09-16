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
#pdb.set_trace()
#TODO: Change Date range here
startF=date(2015,7,16)
endF=date(2015,7,31)
restrict = 'Metadata/SourceName = "Sutardja Dai Hall BACnet" and (uuid ="%s" or uuid = "%s")'\
            %(str(uuid_dict['uuid1']['u']), str(uuid_dict['uuid2']['u']))            

#Create an empty dataframe to put the data in
dts_startF = time.mktime(startF.timetuple())
dts_endF = time.mktime(endF.timetuple())
step_in_seconds = 1*60
dff = pd.DataFrame()
dff['timestamp'] = np.arange(dts_startF, dts_endF, step_in_seconds)
#dff['datetime'] = [datetime.datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S")
#                                for x in dff['timestamp']]
#pdb.set_trace()

## Limited by the number of days to query (~8)
delta = datetime.timedelta(days=7)
endFp = endF - datetime.timedelta(days=1)
start = startF
end = start + min(delta, (endF - start))
time_frames = []
while start != end:
  startDate = start.strftime("%m/%d/%Y %H:%M")
  endDate = end.strftime("%m/%d/%Y %H:%M")
  print "Start date: ", startDate
  print "End date: ", endDate
  query_data = 'select data in ("%s","%s") where ' \
             %(str(startF.strftime("%m/%d/%Y")), \
               str(endF.strftime("%m/%d/%Y"))) + restrict
  data = c.query(query_data)
  tags = c.tags(restrict)

  N = len(data)
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
            df = pd.DataFrame(d, columns=['timestamp', header]) 
            df['timestamp'] = df['timestamp']/1000.0
            #List of timesteps 
            #Find the nearest multiple of 60 of df[0] and df[-1]
            dts_start = math.floor(np.array(df['timestamp'])[0]/60.0)*60.0
            dts_end = math.ceil(np.array(df['timestamp'])[-1]/60.0)*60.0
            dts_target = np.arange(dts_start, dts_end, step_in_seconds)
            #Inerpolate the data_ts
            interp_data = np.interp(dts_target, \
                                    np.array(df['timestamp']),\
                                    np.array(df[header]))
            df_target = pd.DataFrame({'timestamp': dts_target,
                                      '%s' %(str(header)): interp_data}) 
            dff = pd.merge(dff, df_target, how='outer', on=['timestamp']) 
            print " Dwnl data for " + header
      except:
        print "Could not dwn data for " + '_'.join(tag_path.split('/')[-1:])
        pdb.set_trace()
    
  dff['datetime'] = [datetime.datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S")
                                for x in dff['timestamp']]
  dff.to_csv("temp/test_smap-%s-%s_interpolate.csv" \
               %(str(start.strftime("%Y%m%d")),str(end.strftime("%Y%m%d")))))
  start = end
  end += min(delta, (endF - end))

#pdb.set_trace()

date_extremes = [datetime.datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S")
                 for x in [np.array(dff['timestamp'])[0], np.array(dff['timestamp'])[-1]]] 
index_range = pd.date_range(date_extremes[0], date_extremes[-1], freq='1min') 
dff1 = pd.DataFrame(index = index_range)
for name in [uuid_dict['uuid1']['name'], uuid_dict['uuid2']['name']]:
  dff1[name] = np.array(dff[name])

#15 min average data
dff15 = dff1.resample('15min', how=np.mean)

dff1.to_csv("test_smap-%s-%s_1min.csv" %(str(startF.strftime("%Y%m%d")),str(endF.strftime("%Y%m%d"))))
dff15.to_csv("test_smap-%s-%s_15min.csv" %(str(startF.strftime("%Y%m%d")),str(endF.strftime("%Y%m%d"))))

#pdb.set_trace()

print "\n============================= DATA DOWNLOAD COMPLETE ==================================\n"
pdb.set_trace()

