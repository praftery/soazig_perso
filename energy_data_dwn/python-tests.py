from smap.archiver.client import SmapClient
from smap.contrib import dtutil

import pdb, time, datetime
import numpy as np
import pdb
import smtplib
import pprint as pp

c = SmapClient("http://new.openbms.org/backend")
t = time.time()
source = 'Sutardja Dai Hall TAV'
error_uuid_list = ['36335391-49a6-5b21-9f00-fcce66eb5a74']
N = len(error_uuid_list)
where = "Metadata/SourceName = '%s' and (" %(source)\
                    + ' or\n '.join(["uuid = '%s'"] * N) \
                    %tuple(error_uuid_list) + ")"
tags=c.tags(where)
error_dict = {}
error_list = [[1441431059000.0, 1.0],[1441430819000.0, 1.0]]
#error_list = [[1441605186000.0, 0.0]]
pp.pprint(error_list)
for tag in tags:
  name = str(tag['Path'].split('/')[1]) 
  u = str(tag['uuid'])
  if name not in error_dict:
    error_dict[name] = {}
    if u not in error_dict[name]:
      error_dict[name]['uuid'] = u
      error_dict[name]['Readings'] = [] 

def error_array_fill():
  print "\n*************Populating arrays\n"
  for name in error_dict:
    error = []
    u = error_dict[name]['uuid'] 
    array = [d['Readings'] for d in data if d['uuid'] == u][0]
    if tref - array[0][0]/1000 > 180:
      raise Exception("\nQueried smap data is stale - over %s seconds old\n"\
                      %(str(tref - array[0][0]/1000)))   
    error = [r for r in array if r[1]==0.0]
    print "\nError_list: ", error_list
    if error_list:
      print "Filtering from error_list"
      most_recent = max(zip(*error_list)[0])
      print "most recent: ", most_recent
      print [(r[0] - most_recent)/1000 for r in error]
      error = [r for r in error if (r[0] - most_recent)/1000 > 120]
      print "NEW error ", error
    error_list.extend(error)
    print "NEW Error_list: ", error_list
    error_dict[name]['Readings'].extend(array)
    print "\nError_dict: "
    pp.pprint(error_dict)
    if error:
      print "An error occured on " + str(name)

print "\n**************Querying data\n"
tref = int(1441431239)
data = c.prev(where, tref, streamlimit=10, limit=1)
print "Data"
pp.pprint(data)
try:
  error_array_fill()
except Exception, e:
  print "\n*** Error: %s \n "%e
  template = "An exception of type {0} occured. Arguments:\n{1!r}"
  message = template.format(type(e).__name__, e.args) 

pdb.set_trace()  
