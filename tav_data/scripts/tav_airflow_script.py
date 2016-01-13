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

import tav_graphs_functions

#c = SmapClient("http://new.openbms.org/backend")
c = SmapClient(base='http://new.openbms.org/backend',\
               key=['XuETaff882hB6li0dP3XWdiGYJ9SSsFGj0N8'])
source = 'Sutardja Dai Hall BACnet'
points =['AIR_VOLUME'] 
zones_names =['S[0-9]-[0-9][0-9]'] 

for j in ['2014', '2015']:
  for i in range(1,13):
    startDate = str(i) + "/01/" + str(j)
    endDate = str(i+1) + "/01/" + str(j)
    print startDate, endDate
    restrict = tav_graphs_functions.restrict(source, zones_names, points)
    query_data = tav_graphs_functions.query_data(c, restrict, startDate, endDate)
    tags = query_data[0]
    data = query_data[1]
    data_frame = tav_graphs_functions.data_frame(data, tags, points)
    data_frame.to_csv('SDH_airflow_' + str(i).zfill(2) + str(j) + '.csv')

#    pdb.set_trace()

