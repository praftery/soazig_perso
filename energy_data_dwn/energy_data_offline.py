"""
@author Soazig Kaam <soazig.kaam@berkeley.edu>
"""

from smap.archiver.client import SmapClient
from smap.contrib import dtutil
from datetime import datetime, date

import pdb, time
import numpy as np
import pandas as pd
import pprint as pp
import quantities as pq
import csv
import datetime

# SETTINGS

motor_ratings = {'AH2A' : 100.0,
                 'AH2B' : 100.0}

# ELECTRICITY PRICE
cost_mode = 'VARIABLE'

if cost_mode == 'FIXED':
  p = '/fixed_elec_cost'
else:
  p = '/variable_elec_cost'

# Point names of streams to write
pointnames = [
             'electricity_price',
             'chw_plant_total_efficiency',
             'hot_water_price',
             'AH2A_supply_fan_power'
             'AH2B_supply_fan_power',
             'AH2_total_supply_fan_power',
             'AH2_total_supply_fan_power_cost',
             'AH2_panel_fan_power',
             'AH2_panel_fan_power_cost',
             'chilled_water_AH2',
             'chilled_water_AH2_cost',
             'hot_water_AH2',
             'hot_water_AH2_cost',
             'total_zone_load_AH2',
             'measured_chilled_water',
             'measured_chilled_water_cost',
             'total_cost',
             'total_VAV_box_airflow'
             ]
chw_coils = ['AH2A', 'AH2B']
chw_coils = dict(zip(chw_coils,[0.0]*len(chw_coils)*pq.F))
chw_stream_names = []
for name in chw_coils:
  chw_stream_names += ['coil_closed_temp_change_' + name] + \
                      ['chilled_water_' + name]
#rh_coils = []
rh_coils = [
            'S1-01', 'S1-02', 'S1-03', 'S1-04', 'S1-07', 'S1-08'
            'S1-09', 'S1-10', 'S1-13', 'S1-15', 'S1-16', 'S1-17',
            'S1-18', 'S1-19', 'S1-20', 'S2-01', 'S2-02', 'S2-03',
            'S2-04', 'S2-05', 'S2-06', 'S2-07', 'S2-10', 'S2-11',
            'S2-12', 'S2-13', 'S2-14', 'S2-15', 'S2-16', 'S2-17',
            'S2-18', 'S2-19', 'S2-20', 'S2-21', 'S3-01', 'S3-02',
            'S3-03', 'S3-04', 'S3-05', 'S3-06', 'S3-07', 'S3-08',
            'S3-09', 'S3-10', 'S3-11', 'S3-12', 'S3-15', 'S3-16',
            'S3-17', 'S3-18', 'S3-19', 'S3-20', 'S3-21', 'S4-01',
            'S4-02', 'S4-03', 'S4-04', 'S4-05', 'S4-06', 'S4-07',
            'S4-08', 'S4-09', 'S4-11', 'S4-12', 'S4-13', 'S4-15',
            'S4-16', 'S4-18', 'S4-19', 'S4-20', 'S4-21', 'S5-01',
            'S5-02', 'S5-03', 'S5-04', 'S5-05', 'S5-06', 'S5-07',
            'S5-08', 'S5-09', 'S5-10', 'S5-11', 'S5-12', 'S5-13',
            'S5-14', 'S5-16', 'S5-18', 'S5-19', 'S5-20', 'S5-21',
            'S6-01', 'S6-02', 'S6-03', 'S6-04', 'S6-05', 'S6-06',
            'S6-07', 'S6-08', 'S6-10', 'S6-11', 'S6-12', 'S6-13',
            'S6-15', 'S6-17', 'S6-18', 'S6-19', 'S6-20', 'S7-01',
            'S7-02', 'S7-03', 'S7-04', 'S7-05', 'S7-06', 'S7-07',
            'S7-08', 'S7-09', 'S7-10', 'S7-13', 'S7-14', 'S7-15',
            'S7-16'
           ]
rh_coils = dict(zip(rh_coils,[2.0]*len(rh_coils)*pq.F))
rh_stream_names = []
for name in rh_coils:
  rh_stream_names += ['coil_closed_temp_change_' + name] + \
                     ['hot_water_' + name] + \
                     ['instantaneous_zone_load_' + name]

# Query necessary tags for energy data
c = SmapClient(base='http://new.openbms.org/backend',\
               key=['XuETaff882hB6li0dP3XWdiGYJ9SSsFGj0N8'])
source_energy = 'Sutardja Dai Hall Energy Data'
all_points = pointnames 
where_energy = " Metadata/SourceName = '%s' and Path ~ '%s' and (" \
                 %(source_energy, p)\
                 + ' or '.join(["Path ~ '%s'"] * len(all_points))\
                 %tuple(all_points) + ")"

tags = c.tags(where_energy)
startDate = "03/07/2015"
endDate = "07/24/2015"
name = 'energy_data_2015_nocalc.csv'
query_data = 'apply window(first, field="hour") to data in ("' \
             + str(startDate)  + '" , "' + str(endDate) \
             + '") limit 10000000 where' + where_energy 
data = c.query(query_data)

N=len(data)
df = pd.DataFrame()
d = np.array(data[0]['Readings'])
df['timestamp'] = d[:,0]
dt_format = '%Y-%m-%d %H:%M:%S'
df['datetime'] = [datetime.datetime.fromtimestamp(x/1000).strftime(dt_format)
                  for x in d[:,0]]
for i in range(N):
  d = np.array(data[i]['Readings'])
  if d.any():
    df['timestamp'] = d[:,0]
    df['datetime'] = [datetime.datetime.fromtimestamp(x/1000).strftime(dt_format) for x in d[:,0]]
    for p in all_points:
      if p in data[i]['Path']:
        try:
          df[p] = d[:,1]
        except:
         pdb.set_trace()
        print "Data for " + p + " downloaded."

df.to_csv(name)

