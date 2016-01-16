"""
This script is the offline version of the energy_data.py driver.
This script works on historical data of SDH and performs energy calculations
on the data. The script writes the relevant timeseries in a csv file and adds
them on smap. The script can be used to estimate an overview of the potential 
savings and performance of the air handler resulting from the strategies
currently implemented in SDH.

@author Soazig Kaam <soazig.kaam@berkeley.edu>
@author Paul Raftery <praftery@berkeley.edu>
"""

from smap.archiver.client import SmapClient
from smap.contrib import dtutil
from datetime import datetime, date
import os, pdb, time
import numpy as np
import pandas as pd
import pprint as pp
import quantities as pq
import csv
import json
import requests
import subprocess

import e20_electricity_tariff
import vfd
import energy_calcs
import coil_power as cp
import chiller
import zone_load as zl

# SETTINGS
# JSON file  
startDate = "09/04/2014"
endDate = "09/05/2014"
#j_name = 'json_files/power_cost_%s%s%s.json' %(startDate.split('/')[-1], startDate.split('/')[-2],\
#                    startDate.split('/')[-3])
csv_name = 'energy_data_20140904_none.csv'
csv_name_zone = 'energy_data_zone_20140904_non.csv'

#ELECTRICITY PRICE
cost_mode = 'VARIABLE'

if cost_mode == 'FIXED':
  p = '/fixed_elec_cost'
else:
  p = '/variable_elec_cost'

def elec_cost(datetime_obj, mode=None):
  if mode == "FIXED":
    elec = 0.1
  else:
    elec = e20_electricity_tariff.price(datetime_obj, 2)
  return elec

def ts_dt(date_timestamp):
  date_formatted = datetime.fromtimestamp(date_timestamp) 
  return date_formatted

hw_price = float(0.023)

# CHILLER EFFICIENCY
chw_eff = float(5.0*chiller.cop())
## Point names of streams to write
motor_ratings = {'AH2A' : 100.0,
                 'AH2B' : 100.0}
pointnames = [
             'electricity_price',
             'chw_plant_total_efficiency',
             'hot_water_price',
             'AH2A_supply_fan_power',
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

c = SmapClient(base='http://new.openbms.org/backend',\
               key='XuETaff882hB6li0dP3XWdiGYJ9SSsFGj0N8')

# Query necessary tags for energy data
source_energy = 'Sutardja Dai Hall Energy Data'
all_points = pointnames + rh_stream_names + chw_stream_names
where_energy = "Metadata/SourceName = '%s' and Path ~ '%s' and (" \
                 %(source_energy, p)\
                 + ' or '.join(["Path ~ '%s'"] * len(all_points))\
                 %tuple(all_points) + ")"
tags_energy = c.tags(where_energy)

# Query data for energy calcs as AHU level
source = 'Sutardja Dai Hall BACnet'
path_list = {
                'AH2A_SF_VFD' : 'SDH/AH2A/SF_VFD:POWER',
                'AH2B_SF_VFD' : 'SDH/AH2B/SF_VFD:POWER',
                'panel_power' : 'SDH/SW/MSA.CD4RA.PWR_REAL_3_P',
                'measured_chw' : 'SDH/CHW/OFFICE.TONNAGE',
                'AH2A_SAT' : 'SDH/AH2A/SAT',
                'AH2B_SAT' : 'SDH/AH2B/SAT',
                'AH2A_MAT' : 'SDH/AH2A/MAT',
                'AH2B_MAT' : 'SDH/AH2B/MAT',
                'AH2A_CCV' : 'SDH/AH2A/CCV',
                'AH2B_CCV' : 'SDH/AH2B/CCV',
                'AH2A_SF_CFM' : 'SDH/AH2A/SF_CFM',
                'AH2B_SF_CFM' : 'SDH/AH2B/SF_CFM'
                } 
path_list_not = ['STP', 'RAH']

restrict = "Metadata/SourceName = '%s' and ("%source\
                 + ' or '.join(["Path ~ '%s'"] * len(path_list)) \
                 %tuple(path_list.values()) + ") and not (" \
                 + ' or '.join(["Path ~ '%s'"] * len(path_list_not)) \
                 %tuple(path_list_not) + ")"
tags = c.tags(restrict)

## Query data for energy calcs at zone level
path_list_zone = ['AI_3', 'VLV_POS', 'AIR_VOLUME', 'ROOM_TEMP']
restrict_zone = "Metadata/SourceName = '%s' and Path ~ 'S[0-9]-[0-9][0-9]' and ("%source\
                 + ' or '.join(["Path ~ '%s'"] * len(path_list_zone)) \
                 %tuple(path_list_zone) + ")"
tags_zone = c.tags(restrict_zone)
## Initiate dico to write streams on smap/csv file
#power_cost = {}
#for tag in tags + tags_energy:
#  if tag in tags:
#    n = [name for name in path_list if path_list[name] in tag['Path']][0]
#  elif tag in tags_energy:
#    n = str(tag['Path'].split('/')[-1])
#  uuid = str(tag['uuid'])
#  if n in pointnames + path_list.keys():
#    if n not in power_cost:
#      power_cost[n] = {}
#    power_cost[n]['uuid'] = uuid
#    power_cost[n]['Readings'] = []
#  elif n in rh_stream_names + chw_stream_names:
#    z = str(n.split('_')[-1])
#    s  = '_'.join(n.split('_')[:-1])
#    if z not in power_cost:
#      power_cost[z] = {}
#    if s not in power_cost[z]:
#      power_cost[z][s] = {}
#    power_cost[z][s]['uuid'] = uuid
#    power_cost[z][s]['Readings'] = []

# Write the power_cost JSON file initialized
#json.dump(power_cost, j_power_cost_ini, sort_keys=True)
#pdb.set_trace()
#j_power_cost_ini = open('json_files/power_cost_ini.json', 'r')
# Load the data of the empty dicto into power_cost
#power_cost=json.load(j_power_cost_ini)
#j_power_cost_ini.close()

# DATA QUERY zone 
#query_data_zone = 'apply window(first, field="minute") to data in ("' + str(startDate) + '" , "' + str(endDate)\
#             + '") where ' + restrict_zone 
#data_zone = c.query(query_data_zone)
#Nzone=len(data_zone)
#print "Nzone: ", Nzone
#
#df_zone = pd.DataFrame()
#d_zone = np.array(data_zone[0]['Readings'])
#df_zone['timestamp'] = d_zone[:,0]
#dt_format = '%Y-%m-%d %H:%M:%S'
#import datetime
#df_zone['datetime'] = [datetime.datetime.fromtimestamp(x/1000).strftime(dt_format)
#                  for x in d_zone[:,0]]
#for i in range(Nzone):
#  u = data_zone[i]['uuid']
#  d_zone = np.array(data_zone[i]['Readings'])
#  if d_zone.any():
#    for rh in rh_coils:
#      for p in path_list_zone: 
#        if rh in data_zone[i]['Path'] and p in data_zone[i]['Path']:
#          try:
#            df_zone[rh + '_' + p] = d_zone[:,1]
#          except:
#            pdb.set_trace()
#          print "Data for " + rh + '_' + p + " downloaded."
#df_zone.to_csv(csv_name_zone)
##
##pdb.set_trace()
# DATA QUERY AHU
#query_data = 'apply window(first, field="minute") to data in ("' + str(startDate) + '" , "' + str(endDate)\
#             + '") where ' + restrict 
#data = c.query(query_data)
#N=len(data)
#print "N: ", N
#
#df = pd.DataFrame()
#d = np.array(data[0]['Readings'])
#df['timestamp'] = d[:,0]
#dt_format = '%Y-%m-%d %H:%M:%S'
#import datetime
#df['datetime'] = [datetime.datetime.fromtimestamp(x/1000).strftime(dt_format)
#                  for x in d[:,0]]
#for i in range(N):
#  u = data[i]['uuid']
#  d = np.array(data[i]['Readings'])
#  if d.any():
#    for p in path_list:
#     if path_list[p] in data[i]['Path']:
#        try:
#          df[p] = d[:,1]
#        except:
#          pdb.set_trace()
#        print "Data for " + p + " downloaded."
#df.to_csv(csv_name)
#pdb.set_trace()

## to load the dataframe:
df = pd.read_csv(csv_name)
#df_zone = pd.read_csv(csv_name_zone)

# 1. ELECTRICITY and HOT WATER PRICE + chilled water efficiency
df['electricity_price']=[elec_cost(ts_dt(val/1000.0)) for val in df['timestamp']]
df['hot_water_price'] = [hw_price for val in df['timestamp']] 
df['chw_plant_total_efficiency'] = [chw_eff for val in df['timestamp']]

## Fill power_cost dico with readings from data
#for path in path_list:
#  if path in power_cost:
#    u = power_cost[path]['uuid']
#    r = [d['Readings'] for d in data if d['uuid'] == u][0]
#    power_cost[path]['Readings'].extend(r)

# 1. ELECTRICITY and HOT WATER PRICE + chilled water efficiency
#elec_price = np.empty((N,2))
#elec_price[:,0] = np.array(data[0]['Readings'])[:,0]
#elec_price[:,1] = [elec_cost(ts_dt(val/1000.0)) \
#                       for val in elec_price[:,0]][0]
#hotw_price = np.empty((N,2))
#hotw_price[:,0] = np.array(data[0]['Readings'])[:,0]
#hotw_price[:,1] = [hw_price]*N
#chilledw_eff = np.empty((N,2))
#chilledw_eff[:,0] = np.array(data[0]['Readings'])[:,0]
#chilledw_eff[:,1] = [chw_eff]*N

# 2. FAN POWER AND ELECTRICITY COSTS    
# 2. 1. Supply fan power 
# Use data from VFDs and motor sizes + convert VFD % into kW
for coil_name in chw_coils:
  df_temp = [vfd.convert_to_kW(sf_vfd_value,\
             motor_ratings[coil_name]) \
             for sf_vfd_value in df[str(coil_name) + '_SF_VFD']]
  df[str(coil_name) + '_supply_fan_power'] = df_temp 
df['AH2_total_supply_fan_power'] = df['AH2A_supply_fan_power'] + \
                                   df['AH2B_supply_fan_power']
#pd.DataFrame(df['AH2_total_supply_fan_power'].values\
#             *df['electricity_price'].values,\
#              columns=df.columns, index=df.index) 
#pdb.set_trace()
#df['AH2_total_supply_fan_power_cost'] 
#AH2A_sf_vfd = np.array(power_cost['AH2A_SF_VFD']['Readings'])
#AH2A_sf_fp = np.empty((N,2))
#AH2A_sf_fp[:,0] = AH2A_sf_vfd[:,0] 
#AH2A_sf_fp[:,1] = [vfd.convert_to_kW(sf_vfd_value, motor_ratings['AH2A']) \
#                   for sf_vfd_value in AH2A_sf_vfd[:,1]]
#
#AH2B_sf_vfd = np.array(power_cost['AH2B_SF_VFD']['Readings'])
#AH2B_sf_fp = np.empty((N,2))
#AH2B_sf_fp[:,0] = AH2B_sf_vfd[:,0] 
#AH2B_sf_fp[:,1] = [vfd.convert_to_kW(sf_vfd_value, motor_ratings['AH2B']) \
#                   for sf_vfd_value in AH2B_sf_vfd[:,1]]
#
#sf_fp_2[:,1] = AH2A_sf_fp[:,1] + AH2B_sf_fp[:,1]
#sf_fp_2[:,0] = AH2A_sf_fp[:,0]
#sf_fp_2_cost[:,0] = AH2A_sf_fp[:,0]
#sf_fp_2_cost[:,1] = np.array([sf_fp_2[i,1] * elec_price[i,1] \
#                              for i in range(N)])

# 2. 2. Total panel fan power 
df['AH2_panel_fan_power'] = df['panel_power']
#df['AH2_panel_fan_power_cost'] = [power * elec \
#                                  for power in df['AH2_panel_fan_power'] \
#                                  for elec in df['electricity_price']] 
#sf_fp_tot = np.zeros((N,2)) 
#sf_fp_tot_cost = np.empty((N,2))
#pfp = np.array(power_cost['panel_power']['Readings'])
#
#sf_fp_tot[:,0] = pfp[:,0]
#sf_fp_tot[:,1] = pfp[:,1]
#sf_fp_tot_cost[:,0] = pfp[:,0]
#sf_fp_tot_cost[:,1] = np.array([sf_fp_tot[i,1] * elec_price[i,1] \
#                                for i in range(N)])

# 3. CHILLED WATER
# 3. 1. Measured Chilled water
df['measured_chilled_water_AH2'] = df['measured_chw']

#m_chwp = np.zeros((N,2)) 
#m_chwp_cost = np.empty((N,2))
#chwt = np.array(power_cost['measured_chw']['Readings'])
#
#m_chwp[:,0] = chwt[:,0]
#m_chwp[:,1] = chwt[:,1] * 3.517 / chw_eff
#m_chwp_cost[:,0] = chwt[:,0]
#m_chwp_cost[:,1] = np.array([m_chwp[i,1] * elec_price[i,1] \
#                             for i in range(N)])

# 3. 2. Calculate chilled water cost
cfm = pq.UnitQuantity('cfm', pq.foot**3/pq.minute, symbol='cfm')
df['chilled_water_AH2'] = np.nan
df['hot_water_AH2'] = np.nan 
df['total_zone_load_AH2'] = np.nan
df['total_VAV_box_airflow'] = np.nan 
#for rh_name in rh_coils:
#  df_zone['coil_closed_temp_change_' + rh_name] = np.nan
#  df_zone['hot_water_' +rh_name] = np.nan
#  df_zone['instantaneous_zone_load_' + rh_name] = np.nan

for i in range(9, df.shape[0]):
  chw_AH2 =  0.0 * pq.W
  all_sats = []*pq.F
  for coil_name in chw_coils:
    mat = np.array(df[str(coil_name) + '_MAT'][i-9:i+1]) * pq.F
    sat = np.array(df[str(coil_name) + '_SAT'][i-9:i+1]) * pq.F
    ccv = np.array(df[str(coil_name) + '_CCV'][i-9:i+1]) 
    flow = np.array(df[str(coil_name) + '_SF_CFM'][i-9:i+1]) * cfm
#    print coil_name
#    print i, i-9
    if mat[9] and sat[9] and flow[9] and ccv[9]:
      chw_power, temp_change = cp.coil_power(mat, sat, flow, ccv,
                                             chw_coils[coil_name])
      chw_coils[coil_name] = temp_change
      if chw_power < 0.0:
        chw_AH2 += chw_power
      if i % 108000 == 0:
        print "AHU %s - Row %s " %(coil_name,i)
        print "Chilled water power: ", -1.0 * float(chw_AH2)/1000.0 * pq.W
        print "Temperature change: ", chw_coils[coil_name]
      df['chilled_water_AH2'][i] = -1.0*float(chw_AH2)/1000
#    else: 
#      print "insufficient data, skipped row: ", i
#    all_sats = np.append(all_sats,sat)
#  sat_avg = np.mean(all_sats)
#  sat_z = np.repeat(sat_avg,10)*pq.F
#  rh_power_sum = 0.0 * pq.W
#  rh_flow_sum = 0.0 * cfm
#  zone_load_sum = 0.0* pq.W
#  missing = []
#  for rh_name in rh_coils:
##   print rh_name 
#   if rh_name + '_AI_3' in df_zone.columns and \
#       rh_name + '_AIR_VOLUME' in df_zone.columns and \
#       rh_name + '_ROOM_TEMP' in df_zone.columns and \
#       rh_name + '_VLV_POS' in df_zone.columns:
#      flow_z = np.array(df_zone[str(rh_name) + '_AIR_VOLUME'][i-9:i+1]) * cfm 
#      dat_z = np.array(df_zone[str(rh_name) + '_AI_3'][i-9:i+1]) * pq.F
#      rhv_z = np.array(df_zone[str(rh_name) + '_VLV_POS'][i-9:i+1])
#      rt_z = np.array(df_zone[str(rh_name) + '_ROOM_TEMP'][i-9:i+1]) * pq.F
#      rh_flow_sum += np.mean(flow) 
##      print rh_name
#      if flow_z[9] and dat_z[9] and rhv_z[9] and rt_z[9]:
#        rh_power, temp_change = cp.coil_power(sat_z, dat_z, flow_z, rhv_z,
#                                              rh_coils[rh_name])
##        print rh_power, temp_change
#        rh_coils[rh_name] = temp_change
#        zone_load = zl.zone_load(dat_z, rt_z, flow_z)  
#        if rh_power > 0.0:
#          rh_power_sum += rh_power
#        zone_load_sum += zone_load
##        print "details for ", rh_name
##        print "sat ", sat_z
##        print "dat ", dat_z
##        print "rhv ", rhv_z
##        print "rt  ", rt_z
##        print 'float:' + str(flow)
##        print 'rh_power: ' + str(rh_power)
##        print 'instantaneous zone load: ' + str(zone_load)
##        print 'temp_change: ' + str(temp_change) 
#        df_zone['coil_closed_temp_change_' + rh_name][i] = float(temp_change)
#        df_zone['hot_water_' + rh_name][i] = float(rh_power)/1000
#        df_zone['instantaneous_zone_load_' + rh_name][i] = float(zone_load)/1000
##      else: 
##        print "insufficient data, skipped row: ", i
#  df['hot_water_AH2'][i] = float(rh_power)/1000
#  df['total_zone_load_AH2'][i] = float(zone_load_sum)/1000
#  df['total_VAV_box_airflow'][i] = float(rh_flow_sum) 
#  if i % 10800 == 0: 
#    print "Data for row %s downloaded" %(i)
pdb.set_trace()

name_csv_final = 'energy_data_all_20140904.csv'
df.to_csv(name_csv_final)
pdb.set_trace()


### 3. 2. Calculate chilled water cost
#cfm = pq.UnitQuantity('cfm', pq.foot**3/pq.minute, symbol='cfm')
#all_sats = []*pq.F
#chw_AH2 =  0.0 * pq.W
#for coil_name in chw_coils:
#  mat = np.array(power_cost[coil_name + '_MAT']['Readings'])
#  sat = np.array(power_cost[coil_name + '_SAT']['Readings'])
#  ccv = np.array(power_cost[coil_name + '_CCV']['Readings'])
#  flow = np.array(power_cost[coil_name + '_SF_CFM']['Readings'])
#  # Extracting times of the readings
#  mat_t = mat[:,0]
#  sat_t = sat[:,0]
#  ccv_t = ccv[:,0]
#  flow_t = flow[:,0]
#  # Extracting values of the readings
#  mat_r = mat[:,1] * pq.F
#  sat_r = sat[:,1] * pq.F
#  ccv_r = ccv[:,1] 
#  flow_r = flow[:,1] * cfm
#  print coil_name
#  for i in range(9, N):
#    print i, i-9 
#    times = [mat_t[i], sat_t[i], ccv_t[i], flow_t[i]]
#    delta_times = (max(times) - min(times))/1000.0
#    if delta_times < 90.0:    
#      t = np.mean(times)
#      print t
#      chw_power, temp_change = cp.coil_power(mat_r[i-9:i+1],
#                                             sat_r[i-9:i+1],
#                                             flow_r[i-9:i+1],
#                                             ccv_r[i-9:i+1],
#                                             chw_coils[coil_name])
#      chw_coils[coil_name] = temp_change
#      if chw_power < 0.0:
#        chw_AH2 += chw_power
#        print "Chilled water power: ", -1.0 * float(chw_AH2)/1000.0 * pq.W
#        print "Temperature change: ", chw_coils[coil_name]
#    else:
#      print "Delta time for %s is > 90s" %(times)
#      pdb.set_trace()
#    power_cost['chilled_water_AH2']['Readings'][i,1].append(chw_AH2)
#    chw_AH2_cost = -1.0 * elec_cost * float(chw_A2H) / (1000 * chw_eff)
#    power_cost['chilled_water_AH2_cost']['Readings'][i,1].append(chw_AH2_cost)
#    pdb.set_trace()
#  all_sats = np.extend(all_sats,sat)
#sat_svg = np.mean(all_sats)

# fill dico
#for i in range(N):
#  power_cost['electricity_price']['Readings'] = [list(elec_price[i])]
#  power_cost['hot_water_price']['Readings'] = [list(hotw_price[i])]
#  power_cost['chw_plant_total_efficiency']['Readings'] = [list(chilledw_eff[i])]
#  power_cost['AH2A_supply_fan_power']['Readings'] = [list(AH2A_sf_fp[i])]
#  power_cost['AH2B_supply_fan_power']['Readings'] = [list(AH2B_sf_fp[i])]
#  power_cost['AH2_total_supply_fan_power']['Readings'] = [list(sf_fp_2[i])]
#  power_cost['AH2_total_supply_fan_power_cost']['Readings'] = [list(sf_fp_2_cost[i])]
#  power_cost['AH2_panel_fan_power']['Readings'] = [list(sf_fp_tot[i])]
#  power_cost['AH2_panel_fan_power_cost']['Readings'] = [list(sf_fp_tot_cost[i])]
#  power_cost['measured_chilled_water']['Readings'] = [list(m_chwp[i])]
#  power_cost['measured_chilled_water_cost']['Readings'] = [list(m_chwp_cost[i])]
#
#  sub_power_cost = {k: power_cost[k] for k in ['electricity_price',
#                                               'hot_water_price',
#                                               'chw_plant_total_efficiency',
#                                               'AH2A_supply_fan_power',
#                                               'AH2B_supply_fan_power',
#                                               'AH2_total_supply_fan_power',
#                                               'AH2_total_supply_fan_power_cost',
#                                               'AH2_panel_fan_power',
#                                               'AH2_panel_fan_power_cost',
#                                               'measured_chilled_water',
#                                               'measured_chilled_water_cost']}
#                                                                                   
##  sub_power_cost = {k: power_cost[k] for k in ["chw_plant_total_efficiency"]}
#  with open(j_name, 'w') as outfile:
#    json.dump(sub_power_cost, outfile)
#
#  # Updload selected data to smap
#  key_curl = 'XuETaff882hB6li0dP3XWdiGYJ9SSsFGj0N8'
#  url = 'http://new.openbms.org/backend/add/' + key_curl 
#  h = 'Content-Type: application/json' 
#  d = j_name
#  vcl = 'curl -XPOST -d @%s -H "%s" %s > test.out' % (d,h,url) 
#  retval = subprocess.call(vcl, shell=True)
##  pdb.set_trace()
##  url = 'http://new.openbms.org/backend/api/query' 
##  print url
##  payload={'key1': str(key_curl)}
##  r = requests.post(url,data=json.dumps(sub_power_cost), params=payload)
##  print "r.url", r.url
##  print "r.content", r.content
#  if i % 86400 == 0: print "\nPublished data %s" %(i) 
#  pdb.set_trace()
#
#pdb.set_trace()
##### Write json file to loaid smap
#j_power_cost=open('json_files/' + j_name ,'w')
#json.dump(sub_power_cost, j_power_cost, sort_keys=True)
#j_power_cost.close()
#pdb.set_trace()



