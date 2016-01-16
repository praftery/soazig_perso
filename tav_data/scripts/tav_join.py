"""
@author Soazig Kaam <soazig.kaam@berkeley.edu>

"""
from datetime import timedelta, date

import sys, os, pdb
import numpy as np
import pandas as pd
import csv
import time
import pprint as pp
import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), "../functions/"))
import tav_graphs_functions


def join_data(floors, startF1, endF1, startF2, endF2):
  for f in floors:
    floor_path1 = '../csv_output/Floor%s/'%(startF1.year) + 'Floor%s_airflow' %(str(f).zfill(2))  \
        + '%s-%s'%(startF1.strftime("%Y%m%d"), endF1.strftime("%Y%m%d"))\
        + '.csv' 
    floor_path2 = '../csv_output/Floor%s/'%(startF2.year) + 'Floor%s_airflow' %(str(f).zfill(2))  \
        + '%s-%s'%(startF2.strftime("%Y%m%d"), endF2.strftime("%Y%m%d"))\
        + '.csv' 
    floor_path12 = '../csv_output/Floor%s/'%(startF2.year) + 'Floor%s_airflow' %(str(f).zfill(2))  \
        + '%s-%s'%(startF1.strftime("%Y%m%d"), endF2.strftime("%Y%m%d"))\
        + '.csv' 
    df1 = pd.read_csv(floor_path1, index_col=0)
    df2 = pd.read_csv(floor_path2, index_col=0)
    result = df1.append(df2, ignore_index=True) 
    #pdb.set_trace()
    result.to_csv(floor_path12)
    floor_ratio_path1 = '../csv_output/Floor%s/'%(startF1.year) + 'Floor%s_ratios' %(str(f).zfill(2))  \
        + '%s-%s'%(startF1.strftime("%Y%m%d"), endF1.strftime("%Y%m%d"))\
        + '.csv' 
    floor_ratio_path2 = '../csv_output/Floor%s/'%(startF2.year) + 'Floor%s_ratios' %(str(f).zfill(2))  \
        + '%s-%s'%(startF2.strftime("%Y%m%d"), endF2.strftime("%Y%m%d"))\
        + '.csv' 
    floor_ratio_path12 = '../csv_output/Floor%s/'%(startF2.year) + 'Floor%s_ratios' %(str(f).zfill(2))  \
        + '%s-%s'%(startF1.strftime("%Y%m%d"), endF2.strftime("%Y%m%d"))\
        + '.csv' 
    df1_r = pd.read_csv(floor_ratio_path1, index_col=0)
    df2_r = pd.read_csv(floor_ratio_path2, index_col=0)
    result_r = df1_r.append(df2_r, ignore_index=True) 
    #pdb.set_trace()
    result_r.to_csv(floor_ratio_path12)

def airflow_ratios(floors, startF, endF):
  for f in floors:
    floor_path = '../csv_output/Floor%s/'%(startF.year) + 'Floor%s_airflow' %(str(f).zfill(2))  \
        + '%s-%s'%(startF.strftime("%Y%m%d"), endF.strftime("%Y%m%d"))\
        + '.csv' 
    df = pd.read_csv(floor_path, index_col=0, parse_dates=True)
    floor_ratio_path = '../csv_output/Floor%s/'%(startF.year) + 'Floor%s_ratios' %(str(f).zfill(2))  \
        + '%s-%s'%(startF.strftime("%Y%m%d"), endF.strftime("%Y%m%d"))\
        + '.csv' 
    points = ['AIR_VOLUME', 'CTL_FLOW_MIN', 'CTL_FLOW_MAX']
    df_r = tav_graphs_functions.data_ratios(df, points) 
    df_r.replace(np.inf,1)
    df_r.to_csv(floor_ratio_path)  

def df_replace(csv_path, rep_in, rep_out):
    df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
#    df.replace(rep_in,rep_out)
    df=df.where(df==rep_in, rep_out)
    df.to_csv(csv_path)  

startF1 = date(2015, 10, 15)
endF1 = date(2015, 11, 9)
startF2 = date(2015, 11, 9)
endF2 = date(2015, 12, 26)
startF = startF1
endF = endF2
floors = [str(f) for f in range(1,2)]
#join_data(floors, startF1, endF1, startF2, endF2)
#airflow_ratios(floors, startF, endF)
for f in floors:
  csv_path = '../csv_output/Floor%s/'%(startF.year) + 'Floor%s_ratios' %(str(f).zfill(2))  \
        + '%s-%s'%(startF.strftime("%Y%m%d"), endF.strftime("%Y%m%d"))\
        + '.csv' 
  df_replace(csv_path, np.inf, 1)
pdb.set_trace()

