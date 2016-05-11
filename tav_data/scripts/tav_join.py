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


def row_join(file_full, files_add, file_complete):
  print "\n\n==============================Row combine started===================================\n"
  df_full = pd.read_csv(file_full, index_col=0)
  df_add = []
  df_add.append(df_full)
  for f in files_add:
    df_add.append(pd.read_csv(f, index_col=0))
  df_complete = pd.concat(df_add, join='outer', ignore_index=True)
  df_complete.to_csv(file_complete)

def column_join(file_full, files_add, file_complete):
  'files_add should be a list of files'
  print "\n\n==============================Column combine started===================================\n"
  df_full = pd.read_csv(file_full, index_col=0)
  df_add = []
  df_add.append(df_full)
  for f in files_add:
    df_add.append(pd.read_csv(f, index_col=0))
  df_complete = reduce(lambda x,y: \
               pd.merge(x, y, on=['timestamp','datetime']), df_add)
  df_complete.to_csv(file_complete)

file_full = '../csv_output2016/KOAK_weather20160401-20160430.csv' 
#file_full = '../csv_output2016/Energy_TAV20160401-20160501.csv'
file_add = ['../csv_output2016/KOAK_weather20160501-20160510.csv']
#file_add = ['../csv_output2016/Energy_TAV_missing20160401-20160501.csv']
#file_complete = '../csv_output2016/Energy_TAV20160401-20160430.csv'
file_complete = '../csv_output2016/KOAK_weather20160401-20160510.csv'

#column_join(file_full, file_add, file_complete)
row_join(file_full, file_add, file_complete)
pdb.set_trace()
