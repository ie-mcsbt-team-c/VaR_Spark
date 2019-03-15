#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 14:56:14 2019

@author: arthurmaroquenefroissart
"""
#%%

import urllib.request as url
import pandas as pd 
import numpy as np
import seaborn as sbn
import matplotlib.pyplot as plt

#%% Test for instrument with SRCE - Calculation of the returns 

import pandas as pd 

SRCE_df = pd.read_csv("https://raw.githubusercontent.com/ie-mcsbt-team-c/VaR_Spark/master/instruments/SRCE.csv")
SRCE_df = pd.DataFrame(SRCE_df)

dataframes = [SRCE_df]

for dataf in dataframes: 
    
    i = 1
    df = []
    for row in dataf.iterrows():
        if i < (len(SRCE_df) - 5):
            
            x = dataf.iloc[i]
            x1 = x['close']
            x2 = pd.to_numeric(x1)
               
            y = dataf.iloc[i + 5]
            y1 = y['open']
            y2 = pd.to_numeric(y1)
                          
            ret = ((x2 - y2) / y2) 
            
            df.append(ret)
            
            i = i + 1
        
    df = pd.DataFrame(df)
        
#    print(dataf)
SRCE_df['return'] = df
SRCE_df = SRCE_df[['timestamp','return']]
        
#%%

# Create DateTimeIndex with all business days within the time period
# Transfor to df column (2893 entries)

df1 = pd.bdate_range('2008-01-01', '2019-01-31')
df1 = pd.DataFrame(df1,columns=['Date'])


# df1 has 2893 entries and with merging the files only 2790 entries are left - so rows are thrown out and I couldn't fix it yet 
b['Date']= pd.to_datetime(b['Date'])
df2 = pd.merge(df1, b, on=['Date'], how='left')

c['Date']= pd.to_datetime(c['Date'])
df3 = pd.merge(df2, c, on=['Date'], how='left')

d['Date']= pd.to_datetime(d['Date'])
df4 = pd.merge(df3, c, on=['Date'], how='left')

e['Date']= pd.to_datetime(e['Date'])
df5 = pd.merge(df4, c, on=['Date'], how='left')

 
#%% Visualize null-values

df5.isnull().sum()

# The heat map show correlated null-values across the data - meaning that all factors have null-values on the same dates. 
# It can be assumed that these dates are official hollidays, that apply to all factors.
sbn.heatmap(df5.isnull(), cbar=False)

#All columns have the same number of null values (103)
null_counts = df5.isnull().sum()/len(df5)
plt.figure(figsize=(16,8))
plt.xticks(np.arange(len(null_counts))+0.5,null_counts.index,rotation='vertical')
plt.ylabel('fraction of rows with missing data')
plt.bar(np.arange(len(null_counts)),null_counts)


# Df with all rows containing null values including the date (103 entries)
# We can see that each year has around 9-10 dates with null-values spread across the month.
# Therefore filling null values with the proceeding value can be an adequate method to deal with them. 
null_data = df2[df2.isnull().any(axis=1)]

#%% Filling null values 

df5 = df5.fillna(method='bfill')

#%%   





