#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 14:56:14 2019

@author: arthurmaroquenefroissart
"""
#%%

#ANALYSIS OF VaR using Spark/PYTHON

import urllib.request as url
import pandas as pd 
import numpy as np
import seaborn as sbn
import matplotlib.pyplot as plt


#remoteFile = url.urlopen('https://www.quandl.com/api/v3/datasets/OPEC/ORB/data.csv?start_date=2010-02-21&end_date=2019-02-21&api_key=ujUGW2cbgsDmD7sP359j')
#html = remoteFile.read().decode('ascii').splitlines()
#print(html)

#FACTORS : Try to do a for loop for getting all the stock 
#S&P 500 



start_date     = '2010-02-21'
end_date       = '2019-02-02'
api_key_alpha  = 'SQ60DKQWSFAU53XH'
api_key_quandl = 'ujUGW2cbgsDmD7sP359j'


urlsaGSP = ['https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=^GSPC&outputsize=full&apikey=']
urlsaNDAQ = ['https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=NDAQ&outputsize=full&apikey=']
urlsqOPEC = ['https://www.quandl.com/api/v3/datasets/OPEC/ORB/data.csv?start_date=' + start_date + '&end_date=' + end_date + '&api_key=']
urlsqUSTREASURY = ['https://www.quandl.com/api/v3/datasets/USTREASURY/YIELD.csv?start_date=' + start_date + '&end_date=' + end_date + '&api_key=']


for link in urlsaGSP: 
    
    remoteFile = url.urlopen(link + api_key_alpha + '&datatype=csv')
    print(remoteFile)
    html = remoteFile.read().decode('ascii').splitlines()
    b = pd.DataFrame(data=html)
    b = b[0].str.split(",", expand = True)
    b.columns = b.iloc[0]
    b = b[1:] # b = GSP Dataframe
    
    # rename columns to identify later when merging al factors into one data frame to check for null values
    b.rename(columns={'timestamp':'Date', 'open':'bopen', 'high':'bhigh', 'low':'blow', 'close':'bclose', 'volume':'bvolume'}, inplace=True)

for link2 in urlsaNDAQ: 
    
    remoteFile = url.urlopen(link2 + api_key_alpha + '&datatype=csv')
    print(remoteFile)
    html = remoteFile.read().decode('ascii').splitlines()
    c = pd.DataFrame(data=html)
    c = c[0].str.split(",", expand = True)
    c.columns = c.iloc[0]
    c = c[1:] # c = NASDAQ Dataframe
    
    # rename columns to identify later when merging al factors into one data frame to check for null values
    c.rename(columns={'timestamp':'Date', 'open':'copen', 'high':'chigh', 'low':'clow', 'close':'cclose', 'volume':'cvolume'}, inplace=True)
    
for link3 in urlsqOPEC: 
    remoteFile = url.urlopen(link3 + api_key_quandl)
    print(remoteFile)
    html1 = remoteFile.read().decode('ascii').splitlines()
    d = pd.DataFrame(data=html1)
    d = d[0].str.split(",", expand = True)
    d.columns = d.iloc[0]
    d = d[1:] # d = OPEC Dataframe
    
    d.rename(columns={'open':'dopen', 'high':'dhigh', 'low':'dlow', 'close':'dclose', 'volume':'dvolume'}, inplace=True)


for link4 in urlsqUSTREASURY: 
    remoteFile = url.urlopen(link4 + api_key_quandl)
    print(remoteFile)
    html1 = remoteFile.read().decode('ascii').splitlines()
    e = pd.DataFrame(data=html1)
    e = e[0].str.split(",", expand = True)
    e.columns = e.iloc[0]
    e = e[1:] # e = US Treasury Dataframe
    
    e.rename(columns={'open':'eopen', 'high':'ehigh', 'low':'elow', 'close':'eclose', 'volume':'evolume'}, inplace=True)
    


#%% DETECTING NULL VALUES  

# df.info() command tells us the total number of non null observations including the total number of entries. 
# Once number of entries isn’t equal to number of non null observations, we can suspect missing values.
    

# No null values in factor dataframes 
# Total number of entries = non-null observations = 4826 entries

b.info()
c.info()
d.info()
e.info()


b.isnull().sum()
c.isnull().sum()
d.isnull().sum()
e.isnull().sum()


# we are comparing the 'timestamp' column of the dataframes with each other, 
# to make sure they match across all df we pulled. False indicates matching values (ne = not equal) 
#
#datecheck = c.timestamp.ne(d.timestamp, e.timestamp)
#datecheck.unique() #output = array([False])

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
#GET THE DATA 

#USE MODEL OF THE RETURN BASED ON CERTAIN FACTORS (S&P, NASDAQ)

#SAMPLE THE RETURNS OF EACH INSTRUMENTS 

#SAMPLING THE POSSIBLE VALUE OF THE OUTCOMES 

#CALCULATE VaR of the PORTEFEUILLE



