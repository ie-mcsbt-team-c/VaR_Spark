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


#remoteFile = url.urlopen('https://www.quandl.com/api/v3/datasets/OPEC/ORB/data.csv?start_date=2010-02-21&end_date=2019-02-21&api_key=ujUGW2cbgsDmD7sP359j')
#html = remoteFile.read().decode('ascii').splitlines()
#print(html)

#FACTORS : Try to do a for loop for getting all the stock 
#S&P 500 



start_date     = '2008-01-01'
end_date       = '2019-01-31'
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
    
    b.rename(columns={'timestamp':'Date'}, inplace=True)

for link2 in urlsaNDAQ: 
    
    remoteFile = url.urlopen(link2 + api_key_alpha + '&datatype=csv')
    print(remoteFile)
    html = remoteFile.read().decode('ascii').splitlines()
    c = pd.DataFrame(data=html)
    c = c[0].str.split(",", expand = True)
    c.columns = c.iloc[0]
    c = c[1:] # c = NASDAQ Dataframe
    
    c.rename(columns={'timestamp':'Date'}, inplace=True)
    
for link3 in urlsqOPEC: 
    remoteFile = url.urlopen(link3 + api_key_quandl)
    print(remoteFile)
    html1 = remoteFile.read().decode('ascii').splitlines()
    d = pd.DataFrame(data=html1)
    d = d[0].str.split(",", expand = True)
    d.columns = d.iloc[0]
    d = d[1:] # d = OPEC Dataframe


for link4 in urlsqUSTREASURY: 
    remoteFile = url.urlopen(link4 + api_key_quandl)
    print(remoteFile)
    html1 = remoteFile.read().decode('ascii').splitlines()
    e = pd.DataFrame(data=html1)
    e = e[0].str.split(",", expand = True)
    e.columns = e.iloc[0]
    e = e[1:] # e = US Treasury Dataframe
    

    


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


#df2 = pd.DataFrame({'Date':df1})

e['Date']= pd.to_datetime(e['Date'])
b['Date']= pd.to_datetime(b['Date'])
c['Date']= pd.to_datetime(c['Date'])
d['Date']= pd.to_datetime(d['Date'])


df2 = pd.merge(df1, e, how = 'inner', on=['Date'])
df2 = pd.merge(df2, b, how = 'inner', on=['Date'])
df2 = pd.merge(df1, c, how = 'inner', on=['Date'])
df2 = pd.merge(df1, d, how = 'inner', on=['Date'])



#df3 = pd.merge(df, d)

#start_date     = '2008-01-01'
#end_date       = '2019-01-31'
#
#mask = (SRCE['timestamp'] >= start_date) & (SRCE['timestamp'] <= end_date)
#SRCE_df = SRCE.loc[mask]
 
#%%   
#GET THE DATA 

#USE MODEL OF THE RETURN BASED ON CERTAIN FACTORS (S&P, NASDAQ)

#SAMPLE THE RETURNS OF EACH INSTRUMENTS 

#SAMPLING THE POSSIBLE VALUE OF THE OUTCOMES 

#CALCULATE VaR of the PORTEFEUILLE



