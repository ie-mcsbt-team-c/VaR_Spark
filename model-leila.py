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
import requests
import numpy as np

#remoteFile = url.urlopen('https://www.quandl.com/api/v3/datasets/OPEC/ORB/data.csv?start_date=2010-02-21&end_date=2019-02-21&api_key=ujUGW2cbgsDmD7sP359j')
#html = remoteFile.read().decode('ascii').splitlines()
#print(html)

#FACTORS : Try to do a for loop for getting all the stock 
#S&P 500 

response = requests.get("https://raw.githubusercontent.com/ie-mcsbt-team-c/VaR_Spark/master/Symbols.txt")
symbols = response.text
#instruments = symbols.split()

group1 = ["PIH", "FLWS", "FCTY", "FCCY", "SRCE"]
group2 = ["FUBC","VNET","TWOU","DGLD","JOBS"]
group3 = ["EGHT","AVHI","SHLM","AAON","ASTM"]
group4 = ["ABAX","XLRN","ACTA","BIRT","MULT"]
group5 = ["YPRO","AEGR","MDRX","EPAX","DOX"]
group6 = ["UHAL","MTGE","CRMT","FOLD","BCOM"]
group7 = ["BOSC","HAWK","CFFI","CHRW","KOOL"]
group8 = ["HOTR","PLCE","JRJC","CHOP","HGSH"]
group9 = ["HTHT","IMOS","DAEG","DJCO","SATS"]
group10 = ["WATT","INBK","FTLB","QABA","GOOG"]

factors_a =  ["^GSPC","NSDQ"]
#factors_q =  

start_date     = '2010-02-21'
end_date       = '2019-02-02'
api_key_alpha  = 'SQ60DKQWSFAU53XH'
api_key_quandl = 'ujUGW2cbgsDmD7sP359j'

urlsa = [ 
        'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=^GSPC&outputsize=full&apikey=', 
        'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=NDAQ&outputsize=full&apikey=',
        ]

urlsq = ['https://www.quandl.com/api/v3/datasets/OPEC/ORB/data.csv?start_date=' + start_date + '&end_date=' + end_date + '&api_key=' + api_key_quandl, 
         'https://www.quandl.com/api/v3/datasets/USTREASURY/YIELD.csv?start_date=' + start_date + '&end_date=' + end_date + '&api_key=' + api_key_quandl]


#for link in urlsa: 
#    
#    remoteFile = url.urlopen(link + api_key_alpha + '&datatype=csv')
#    print(remoteFile)
#    html = remoteFile.read().decode('ascii').splitlines()
#    b = pd.DataFrame(data=html)
#    b = b[0].str.split(",", expand = True)
#    b.columns = b.iloc[0]
#    b = b[1:]
#    
#    
#for links2 in urlsq: 
#    remoteFile = url.urlopen(link + api_key_quandl + '&datatype=csv' )
#    print(remoteFile)
#    html1 = remoteFile.read().decode('ascii').splitlines()
#    c = pd.DataFrame(data=html1)
#    c = c[0].str.split(",", expand = True)
#    c.columns = c.iloc[0]
#    c = c[1:]


#d is dictionary of dataframes
d = {}

#change the name of the group to group+1 to download the next 5 instruments 
#(allow 1 min btw each, download limited to 5 per min)

for ins in group10:
      
    d[ins] = pd.DataFrame()
        
    link = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol="
    remoteFile = url.urlopen(link + ins + "&outputsize=full&apikey=" + api_key_alpha + "&datatype=csv")
    print(remoteFile)
    html = remoteFile.read().decode('ascii').splitlines()
    
    key = pd.DataFrame(data=html)
    
    key = key[0].str.split(",", expand = True)
    key.columns = key.iloc[0]
    d[ins] = key[1:]  
    
for ins in d:    
    
    export_csv = d[ins].to_csv(r"C:\Users\Leila\Desktop\instruments\{}.csv".format(ins), index = None, header=True) #Don't forget to add '.csv' at the end of the path


#%%Example with SRCE
    
import pandas as pd 

SRCE = pd.read_csv("https://raw.githubusercontent.com/ie-mcsbt-team-c/VaR_Spark/master/instruments/SRCE.csv")

#we apply a mask to limit the dates to our imposed timeframes

start_date     = '2008-01-01'
end_date       = '2019-01-31'

mask = (SRCE['timestamp'] >= start_date) & (SRCE['timestamp'] <= end_date)
SRCE_df = SRCE.loc[mask]

#%%
#GET THE DATA 

#USE MODEL OF THE RETURN BASED ON CERTAIN FACTORS (S&P, NASDAQ)

#SAMPLE THE RETURNS OF EACH INSTRUMENTS 

#SAMPLING THE POSSIBLE VALUE OF THE OUTCOMES 

#CALCULATE VaR of the PORTFOLIO



