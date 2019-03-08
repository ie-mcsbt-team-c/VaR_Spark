#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 14:56:14 2019

@author: arthurmaroquenefroissart
"""
#%%

#ANALYSIS OF VaR using Spark/PYTHON

import quandl
import urllib.request as url
import pandas as pd 

#remoteFile = url.urlopen('https://www.quandl.com/api/v3/datasets/OPEC/ORB/data.csv?start_date=2010-02-21&end_date=2019-02-21&api_key=ujUGW2cbgsDmD7sP359j')
#html = remoteFile.read().decode('ascii').splitlines()
#print(html)

#FACTORS : Try to do a for loop for getting all the stock 
#S&P 500 



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


for link in urlsa: 
    
    remoteFile = url.urlopen(link + api_key_alpha + '&datatype=csv')
    print(remoteFile)
    html = remoteFile.read().decode('ascii').splitlines()
    b = pd.DataFrame(data=html)
    b = b[0].str.split(",", expand = True)
    b.columns = b.iloc[0]
    b = b[1:]
    
    
for links2 in urlsq: 
    remoteFile = url.urlopen(link + api_key_quandl + '&datatype=csv' )
    print(remoteFile)
    html1 = remoteFile.read().decode('ascii').splitlines()
    c = pd.DataFrame(data=html1)
    c = c[0].str.split(",", expand = True)
    c.columns = c.iloc[0]
    c = c[1:]
    


#%%
#GET THE DATA 

#USE MODEL OF THE RETURN BASED ON CERTAIN FACTORS (S&P, NASDAQ)

#SAMPLE THE RETURNS OF EACH INSTRUMENTS 

#SAMPLING THE POSSIBLE VALUE OF THE OUTCOMES 

#CALCULATE VaR of the PORTEFEUILLE



