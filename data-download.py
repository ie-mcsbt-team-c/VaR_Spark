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

#response = requests.get("https://raw.githubusercontent.com/ie-mcsbt-team-c/VaR_Spark/master/Symbols.txt")
#symbols = response.text
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

G1=["AVHI","BIRT","DAEG","EPAX"]
G2=["SHLM","YPRO","HAWK","HOTR"]

factors_a =  ["^GSPC","NDAQ"]
factors_q =  ["OPEC/ORB/data.csv","USTREASURY/YIELD.csv"]

start_date     = '2010-02-21'
end_date       = '2019-02-02'
api_key_alpha  = '6CY7P6QD71BDNVQX'
api_key_quandl = 'ujUGW2cbgsDmD7sP359j'


# d is dictionary of dataframes for instruments
d = {}
# f is a dictionary of dataframes for factors
f = {}
    
for factor in factors_a:
      
    f[factor] = pd.DataFrame()
        
    link = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol="
    remoteFile = url.urlopen(link + factor + "&outputsize=full&apikey=" + api_key_alpha + "&datatype=csv")
    print(remoteFile)
    html = remoteFile.read().decode('ascii').splitlines()
    
    key = pd.DataFrame(data=html)
    
    key = key[0].str.split(",", expand = True)
    key.columns = key.iloc[0]
    f[factor] = key[1:]  
    
#OPEC
 
f["OPEC"] = pd.DataFrame()
        
link = "https://www.quandl.com/api/v3/datasets/OPEC/ORB/data.csv?start_date="
remoteFile = url.urlopen(link + start_date + '&end_date=' + end_date + '&api_key=' + api_key_quandl)
print(remoteFile)
html = remoteFile.read().decode('ascii').splitlines()

key = pd.DataFrame(data=html)

key = key[0].str.split(",", expand = True)
key.columns = key.iloc[0]
f["OPEC"] = key[1:]  
    
#TREASURY
 
f["TREASURY"] = pd.DataFrame()
    
link = "https://www.quandl.com/api/v3/datasets/USTREASURY/YIELD.csv?start_date="
remoteFile = url.urlopen(link + start_date + '&end_date=' + end_date + '&api_key=' + api_key_quandl)
print(remoteFile)
html = remoteFile.read().decode('ascii').splitlines()

key = pd.DataFrame(data=html)

key = key[0].str.split(",", expand = True)
key.columns = key.iloc[0]
f["TREASURY"] = key[1:]  

#change the name of the group to group+1 to download the next 5 instruments 
#(allow 1 min btw each, download limited to 5 per min)
    
for ins in G1:
      
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
    
    export_csv = d[ins].to_csv(r"C:\Users\Leila\Desktop\VaR_Spark\instruments\{}.csv".format(ins), index = None, header=True) #Don't forget to add '.csv' at the end of the path

for factor in f:    
    
    export_csv = f[factor].to_csv(r"C:\Users\Leila\Desktop\VaR_Spark\factors\{}.csv".format(factor), index = None, header=True)


#%%
#GET THE DATA 

#USE MODEL OF THE RETURN BASED ON CERTAIN FACTORS (S&P, NASDAQ)

#SAMPLE THE RETURNS OF EACH INSTRUMENTS 

#SAMPLING THE POSSIBLE VALUE OF THE OUTCOMES 

#CALCULATE VaR of the PORTFOLIO



