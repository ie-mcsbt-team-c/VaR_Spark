#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 15 11:32:44 2019

@author: yotroz
"""
#%%
import pandas as pd 
import urllib.request as url
import requests



response = requests.get("https://raw.githubusercontent.com/ie-mcsbt-team-c/VaR_Spark/master/Symbols.txt")
symbols = response.text
instruments = symbols.split()

#%%


instruments = ["AAON","ABAX","ACTA","AEGR","BCOM","BOSC","CFFI","CHOP","CHRW","CRMT","DGLD","DJCO","DOX","EGHT","FCCY","FCTY","FLWS","FOLD","FTLB","FUBC","GOOG","HGSH","HTHT","IMOS","INBK","JOBS","JRJC","KOOL","MDRX","MTGE","MULT","PIH","PLCE","QABA","SATS","SRCE","TWOU","UHAL","VNET","WATT","XLRN"]

d = {}

for ins in instruments: 

    df = pd.read_csv("https://raw.githubusercontent.com/ie-mcsbt-team-c/VaR_Spark/master/instruments/{}.csv". format(ins))
    
    d[ins] = pd.DataFrame(df)
    
dataframes = d

for df in dataframes: 
    
    i = 0
    pf = []
    
    for row in dataframes[df].itertuples(): 
        row = pd.DataFrame([row])
        if i < (len(dataframes[df]) - 5):
#            if dataf == (b) | (c): 
        
            x = dataframes[df].iloc[i]
            x1 = x['close']
            x2 = pd.to_numeric(x1)
            
            y = dataframes[df].iloc[i + 4]
            y1 = y['open']
            y2 = pd.to_numeric(y1)
            
            ret = ((x2 - y2) / y2).item()

            pf.append(ret)
            i = i + 1
        
    pf = pd.DataFrame(pf)
  
    dataframes[df]['return'] = pf
        
#%%    
        
for df in dataframes:

    dataframes[df] = dataframes[df][['timestamp', 'return']]
    dataframes[df].rename(columns={'timestamp':'Date'}, inplace=True)


 #%%
#Create instruments_returns
#dz = pd.DataFrame(columns=["Date"])
#
#dz['Date']= pd.to_datetime(dz['Date'])

dz = pd.bdate_range('2008-01-01', '2019-01-31')
dz = pd.DataFrame(dz,columns=['Date'])

dataframes["AAON"]['Date']= pd.to_datetime(dataframes["AAON"]['Date'])
dz = pd.merge(dz,dataframes["AAON"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["ABAX"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["ACTA"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["AEGR"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["BCOM"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["BOSC"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["CFFI"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["CHOP"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["CHRW"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["CRMT"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["DGLD"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["DJCO"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["DOX"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["EGHT"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["FCCY"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["FCTY"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["FLWS"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["FOLD"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["FTLB"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["FUBC"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["GOOG"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["HGSH"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["HTHT"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["IMOS"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["INBK"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["JOBS"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["JRJC"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["KOOL"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["MDRX"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["MTGE"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["MULT"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["PIH"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["PLCE"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["QABA"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["SATS"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["SRCE"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["TWOU"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["UHAL"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["VNET"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["WATT"], how = 'outer', on=['Date'])
dz = pd.merge(dz,dataframes["XLRN"], how = 'outer', on=['Date'])

instruments_returns = dz
export_csv = instruments_returns.to_csv(r"C:\Users\Leila\Desktop\VaR_Spark\instruments_returns.csv", index = None, header=True) #Don't forget to add '.csv' at the end of the path

#for df in dataframes :    
#    
#
#    dataframes[df]['Date']= pd.to_datetime(dataframes[df]['Date'])
#    dz = pd.merge(dz,dataframes[df], how = 'inner', on=['Date'])

