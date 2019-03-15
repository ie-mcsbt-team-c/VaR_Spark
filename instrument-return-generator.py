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


print(instruments)

d_instruments = {}

for instrument in instruments: 
    print(instrument)
    df = pd.read_csv("https://raw.githubusercontent.com/ie-mcsbt-team-c/VaR_Spark/master/instruments/{}.csv". format(instrument))
    d_instruments[instrument] = pd.DataFrame(df)
    


dataframes = d_instruments

for dataf in dataframes: 
    
    i = 0
    prodf = []
    for row in dataframes[dataf].itertuples(): 
        row = pd.DataFrame([row])
        if i < (len(dataframes[dataf]) - 5):
#            if dataf == (b) | (c): 
        
            x = dataframes[dataf].iloc[i]
            x1 = x['close']
            x2 = pd.to_numeric(x1)
            print(x2)
            
            y = dataframes[dataf].iloc[i + 4]
            y1 = y['open']
            y2 = pd.to_numeric(y1)
            print(y2)
                
                
            ret = ((x2 - y2) / y2).item()
            print(ret)
            prodf.append(ret)
            i = i + 1
        
    prodf = pd.DataFrame(prodf)
    print(prodf)  
    dataframes[dataf]['return'] = prodf
    
    print(dataf)

        
#%%    
        
for dataf in dataframes: 
    for col in dataframes[dataf]: 
        dataframes[dataf] = dataframes[dataf][['timestamp', 'return']]
        dataframes[dataf].rename(columns={'timestamp':'Date'}, inplace=True)



        
        

 #%%

