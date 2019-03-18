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
import pandas as pd 
import statsmodels.api as sm
#%% Read Factors Returns 



factors_returns = pd.read_csv("https://raw.githubusercontent.com/ie-mcsbt-team-c/VaR_Spark/master/factors_returns.csv")
instruments_returns = pd.read_csv("https://raw.githubusercontent.com/ie-mcsbt-team-c/VaR_Spark/master/instruments_returns.csv")
factors_returns = pd.DataFrame(factors_returns)
factors_returns=factors_returns.drop(["Date"], axis=1)
print(factors_returns)
#%%

#%%

def featurize(factors_returns):
       original = factors_returns
       sign = np.sign(factors_returns)
       power = np.power(factors_returns,2)
       root = np.power(abs(factors_returns),0.5)
       result = sign*(original*power+root)
       
       return result

factors_featurize_returns = pd.DataFrame(featurize(factors_returns))


#%%
def estimateParams(y, x):
    return sm.OLS(y, x).fit().params

factors_featurize_returns_intercept = sm.add_constant(factors_featurize_returns, prepend=True)

weights = [estimateParams(stockReturns, factor_columns) for stockReturns in stocksReturns] 

