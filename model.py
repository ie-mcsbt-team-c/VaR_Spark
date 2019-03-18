#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 14:56:14 2019

"""
#%%

import urllib.request as url
import pandas as pd 
import numpy as np
import seaborn as sbn
import matplotlib.pyplot as plt

#%% Import dataframes

instrumentsReturns = pd.read_csv("https://raw.githubusercontent.com/ie-mcsbt-team-c/VaR_Spark/master/instruments_returns.csv")
instrumentsReturns = pd.DataFrame(instrumentsReturns)

factors_with_dates =  pd.read_csv("https://raw.githubusercontent.com/ie-mcsbt-team-c/VaR_Spark/master/factors_returns.csv")
factors_with_dates = pd.DataFrame(factors_with_dates)

factorsReturns = pd.read_csv("https://raw.githubusercontent.com/ie-mcsbt-team-c/VaR_Spark/master/factors_returns.csv")
factorsReturns = pd.DataFrame(factorsReturns)

factorsReturns["Date"] = pd.to_datetime(factorsReturns["Date"])
factors_with_dates["Date"] = pd.to_datetime(factors_with_dates["Date"])
instrumentsReturns["Date"] = pd.to_datetime(instrumentsReturns["Date"])

 
#%% Visualize null-values for Factors

# Assess the number of null values

factorsReturns.isnull().sum()

# The heat map show correlated null-values across the data - meaning that all factors have null-values on the same dates. 
# It can be assumed that these dates are official hollidays, that apply to all factors.
sbn.heatmap(factorsReturns.isnull(), cbar=False)

#All columns have the same number of null values (103)
null_counts = factorsReturns.isnull().sum()/len(factorsReturns)
plt.figure(figsize=(16,8))
plt.xticks(np.arange(len(null_counts))+0.5,null_counts.index,rotation='vertical')
plt.ylabel('fraction of rows with missing data')
plt.bar(np.arange(len(null_counts)),null_counts)


# Df with all rows containing null values including the date (103 entries)
# We can see that each year has around 9-10 dates with null-values spread across the month.
# Therefore filling null values with the proceeding value can be an adequate method to deal with them. 
null_data = factorsReturns[factorsReturns.isnull().any(axis=1)]

#%% Filling null values for Factors

factorsReturns = factorsReturns.fillna(method='bfill')
factorsReturns = factorsReturns.fillna(method='ffill')

#careful null values fill should be done individually for each instrument and trim out the 
#inexsting dates, and before the returns are calculated..

#%%
#Featurization of the factors

#Creation of a matrix of factors
Dates = pd.DataFrame(factorsReturns["Date"])
F1 = np.array(factorsReturns["f_GSP"])
F2 = np.array(factorsReturns["f_NDAQ"])
F3 = np.array(factorsReturns["f_OPEC"])
F4 = np.array(factorsReturns["f_TREASURY"])
factorsReturns = list((F1,F2,F3,F4))

def transpose(matrix):
    return [[matrix[j][i] for j in range(len(matrix))] for i in range(len(matrix[0]))]

def get_features(factorReturns):
    factorReturns = list(factorReturns)
    squaredReturns = [np.sign(element)*(element)**2 for element in factorReturns]
    squareRootedReturns = [np.sign(element)*abs(element)**0.5 for element in factorReturns]
    # concat new features
    return squaredReturns + squareRootedReturns + factorReturns

# transpose factorsReturns
factorMat = transpose(factorsReturns)

# featurize each row of factorMat
factorFeatures = list(map(get_features,factorMat))

Dates = pd.DataFrame(factors_with_dates["Date"])
F = pd.DataFrame(factorFeatures)
F = pd.merge(F,Dates, left_index = True, right_index = True)

#%% Visualize null-values for Instruments

# Assess the number of null values

instrumentsReturns.isnull().sum()

# The heat map show correlated null-values across the data - meaning that all factors have null-values on the same dates. 
# It can be assumed that these dates are official hollidays, that apply to all factors.
sbn.heatmap(instrumentsReturns.isnull(), cbar=False)

# Number of null values accross instruments varies a lot. It would not be appropriate
# to fill based on the same strategy as the factors, therefore we will align the factors
# to run a model based on the available timeframe for each instrument
null_counts = instrumentsReturns.isnull().sum()/len(instrumentsReturns)
plt.figure(figsize=(16,8))
plt.xticks(np.arange(len(null_counts))+0.5,null_counts.index,rotation='vertical')
plt.ylabel('fraction of rows with missing data')
plt.bar(np.arange(len(null_counts)),null_counts)


#%% This align_dates function will be used to adapt the featuresReturns to 
# the available timeframe of each instrument

def align_dates(ins):
    
    A = instrumentsReturns[["Date",ins]]
    A = A.dropna()
    B = F
    
    X = pd.merge(A,B,on="Date")
    return X

# testing the function with one instrument
align_dates("SRCE")

    
#%%
#Training a linear regression model with our 4 features for each instrument and saving
# the coefficients and intercepts in lists to reuse later

from sklearn.linear_model import LinearRegression

def get_params(ins):
    
    X = align_dates(ins).drop(columns=["Date",ins],axis=0)
    y = align_dates(ins)[ins]
    
    lm= LinearRegression()

    lm.fit(X,y)
    coef = lm.coef_
    inter = lm.intercept_

    params = [coef,inter]
    return params


# testing the function with one instrument
get_params("SRCE")

#%%
# Generating a random multivariate sample
#
##We could sample each factor independently but, in that case, we will be ignoring the
##correlation among the four factors. 
##We will use a multivariate normal distribution. To get sample values we just we can just use 
## numpy like this:
##f1, f2, f3, f4 = numpy.random.multivariate_normal(mean, cov)

instruments = ["PIH", "FLWS", "FCTY", "FCCY", "SRCE",
               "FUBC","VNET","TWOU","DGLD","JOBS"
               ,"EGHT","AAON","ABAX","XLRN","ACTA"
               ,"MULT","AEGR","MDRX","DOX"
               ,"UHAL","MTGE","CRMT","FOLD","BCOM"
               ,"BOSC","CFFI","CHRW","KOOL"
               ,"PLCE","JRJC","CHOP","HGSH"
               ,"HTHT","IMOS","DJCO","SATS"
               ,"WATT","INBK","FTLB","QABA","GOOG"]

factorsReturns_list = list((F1,F2,F3,F4))

cov = np.cov(factorsReturns_list)
mean = list((F1.mean(axis=0),F2.mean(axis=0),F3.mean(axis=0),F4.mean(axis=0)))

sample = np.random.multivariate_normal(mean, cov)

print(cov)
print(mean)
print(sample)

#Approach in python to generate trials and store the returns in a list

def generate_trial(t,mean,cov):
    
    #creating an empty list to store the returns
    
    trialReturns = []
    
    #creating a loop to go over t number of trials

    for i in range(0, t):
    
    #initializing the total portfolio return
    
        trial_portfolioReturn = 0
        
    #generating one sample of 4 factors
    
        trial_factorReturns = np.random.multivariate_normal(mean, cov)
        
    #featurizing sampled factors into 12 features
    
        trial_featuresReturns = get_features(trial_factorReturns)  
        
    #creating a loop to generate return of 1 instrument
        
        for ins in instruments:       
            
    #run linear model for specific instrument and applying coefs + intercept on the 
    #sampled features to get sampled return
        
            trial_instrumentReturn = sum((get_params(ins)[0] * trial_featuresReturns) + get_params(ins)[1])
            
            trial_portfolioReturn += trial_instrumentReturn*(1/(len(instruments)))
        
        trialReturns.append(trial_portfolioReturn)
    
    return trialReturns

test = generate_trial(5,mean,cov)

def fivePercentVaR(trials):
    numTrials = len(trials)

    fivePercent = max(round(numTrials/20.0), 1)

    trials.sort()

    fivePercentWorst = trials[0:fivePercent]
    return fivePercentWorst[-1]

fivePercentVaR(test)

#%%

"""
Adapting the approach for Spark

"""

#For simplicity purposes we will rewrite the generate_trials function in order 
#to take the coefficients and intercepts from csv files instead of generating them
#inside of the function

#First we need to create csv files containing the coefficients and intercepts

coef = []
inter = []

for ins in instruments:
    
    coef.append(get_params(ins)[0])
    inter.append(get_params(ins)[1])

coefs = pd.DataFrame(coef)    
export_csv = coefs.to_csv(r"C:\Users\Leila\Desktop\VaR_Spark\coefs.csv", index = None, header=False)

inter = pd.DataFrame(inter)    
export_csv = inter.to_csv(r"C:\Users\Leila\Desktop\VaR_Spark\inter.csv", index = None, header=False)  

#We need to create csv with cov and mean 

cov = np.cov(factorsReturns_list)
cov = pd.DataFrame(cov)
export_csv = cov.to_csv(r"C:\Users\Leila\Desktop\VaR_Spark\cov.csv", index = None, header=False) 

mean = list((F1.mean(axis=0),F2.mean(axis=0),F3.mean(axis=0),F4.mean(axis=0)))
mean = pd.DataFrame(mean)
export_csv = mean.to_csv(r"C:\Users\Leila\Desktop\VaR_Spark\mean.csv", index = None, header=False)

#Now we need to modify our generate_trials function in order to use the params 
#from the csv files

def generate_trial(t,mean,cov,coefs,inter):
    
    #creating an empty list to store the returns
    
    trialReturns = []
    
    #creating a loop to go over t number of trials

    for i in range(0, t): 
         
    #initializing the total portfolio return
            
            trial_portfolioReturn = 0
            
    #generating one sample of 4 factors
            
            trial_factorReturns = np.random.multivariate_normal(mean, cov)
            
    #featurizing sampled factors into 12 features
    
            trial_featuresReturns = get_features(trial_factorReturns)
            
    #run linear model for specific instrument and applying coefs + intercept on the 
    #sampled features to get sampled return
            
            for k in range(len(coefs)):
            
                trial_instrumentReturn = sum([coefs[k][i] * trial_featuresReturns[i] for i in range(len(trial_featuresReturns))]) + inter[k]
             
    #it doesn´t makes sens to add them up though?
    
            trial_portfolioReturn += trial_instrumentReturn
        
            trialReturns.append(trial_portfolioReturn)
    
    return trialReturns

parallelism = 2
t = 10000
trial_indexes = list(range(0, parallelism))
seedRDD = sc.parallelize(trial_indexes, parallelism)

trials = seedRDD.flatMap(lambda idx: \
                generate_trial(
                    max(int(t/parallelism), 1), 
                    mean.value, cov.value,
                    coefs.value,inter.value
                ))
trials.cache()

valueAtRisk = fivePercentVaR(trials)

print ("Value at Risk(VaR) 5%:", valueAtRisk)


#Functions with Spark Libraries

def fivePercentVaR(trials):
    numTrials = trials.count()
    topLosses = trials.takeOrdered(max(round(numTrials/20.0), 1))
    return topLosses[-1]









    



