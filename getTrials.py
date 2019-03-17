# -*- coding: utf-8 -*-
"""
Created on Sun Mar 17 13:12:07 2019

@author: Leila
"""

#%%

import numpy as np
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("basic") \
    .getOrCreate()

mean = spark.read.csv("mean.csv",header=False,sep="\n");
cov = spark.read.csv("cov.csv",header=False,sep="\n");
coefs = spark.read.csv("coefs.csv",header=False,sep="\n");
inter = spark.read.csv("inter.csv",header=False,sep="\n");

def generate_trial(mean,cov):
    
    #creating an empty list to store the returns
    
#    trialReturns = []
    
    #creating a loop to go over t number of trials

#    for i in range(0, t): 
         
    #initializing the total portfolio return
            
#            trial_portfolioReturn = 0
            
    #generating one sample of 4 factors
            
    trial_factorReturns = np.random.multivariate_normal(mean, cov)
    
    print(trial_factorReturns)
    
generate_trial(mean,cov)

spark.stop()


            

