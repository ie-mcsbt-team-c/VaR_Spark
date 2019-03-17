# -*- coding: utf-8 -*-
"""
Created on Sun Mar 17 13:12:07 2019

@author: Leila
"""

#%%

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)
    
mean = spark.read
        .format("csv")
        .option("header", "false") 
        .load("C:\Users\Leila\Desktop\VaR_Spark\mean.csv")
        
print(mean)

spark.stop()