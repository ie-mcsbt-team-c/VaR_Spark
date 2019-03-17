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
    
df = spark.read.format("csv").option("header", "true").load("mean.csv")
        
print(df)

spark.stop()