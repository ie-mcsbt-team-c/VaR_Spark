# -*- coding: utf-8 -*-
"""
Created on Sun Mar 17 13:12:07 2019

@author: Leila
"""

#%%

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.csv("mean.csv",header=True,sep="\n");

print(df.collect())