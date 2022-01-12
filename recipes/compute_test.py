# -*- coding: utf-8 -*-
import dataiku
from dataiku import spark as dkuspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
import time

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

# Read recipe inputs
revenue_prediction_co_prepared = dataiku.Dataset("revenue_prediction_co_prepared")
revenue_prediction_co_prepared_df = dkuspark.get_dataframe(sqlContext, revenue_prediction_co_prepared)

time.sleep(60)
# Compute recipe outputs from inputs
# TODO: Replace this part by your actual code that computes the output, as a SparkSQL dataframe
test_df = revenue_prediction_co_prepared_df # For this sample code, simply copy input to output

# Write recipe outputs
test = dataiku.Dataset("test")
dkuspark.write_with_schema(test, test_df)
