# -*- coding: utf-8 -*-
import dataiku
from dataiku import spark as dkuspark
from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

# Read recipe inputs
rows = dataiku.Dataset("rows")
rows_df = dkuspark.get_dataframe(sqlContext, rows)

# Compute recipe outputs from inputs
# TODO: Replace this part by your actual code that computes the output, as a SparkSQL dataframe
pyspark_output_df = rows_df # For this sample code, simply copy input to output

# Write recipe outputs
pyspark_output = dataiku.Dataset("pyspark-output")
dkuspark.write_with_schema(pyspark_output, pyspark_output_df)
