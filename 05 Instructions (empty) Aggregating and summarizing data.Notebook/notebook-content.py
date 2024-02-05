# Synapse Analytics notebook source

# METADATA ********************

# META {
# META   "synapse": {
# META     "lakehouse": {
# META       "default_lakehouse": "c95d7b56-9a7f-4b7c-baf4-ea0bdaacbbf7",
# META       "default_lakehouse_name": "PySparkLakehouse",
# META       "default_lakehouse_workspace_id": "e8b3335a-5e83-466c-bd0d-748c45da7cc9",
# META       "known_lakehouses": [
# META         {
# META           "id": "c95d7b56-9a7f-4b7c-baf4-ea0bdaacbbf7"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Aggregating and summarizing data

# CELL ********************

from pyspark.sql import SparkSession
import pandas as pd
import plotly.express as px

spark = SparkSession.builder.appName('05_Aggregating_and_summarizing_data').getOrCreate()


# CELL ********************


# MARKDOWN ********************

# You can do a simple count of values:

# CELL ********************


# MARKDOWN ********************

#  If you want percentages you need to calculate the entire count first:

# CELL ********************


# MARKDOWN ********************

#  But one of the most powerful features of pySpark is the method `.groupby()`
#  This makes it possible to divide your data in groups and summarize it however you like.
#  With built-in functions such as .mean(), but you also create your own custom functions

# MARKDOWN ********************

#  Let's first group data by title type

# CELL ********************


# MARKDOWN ********************

#  Okay, we grouped the data, we don't see anything. We still need to specify what to do with the groups. Let's calculate a `.mean()`.

# CELL ********************


# MARKDOWN ********************

#  Or just a `.count()` of the columns:

# CELL ********************


# MARKDOWN ********************

#  Or check the mean rating per start year (for movies) and use pandas plotting

# CELL ********************


# MARKDOWN ********************

#  But some years have much more movies than others. How does that effect the mean rating per start year. <br> Below is an example of method chaining: applying all sorts of functions after eachother.

# CELL ********************


# MARKDOWN ********************

#  And let's plot the result with plotly express

# CELL ********************


# MARKDOWN ********************

# There's a whole of functions you can apply to groups. These are just examples:
# - count()
# - mean()
# - min()
# - max()
# 
# 
# See more info here:  
# 
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.html#pyspark.sql.GroupedData

# MARKDOWN ********************

# Last thing: let's say you want to apply a custom aggregation to your groups. How do we do that?
# 
# In PySpark, you can achieve custom aggregation using the agg() function along with udf (User Defined Function). Here's how you can do it:
# 
# First, you need to define your custom function and convert it into a udf.

# CELL ********************


# MARKDOWN ********************

# Then, you can use this udf in your aggregation:

# CELL ********************

