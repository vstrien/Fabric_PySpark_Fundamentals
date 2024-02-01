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

# # Basic manipulations of your dataframe

# MARKDOWN ********************

#  How can we add a column, delete a column, rename columns etc.?

# CELL ********************

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('02_basic_manipulations').getOrCreate()

# CELL ********************

df = spark.read.csv('Files/most_voted_titles_enriched.csv', inferSchema=True, header=True)

# CELL ********************

display(df.limit(3))

# MARKDOWN ********************

#  Let's say we don't like primary Title column, let's delete it

# CELL ********************

display(df.drop('primaryTitle').limit(3))

# MARKDOWN ********************

#  Let's now add a column that takes the metascore and divides it by 10

# CELL ********************

df = df.withColumn('new_metascore', df['metascore'] / 10)
display(df.where(df.metascore > 10).limit(3))

# MARKDOWN ********************

#  If we want to rename a column, we can use `.rename()`

# CELL ********************

display(df.withColumnRenamed('startYear', 'start_year').limit(3))

# MARKDOWN ********************

#  But we haven't assigned it yet back to the df variable, it's still called startYear

# CELL ********************

display(df.limit(3))

# MARKDOWN ********************

#  So we have to write it back to the variable dataframe

# CELL ********************

df = df.withColumnRenamed('startYear', 'start_year')
display(df.limit(3))

# MARKDOWN ********************

#  What shall we do with null values? We can use .fillna() to give them a specific value.

# CELL ********************

display(df.filter(df.endYear.isNull()).limit(3))

# MARKDOWN ********************

# So `tt0010323` has an endYear that has a null-value (NaN) right now. With the `fillna` method we can fill those specific values, but not the others:

# CELL ********************

df = df.fillna({'endYear': -1})
display(df.filter(df.tconst == "tt0010323").limit(3))

# MARKDOWN ********************

#  Sorting your dataframe is also important. This can be done with `.sort()` Don't forget to inspect additional arguments of this function using `Shift + Tab` inside the function.

# MARKDOWN ********************

#  1. Let's first sort the dataframe on `originalTitle` using argument `by`

# CELL ********************

display(df.sort('originalTitle'))

# MARKDOWN ********************

#  2. Now sort the whole list on title in descending order.

# CELL ********************

display(df.sort('originalTitle', ascending=False))

# MARKDOWN ********************

#  3. Or we can sort on multiple columns. Now we need to use lists:

# CELL ********************

display(df.sort(['startYear', 'runtimeMinutes'], ascending=[False, True]))
