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

# # Creating selections and subsets of your data

# MARKDOWN ********************

#  There are many ways to get selections or subsets of your data:
#  - selecting a column with `df['averageRating']`
#  - selecting multiple columns using a list: `df[['tconst', 'averageRating']]`
#  - selecting a subset using a condition: `df[df['averageRating'] > 9.0]`
#  - using `.query("averageRating > 0")`

# MARKDOWN ********************

#  Let's first read in our data again and check the first few lines

# CELL ********************

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('03_Making_selections').getOrCreate()
df = spark.read.csv('Files/csvsources/most_voted_titles_enriched.csv', inferSchema=True, header=True, multiLine=True)

display(
    df.limit(3)
)

# MARKDOWN ********************

#  Let's say we only want 1 column. How do we do that? Here are 2 ways:

# MARKDOWN ********************

#  1. Specifying the column you want: let's say we want to only look at the startYear column

# CELL ********************


# MARKDOWN ********************

# You can also just select the column. But you can't really look into it - it's only a reference:

# CELL ********************


# CELL ********************


# MARKDOWN ********************

# The column names are also attributes, so you also use the dot notation

# CELL ********************


# MARKDOWN ********************

#  So selecting multiple columns can be done by using a list
#  (This is new syntax - not every colleague may be aware of this)

# CELL ********************


# MARKDOWN ********************

#  Let's say you only want titles with an average rating greater than 9.0. We can use expressions like this:

# CELL ********************


# CELL ********************


# MARKDOWN ********************

#  But we want multiple conditions: average rating greater than 9 AND only movies:

# CELL ********************


# CELL ********************


# CELL ********************


# MARKDOWN ********************

#  But this gets tedious, so I myself prefer to use the dataframe method `.filter()`

# CELL ********************


# MARKDOWN ********************

#  One handy way of selecting strings still is using `.isin()`

# CELL ********************


# MARKDOWN ********************

# If you want to find a string in a text, you can use `.contains('your_text')`

# CELL ********************


# MARKDOWN ********************

# If you want to do it case-insensitive, use the `lower` function, which can operate on a column.

# CELL ********************

