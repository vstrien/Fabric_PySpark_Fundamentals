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

# # Let's read in and inspect some imdb movies and tv series data
# 
# 
# If you don't remember what command to use: check the cheat sheet! :)
# 
# 
# The dataset is a subset of scraped data from IMDB https://www.imdb.com/ and includes the most voted titles. Only movies with at least 25000 votes are in this dataset.
# 
# Let's see if we can find info on the best movies and tv series or just your favorite ones!

# MARKDOWN ********************

# 1. First things first: we first need to get a Spark session. 
# 
# Please run the following import and start the session:
# 
# ```python
# from pyspark.sql import SparkSession
# 
# spark = SparkSession.builder.appName('01_Exercise').getOrCreate()
# ```

# CELL ********************


# MARKDOWN ********************

#  2) Read in data from file `Files/csvsources/most_voted_titles_enriched.csv` and assign it to a variable `df`

# CELL ********************


# MARKDOWN ********************

#  3) Show the first few lines of your dataframe

# CELL ********************


# MARKDOWN ********************

#  4) What is the shape of your dataframe

# CELL ********************


# MARKDOWN ********************

#  5) Get an overview of general info of your dataframe: which columns are there, which datatypes do the columns have, how many null values etc.

# CELL ********************


# MARKDOWN ********************

#  6) How many null values are there in the columns of your dataframe?

# CELL ********************


# MARKDOWN ********************

#  7) Print all the column names of your dataframe

# CELL ********************


# MARKDOWN ********************

#  8) Use `df['genre1'].value_counts()` to figure out which genres there are.

# CELL ********************


# MARKDOWN ********************

#  9) Also do a `.value_counts()` for column `endYear`. Try it once with argument `dropna=True` and once with using argument `dropna=False`

# CELL ********************


# MARKDOWN ********************

#  10) Try tab completion to see what functions and attributes are all available for your dataframe: write `df.` followed by `Tab`

# CELL ********************

