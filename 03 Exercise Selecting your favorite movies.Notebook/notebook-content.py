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

#  Let's finally do some selections and find your favorite movies

# MARKDOWN ********************

# 1) Get a Spark session, read in the movies dataset and make sure the session is loaded correctly (headers, datatypes, multi-lines)
# 
# `Files/csvsources/most_voted_titles_enriched.csv`

# CELL ********************


# MARKDOWN ********************

#  2) Create a new dataframe that only has columns `['titleType', 'originalTitle', 'startYear', 'genres', 'averageRating', 'primary_language', 'country']` of your dataframe and assign it to variable `df_new`. 

# CELL ********************


# MARKDOWN ********************

#  3) Select all movies and tv series that have an average rating of 9.2 or higher. How many are there? Is your favorite movie or tv series in the list?

# CELL ********************


# MARKDOWN ********************

#  4) Select all movies and tv series that have an average rating of 9.2 or higher AND sort them on start year. Which one ends up last?

# CELL ********************


# MARKDOWN ********************

# 5. Find your favorite movie or tv series in the list. What rating does it get?
#  
# Import `functions` from `pyspark.sql` as `F`, then use `F.col(..).contains(..)`

# CELL ********************


# MARKDOWN ********************

#  6) Find all movies (not tvSeries) that have a rating higher than 8.5 and are in English

# CELL ********************


# MARKDOWN ********************

#  7) Filter you dataset on all movies from Netherlands or Belgium. What is the highest rated movie in the list?
#  Hints:
# - you can try filtering using method `.isin(['Netherlands', 'Belgium'])`

# CELL ********************

