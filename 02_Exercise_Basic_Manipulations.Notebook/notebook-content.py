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

# # Trying basic manipulations on movies yourself

# MARKDOWN ********************

#  1) import pandas and read in the movie dataset (`'https://github.com/wortell-smart-learning/python-data-fundamentals/raw/main/data/most_voted_titles_enriched.csv'`) and assign to a variable df 

# CELL ********************


# MARKDOWN ********************

#  2) Run the following line to make sure you can see all the columns of your dataframe

# CELL ********************

pd.options.display.max_columns = 100

# MARKDOWN ********************

#  3) Check the first few lines of your dataframe

# CELL ********************


# MARKDOWN ********************

#  4) Get the summary statistics of your dataframe using `.describe()`. What is the mean averageRating and what is the max averageRating for the movies?

# CELL ********************


# MARKDOWN ********************

#  5) Sort your dataframe on `runtimeMinutes`. What is the shortest and longest (use argument `ascending=False`) movie or tv series in this dataset?

# CELL ********************


# MARKDOWN ********************

#  6) In the answer to the question before we almost only saw tv-series. Sort on both `titleType` and `runtimeMinutes` to figure out what the longest movie is in this dataset.

# CELL ********************


# MARKDOWN ********************

#  7) Check which column has the most null values.

# CELL ********************


# MARKDOWN ********************

#  8) We are going to delete column `endYear`. Don't forget to assign it back to your variable df. Also check make sure that your df now has 25 columns instead of 26 columns.

# CELL ********************


# MARKDOWN ********************

#  9) Create a new column called `new_rating` that multiplies column `averageRating` by 10 (so it is now on a scale of 1 to 100).

# CELL ********************


# MARKDOWN ********************

#  10) Do a value counts to find out how many adult movies are in this dataset (use column `isAdult`)

# CELL ********************

