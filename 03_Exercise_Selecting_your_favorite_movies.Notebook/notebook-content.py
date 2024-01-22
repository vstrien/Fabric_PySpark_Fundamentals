# Synapse Analytics notebook source


# MARKDOWN ********************

#  Let's finally do some selections and find your favorite movies

# MARKDOWN ********************

#  1) import pandas, read in the movies dataset and make sure you see all the columns
# 
# https://github.com/wortell-smart-learning/python-data-fundamentals/raw/main/data/most_voted_titles_enriched.csv

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

#  5) Find your favorite movie or tv series in the list. What rating does it get? (use `.str.contains('your_text', case=False)`, see also the cheatsheet)

# CELL ********************


# MARKDOWN ********************

#  6) Find all movies (not tvSeries) that have a rating higher than 8.5 and are in English

# CELL ********************


# MARKDOWN ********************

#  7) Filter you dataset on all movies from Netherlands or Belgium. What is the highest rated movie in the list?
#  Hints:
# - you can try filtering using method `.isin(['Netherlands', 'Belgium'])`

# CELL ********************

