# Synapse Analytics notebook source


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

import pandas as pd
pd.options.display.max_columns = 50

df = pd.read_csv('https://github.com/wortell-smart-learning/python-data-fundamentals/raw/main/data/most_voted_titles_enriched.csv')

df.head(3)

# MARKDOWN ********************

#  Let's say we only want 1 column. How do we do that? Here are 2 ways:

# MARKDOWN ********************

#  1. Specifying the column you want: let's say we want to only look at the startYear column

# CELL ********************

df['startYear']

# MARKDOWN ********************

#  Specifying only 1 column gives you a Series

# CELL ********************

type(df['startYear'])

# MARKDOWN ********************

#  2. The column names are also attributes, so you also use the dot notation

# CELL ********************

df.startYear

# MARKDOWN ********************

#  So selecting multiple columns can be done by using a list

# CELL ********************

columns_needed = ['tconst', 'averageRating', 'startYear']

df[columns_needed]

# MARKDOWN ********************

#  Let's say you only want titles with an average rating greater than 9.0. We need to use boolean vectors:

# CELL ********************

df['averageRating'] > 9.0

# CELL ********************

df[df['averageRating'] > 9.0].head(3)

# MARKDOWN ********************

#  But we want multiple conditions: average rating greater than 9 AND only movies:

# CELL ********************

(df['titleType'] == 'movie')

# CELL ********************

(df['averageRating'] > 9.0)

# CELL ********************

df[(df['titleType'] == 'movie') & (df['averageRating'] > 9.0)].head(2)

# MARKDOWN ********************

#  But this gets tedious, so I myself prefer to use the dataframe method `.query()`

# CELL ********************

df.query("titleType == 'movie' and averageRating > 9").head(2)

# MARKDOWN ********************

#  One handy way of selecting strings still is using `.isin()`

# CELL ********************

df[df['genre1'].isin(['Crime', 'Drama'])].head(2)

# MARKDOWN ********************

#  Ok, ok, just one more thing: if you want to find a string in a text, you can use `.str.contains('your_text', case=False)`

# CELL ********************

df[df['originalTitle'].str.contains('godfather', case=False)]

# CELL ********************

