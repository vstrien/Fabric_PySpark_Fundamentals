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

#  Let's first read in our data again
# https://github.com/wortell-smart-learning/python-data-fundamentals/raw/main/data/most_voted_titles_enriched.csv

# CELL ********************


# CELL ********************


# CELL ********************


# MARKDOWN ********************

#  Let's say we only want 1 column. How do we do that? Here are 2 ways:

# MARKDOWN ********************

#  1. Specifying the column you want: let's say we want to only look at the startYear column

# CELL ********************


# MARKDOWN ********************

#  Specifying only 1 column gives you a Series. Let's check the type.

# CELL ********************


# MARKDOWN ********************

#  2. The column names are also attributes, so you also use the dot notation

# CELL ********************


# MARKDOWN ********************

#  So selecting multiple columns can be done by using a list

# CELL ********************


# MARKDOWN ********************

#  Let's say you only want titles with an average rating greater than 9.0. We need to use boolean vectors:

# CELL ********************


# CELL ********************


# MARKDOWN ********************

#  But we want multiple conditions: average rating greater than 9 AND only movies:

# CELL ********************


# CELL ********************


# CELL ********************


# MARKDOWN ********************

#  But this gets tedious, so I myself prefer to use the dataframe method `.query()`

# CELL ********************


# MARKDOWN ********************

#  One handy way of selecting strings still is using `.isin()`. Let's check for titles that are in genre or drama

# CELL ********************


# MARKDOWN ********************

#  Ok, ok, just one more thing: if you want to find a string in a text, you can use `.str.contains('your_text', case=False)`

# CELL ********************

