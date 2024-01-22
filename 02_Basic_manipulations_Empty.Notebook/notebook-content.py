# Synapse Analytics notebook source


# MARKDOWN ********************

# # Basic manipulations of your dataframe

# MARKDOWN ********************

#  How can we add a column, delete a column, rename columns etc.?

# MARKDOWN ********************

# Let's read in the Movies-data and check the first few lines
# 
# https://github.com/wortell-smart-learning/python-data-fundamentals/raw/main/data/most_voted_titles_enriched.csv

# CELL ********************


# CELL ********************


# CELL ********************


# MARKDOWN ********************

#  Notice that we don't see all columns when doing `df.head()` that's annoying.<br>Let's first check some of the standard settings and then change them:

# CELL ********************


# MARKDOWN ********************

#  This means only 20 columns will be shown, but our dataframe has 26 columns. Let's change this setting:

# CELL ********************


# MARKDOWN ********************

#  Let's say we don't like primary Title column, let's delete it

# CELL ********************


# MARKDOWN ********************

#  Let's now create a new column that takes the metascore and divides it by 10

# CELL ********************


# MARKDOWN ********************

#  If we want to rename a column, we can use `.rename()`

# CELL ********************


# MARKDOWN ********************

#  But we haven't assigned it yet back to the df variable, it's still called startYear

# CELL ********************


# MARKDOWN ********************

#  So we have to write it back to the variable dataframe

# CELL ********************


# MARKDOWN ********************

#  What shall we do with null values? We can use .fillna() to give them a specific value.

# CELL ********************


# MARKDOWN ********************

#  Sorting your dataframe is also important. This can be done with `.sort_values()` Don't forget to inspect additional arguments of this function using `Shift + Tab` inside the function.

# MARKDOWN ********************

#  1. Let's first sort the dataframe on `originalTitle` using argument `by`

# CELL ********************


# MARKDOWN ********************

#  2. Now sort the whole list on title in descending order, using `ascending=False`

# CELL ********************


# MARKDOWN ********************

#  3. Or we can sort on multiple columns. Now we need to use lists:

# CELL ********************

