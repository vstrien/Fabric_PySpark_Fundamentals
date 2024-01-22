# Synapse Analytics notebook source


# MARKDOWN ********************

# # Basic manipulations of your dataframe

# MARKDOWN ********************

# ### How can we add a column, delete a column, rename columns etc.?

# CELL ********************

import pandas as pd

# CELL ********************

df = pd.read_csv('https://github.com/wortell-smart-learning/python-data-fundamentals/raw/main/data/most_voted_titles_enriched.csv')

# CELL ********************

df.head(3)

# MARKDOWN ********************

# ### Notice that we don't see all columns when doing `df.head()` that's annoying.<br>Let's first check some of the standard settings and then change them:

# CELL ********************

pd.options.display.max_columns

# MARKDOWN ********************

# ### This means only 20 columns will be shown, but our dataframe has 26 columns. Let's change this setting:

# CELL ********************

pd.options.display.max_columns = 50

# MARKDOWN ********************

# ### Let's say we don't like primary Title column, let's delete it

# CELL ********************

df.drop(columns=['primaryTitle']).head(3)

# MARKDOWN ********************

# ### Let's now add a column that takes the metascore and divides it by 10

# CELL ********************

df['new_metascore'] = df['metascore'] / 10.
df.head(3)

# MARKDOWN ********************

# ### If we want to rename a column, we can use `.rename()`

# CELL ********************

df.rename(columns={'startYear': 'start_year'}).head(3)

# MARKDOWN ********************

# ### But we haven't assigned it yet back to the df variable, it's still called startYear

# CELL ********************

df.head(3)

# MARKDOWN ********************

# ### So we have to write it back to the variable dataframe

# CELL ********************

df = df.rename(columns={'startYear': 'start_year'})
df.head(3)

# MARKDOWN ********************

# ### What shall we do with null values? We can use .fillna() to give them a specific value.

# CELL ********************

df['endYear'] = df['endYear'].fillna(-1)

# MARKDOWN ********************

# ### Sorting your dataframe is also important. This can be done with `.sort_values()` Don't forget to inspect additional arguments of this function using `Shift + Tab` inside the function.

# MARKDOWN ********************

# ### 1. Let's first sort the dataframe on `originalTitle` using argument `by`

# CELL ********************

df.sort_values(by='originalTitle')

# MARKDOWN ********************

# ### 2. Now sort the whole list on title in descending order, using `ascending=False`

# CELL ********************

df.sort_values(by='originalTitle', ascending=False)

# MARKDOWN ********************

# ### 3. Or we can sort on multiple columns. Now we need to use lists:

# CELL ********************

df.sort_values(by=['startYear', 'runtimeMinutes'], ascending=[False, True])
