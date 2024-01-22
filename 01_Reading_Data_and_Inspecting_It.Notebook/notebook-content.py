# Synapse Analytics notebook source


# MARKDOWN ********************

# # Reading in data files and inspecting the resulting dataframe

# MARKDOWN ********************

#  Okay, we have some data and we would like to inspect it, wrangle it, and analyze it.
#  It of course all starts with getting the data in your pandas dataframe.

# MARKDOWN ********************

#  There are many ways to get data into pandas, most have the following syntax:
# - `pd.read_csv()`
# - `pd.read_excel()`
# - `pd.read_parquet()`
# - `pd.read_sql()`
# - etc. etc.
# 
#  See this link for more options:
# https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html

# MARKDOWN ********************

#  But first, let's start with importing pandas

# CELL ********************

import pandas as pd

# MARKDOWN ********************

#  We check which files are available in the directory with a magic command `%ls`:

# CELL ********************

%ls

# MARKDOWN ********************

#  Let's read in some data with `pd.read_csv()`. It's common practice to assign the result to a variable called `df`

# CELL ********************

df = pd.read_csv('https://github.com/wortell-smart-learning/python-data-fundamentals/raw/main/data/titanic.csv')

# MARKDOWN ********************

#  What did we just create here? We can use the general python function `type()` to get info what type of object this is:

# CELL ********************

type(df)

# MARKDOWN ********************

#  Let's see what we got when we did this and inspect the first lines with `df.head()`. This is a method that is available on dataframes and series.
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.head.html

# CELL ********************

df.head()

# MARKDOWN ********************

#  Or check the last lines with `df.tail()`

# CELL ********************

df.tail(3)

# MARKDOWN ********************

#  What is the shape of this dataframe? We can use the attribute `.shape`

# CELL ********************

df.shape

# MARKDOWN ********************

#  What are the names of all columns? We can see that with another attribute called `.columns`

# CELL ********************

df.columns

# CELL ********************

type(df.columns)

# MARKDOWN ********************

#  As you can see pandas calls this an Index (which contains all column names)

# MARKDOWN ********************

#  And while we're at it, let's also check the index of this dataframe with the attribute .index

# CELL ********************

df.index

# MARKDOWN ********************

#  DataFrames have many methods and attributes, you can check them with tab completion

# CELL ********************

df.

# MARKDOWN ********************

#  Let's see what the dataframe looks like in general by using dataframe method `.info()` 

# CELL ********************

df.info()

# MARKDOWN ********************

#  Reading in data with `pd.read_csv()` went very easy (maybe too easy?). Let's check what arguments are available for this function, using `Shift + Tab` inside the function.

# CELL ********************

pd.read_csv()

# MARKDOWN ********************

#  Can we maybe get a short numerical summary of the data? Yes, we can, with: `df.describe()`

# CELL ********************

df.describe(include='all')

# MARKDOWN ********************

#  To get quick info about a column of the counts we can do `df['column_name'].value_counts(dropna=False)`

# CELL ********************

df['sex'].value_counts()

# MARKDOWN ********************

#  New concepts discussed here:
# - general pandas methods: `pd.read_csv()`
# - attributes of dataframes, such as: `df.shape`, `df.columns`
# - and methods of a dataframe: `df.head()`, `df.info()`, `df.describe()`
# - get counts of values in a column: df['column_name'].value_counts() 

# CELL ********************

