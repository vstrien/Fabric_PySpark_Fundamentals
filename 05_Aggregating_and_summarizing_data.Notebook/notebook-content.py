# Synapse Analytics notebook source

# METADATA ********************

# META {
# META   "synapse": {
# META     "lakehouse": {
# META       "default_lakehouse": "c95d7b56-9a7f-4b7c-baf4-ea0bdaacbbf7",
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Aggregating and summarizing data

# CELL ********************

import pandas as pd
import plotly.express as px

pd.__version__

# CELL ********************

df = pd.read_csv('https://github.com/wortell-smart-learning/python-data-fundamentals/raw/main/data/most_voted_titles_enriched.csv')

df.head(3)

# MARKDOWN ********************

#  You can do a simple count of values with `.value_counts()`, but this only works on a Series (so just 1 column). Please notice the argument  `dropna=False`

# CELL ********************

df['endYear'].value_counts(dropna=False).head(5)

# MARKDOWN ********************

#  If you want percentages you can use argument `normalize=True` 

# CELL ********************

df['endYear'].value_counts(dropna=False, normalize=True).head(5)

# MARKDOWN ********************

#  But one of the most powerful features of pandas is the method `.groupby()`
#  This makes it possible to divide your data in groups and summarize it however you like.
#  With built-in functions such as .mean(), but you also create your own custom functions

# MARKDOWN ********************

#  Let's first group data by title type

# CELL ********************

df.groupby(by='titleType', dropna=False)

# MARKDOWN ********************

#  Okay, we grouped the data, we don't see anything. We still need to specify what to do with the groups. Let's calculate a `.mean()`.

# CELL ********************

df.groupby(by='titleType', dropna=False).mean()

# MARKDOWN ********************

#  Or just a `.count()` of the columns:

# CELL ********************

df.groupby(by='titleType', dropna=False).count()

# MARKDOWN ********************

#  But we see a count of all the columns, I just want the `.count()` of 1 column:

# CELL ********************

df.groupby(by='titleType', dropna=False)[['genres']].count()

# MARKDOWN ********************

#  Or a count of your groups over multiple columns

# CELL ********************

df.groupby(by='titleType', dropna=False)[['startYear', 'endYear', 'genres']].count()

# MARKDOWN ********************

#  Or check the mean rating per start year (for movies) and use pandas plotting

# CELL ********************

df.query('titleType == "movie"').groupby('startYear')[['averageRating']].mean().plot()

# MARKDOWN ********************

#  But some years have much more movies than others. How does that effect the mean rating per start year. <br> Below is an example of method chaining: applying all sorts of functions after eachother.

# CELL ********************

df_group_startyear = (df
    .query('titleType == "movie"')
    .groupby('startYear', as_index=False)
    .agg(mean=('averageRating', 'mean'), count=('averageRating', 'count'))
)
df_group_startyear.head(3)

# MARKDOWN ********************

#  And let's plot the result with plotly express

# CELL ********************

px.scatter(
    title='Average rating per start year', 
    data_frame=df_group_startyear, 
    x='startYear', 
    y='mean', 
    size='count', 
    size_max=25,
    height=400,
    width=700,
)

# MARKDOWN ********************

#  There's a whole of functions you can apply to groups. These are just examples:
# - count()
# - mean()
# - min()
# - max()
# - rolling()
# - cumsum()
# - diff()
# - ffill()
# 
# See more info here:<br>https://pandas.pydata.org/pandas-docs/stable/reference/groupby.html

# MARKDOWN ********************

#  Last thing: let's say you want to apply a custom aggregation to your groups. How do we do that?

# CELL ********************

def custom_mean_calulation(x):
    return x.mean() * 10

df.groupby('startYear')['averageRating'].apply(custom_mean_calulation)

# MARKDOWN ********************

#  Oh, let's say you want to group averages, but add those group averages to every row of your dataframe:

# CELL ********************

df['group_mean_rating'] = df.groupby('startYear')['averageRating'].transform('mean')
df[['tconst', 'startYear', 'averageRating', 'group_mean_rating']].query('startYear > 1960').head(5)

# CELL ********************

