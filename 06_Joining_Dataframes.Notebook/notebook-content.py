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

# # Joining dataframes

# MARKDOWN ********************

#  In SQL we use inner joins, left joins, and outer joins to connect 2 tables to eachother. <br>The join to connect the 2 tables is made on a id or column that can be found in both datasets

# MARKDOWN ********************

#  In pandas the easiest way to join 2 tables is to use `df.merge()`
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.merge.html

# MARKDOWN ********************

#  Let's create some example data

# CELL ********************

import pandas as pd

movie_data = [
    ['Pulp Fiction', 90],
    ['James Bond', 120],
    ['Titanic', 115],
]

movies = pd.DataFrame(movie_data, columns=['title', 'runtime'])

actor_data = [
    ['Pulp Fiction', 'John Travolta'],
    ['Pulp Fiction', 'Samuel L. Jackson'],
    ['James Bond', 'Sean Connery'],
    ['Terminator', 'Arnold Schwarzenegger'],
]

actors = pd.DataFrame(actor_data, columns=['title', 'name'])

# CELL ********************

movies

# CELL ********************

actors

# MARKDOWN ********************

#  Let's try an inner join and see what the result of the inner join looks like

# CELL ********************

df_inner = movies.merge(actors, how='inner', on='title')
df_inner

# MARKDOWN ********************

#  FYI: instead of using a dataframe function, you can also use the general pandas function pd.merge()

# CELL ********************

df_inner = pd.merge(movies, actors, how='inner', on='title')
df_inner

# MARKDOWN ********************

#  Let's do a left join from movies to actors and see what that result looks like

# CELL ********************

df_left = movies.merge(actors, how='left', on='title')
df_left

# MARKDOWN ********************

#  And let's do the outer join. All the syntax is quite similar

# CELL ********************

df_outer = movies.merge(actors, how='outer', on='title')
df_outer

# CELL ********************

