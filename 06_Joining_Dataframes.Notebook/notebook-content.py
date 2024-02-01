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

# In Spark the easiest way to join 2 tables is to use `df.join()`
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html

# MARKDOWN ********************

#  Let's create some example data

# CELL ********************

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('05_Aggregating_and_summarizing_data').getOrCreate()

movie_data = [
    ['Pulp Fiction', 90],
    ['James Bond', 120],
    ['Titanic', 115],
]

movies = spark.createDataFrame(movie_data, ['title', 'runtime'])

actor_data = [
    ['Pulp Fiction', 'John Travolta'],
    ['Pulp Fiction', 'Samuel L. Jackson'],
    ['James Bond', 'Sean Connery'],
    ['Terminator', 'Arnold Schwarzenegger'],
]

actors = spark.createDataFrame(actor_data, ['title', 'name'])

# CELL ********************

display(movies)

# CELL ********************

display(actors)

# MARKDOWN ********************

#  Let's try an inner join and see what the result of the inner join looks like

# CELL ********************

df_inner = movies.join(actors, how='inner', on='title')
display(df_inner)

# MARKDOWN ********************

#  Let's do a left join from movies to actors and see what that result looks like

# CELL ********************

df_left = movies.join(actors, how='left', on='title')
display(df_left)

# MARKDOWN ********************

#  And let's do the outer join. All the syntax is quite similar

# CELL ********************

df_outer = movies.join(actors, how='outer', on='title')
display(df_outer)

# MARKDOWN ********************

# ## Joining group aggregates - 'self-joins'
# 
# In the previous module, we looked at aggregates. One example that passed was the average movie score per year:

# CELL ********************

df = spark.read.csv('Files/most_voted_titles_enriched.csv', inferSchema=True, header=True)
df = df.filter(df['titleType'].isin(['tvSeries', 'movie']))

# MARKDOWN ********************

# Now let's say you want to group averages, but add those group averages to every row of your dataframe.
# 
# You cannot "just" add the new (averaged, grouped by year) dataframe to your original dataframe, as the number of rows differ:

# CELL ********************

df_avgs = df.groupby('startYear').mean('averageRating').withColumnRenamed('avg(averageRating)', 'groupMeanRating')
print(f"Number of rows in movie set: {df.count()}")
print(f"Number of rows in averaged set: {df_avgs.count()}")

# MARKDOWN ********************

# However, because the `startYear` column has unique values (after all, these are the values we grouped by), we can join it back to the dataframe.

# CELL ********************

display(df_avgs)

# CELL ********************

df_joined = df.join(df_avgs, on='startYear')
# By default, JOIN will choose an Inner join, and if column names on both sides are equivalent, we don't need to mention them twice:
display(
    df_joined
    .select(['tconst', 'startYear', 'averageRating', 'groupMeanRating'])
    .filter('startYear > 1960')
    .limit(5)
)

# MARKDOWN ********************

# ## Broadcasts

# MARKDOWN ********************

# When joining a big dataset to a small dataset, you can mark the small dataset for broadcast. 
# 
# This means a copy of the dataset will be kept on all nodes of the cluster where the large DataFrame resides, so the large DataFrame will remain in place (and is not shuffled across the network).
# 
# Please note that you should only broadcast DataFrames that are small enough to fit in memory, otherwise you may run into memory issues.

# CELL ********************

from pyspark.sql.functions import broadcast

# MARKDOWN ********************

# In our example, this won't help us: the larger side of the join is still a very small dataset (only 5830 rows):

# CELL ********************

# MAGIC %%timeit
# MAGIC spark.catalog.clearCache()
# MAGIC df.join(df_avgs, on='startYear').count()


# CELL ********************

# MAGIC %%timeit
# MAGIC spark.catalog.clearCache()
# MAGIC df.join(broadcast(df_avgs), on='startYear').count()

# CELL ********************

