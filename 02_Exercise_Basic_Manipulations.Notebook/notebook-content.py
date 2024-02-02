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

# 1. Acquire a Spark session, read in the movie dataset (`'Files/csvsources/most_voted_titles_enriched.csv'`) and assign to a variable df
# 
# Besides setting the header and schema, add `multiLine=True` option to `spark.read.csv()`

# CELL ********************

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('02_exercise').getOrCreate()

# CELL ********************

df = spark.read.csv('Files/csvsources/most_voted_titles_enriched.csv', header=True, inferSchema=True, multiLine=True)

# MARKDOWN ********************

# 2. Check the first few lines of your dataframe

# CELL ********************

display(
    df.limit(3)
)

# MARKDOWN ********************

# Get the summary statistics of your dataframe using `.describe()`. 
# 
# In order for `describe` to work, you need to select only the numeric columns ('isAdult', 'startYear', 'endYear', 'runtimeMinutes', 'averageRating', 'numVotes', 'metascore' - but if all is too easy, select the numeric columns programmatically 🙂)
# 
# 3. What is the mean averageRating and what is the max averageRating for the movies?

# CELL ********************

numeric_types = ["int", "double", "float", "decimal"]
numeric_cols = [field[0] for field in df.dtypes if field[1] in numeric_types]
print(numeric_cols)

# CELL ********************

display(
    df.select(numeric_cols).describe()
)

# MARKDOWN ********************

# 5. Sort your dataframe on `runtimeMinutes`. What is the shortest and longest (use argument `ascending=False`) movie or tv series in this dataset?

# CELL ********************

display(
    df.sort('runtimeMinutes').limit(1)
)

# CELL ********************

display(
    df.sort('runtimeMinutes', ascending=False).limit(1)
)

# MARKDOWN ********************

# 6. In the answer to the question before we almost only saw tv-series. Sort on both `titleType` and `runtimeMinutes` to figure out what the longest movie is in this dataset.

# CELL ********************

display(
    df.sort(['titleType', 'runtimeMinutes'], ascending=[True, False]).limit(1)
)

# MARKDOWN ********************

# 7. We are going to delete column `endYear`. Don't forget to assign it back to your variable df. Also check make sure that your df now has 25 columns instead of 26 columns.

# CELL ********************

df = df.drop('endYear')
len(df.columns)

# MARKDOWN ********************

#  9) Create a new column called `new_rating` that multiplies column `averageRating` by 10 (so it is now on a scale of 1 to 100).

# CELL ********************

df = df.withColumn("new_rating", df.averageRating * 10)
display(
    df.select("primaryTitle", "averageRating", "new_rating")
)

# MARKDOWN ********************

#  10) Find out how many adult movies are in this dataset (use column `isAdult`)

# CELL ********************

display(
    df.groupBy('isAdult').count()
)

# CELL ********************

