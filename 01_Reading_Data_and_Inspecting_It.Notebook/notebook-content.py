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

# # Reading in data files and inspecting the resulting dataframe

# MARKDOWN ********************

#  Okay, we have some data and we would like to inspect it, wrangle it, and analyze it.
#  It of course all starts with getting the data in your PySpark dataframe.

# MARKDOWN ********************

#  There are many ways to get data into PySpark, most have the following syntax:
# - `spark.read.csv()`
# - `spark.read.json()`
# - `spark.read.parquet()`
# - `spark.read.jdbc()`
# - etc. etc.
# 
#  See this link for more options:
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/io.html

# MARKDOWN ********************

#  But first, let's connect to a Spark session so we can use the pyspark.sql-methods

# CELL ********************

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('example_app').getOrCreate()

# MARKDOWN ********************

#  We check which files are available in the directory with a magic command `%ls`:

# CELL ********************

%ls

# MARKDOWN ********************

#  Let's read in some data with `spark.read.csv()`. It's common practice to assign the result to a variable called `df`.  
#  We'll use the shorthand notation to load files from the attached Lakehouse here:

# CELL ********************

df = spark.read.csv('Files/titanic.csv', inferSchema=True, header=True)

# MARKDOWN ********************

#  What did we just create here? We can use the general python function `type()` to get info what type of object this is:

# MARKDOWN ********************


# CELL ********************

type(df)

# MARKDOWN ********************

#  Let's see what we got when we did this and inspect the first lines with `df.head()`. This is a method that is available on dataframes and series.
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.head.html?highlight=head#pyspark.sql.DataFrame.head

# CELL ********************

df.head(3)

# MARKDOWN ********************

# If you want to display it in a nice visual format, you could also use the `toPandas()` method.  
# Be aware that this will force a computation (and thus can kill your performance)!

# CELL ********************

df.toPandas().head(3)

# MARKDOWN ********************

#  Or check the last lines with `df.tail()`

# CELL ********************

df.tail(3)

# MARKDOWN ********************

#  What is the shape of this dataframe? We can use the `.columns` attribute and the `.count()` method to find out the shape:

# CELL ********************

n_columns = len(df.columns)
n_rows = df.count()
shape = (n_columns, n_rows)
print("Shape: ", shape)

# MARKDOWN ********************

#  What are the names of all columns? We can see that with another attribute called `.columns`

# CELL ********************

df.columns

# CELL ********************

type(df.columns)

# MARKDOWN ********************

#  As you can see pandas calls this an Index (which contains all column names)

# MARKDOWN ********************

#  DataFrames have many methods and attributes, you can check them with tab completion

# CELL ********************

df.

# MARKDOWN ********************

#  Let's see what the dataframe looks like in general by using dataframe methods:
#  
#  * `.printSchema()`
#  * `.describe()`
#  * `.count()`

# CELL ********************

df.printSchema()

# CELL ********************

df.describe().toPandas() # Using toPandas() for a nicer view here.

# CELL ********************

df.count()

# MARKDOWN ********************

#  Reading in data with `pd.read_csv()` went very easy (maybe too easy?). Let's check what arguments are available for this function, using `Ctrl + Space` inside the function.

# CELL ********************

pd.read.csv()

# MARKDOWN ********************

#  To get quick info about a column of the counts 
#  
#  * In Pandas we would do `df['column_name'].value_counts(dropna=False)`
#  * In `spark.sql` we will do a `groupBy` and `count` instead
# 
#  

# CELL ********************

df.groupBy('sex').count().show()

# MARKDOWN ********************

#  New concepts discussed here:
# - general pandas methods: `pandas.read.csv()`
# - attributes of dataframes, such as: `df.columns`
# - and methods of a dataframe: `df.head()`, `df.describe()`
# - get counts of values in a column: df.groupBy('column_name').count().show()

# CELL ********************

