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

# # Creating plots of your dataframes

# MARKDOWN ********************

#  The easiest way to create plots good plot is these 2 options:
#  - use the seaborn library for plots
#  - or if you want interactive plots use a library called plotly express

# CELL ********************

from pyspark.sql import SparkSession
import seaborn as sns

import plotly.express as px

# some nicer plotly setting
import plotly.io as pio
pio.templates.default = 'plotly_white'

spark = SparkSession.builder.appName('04_Plotting_Data').getOrCreate()
df = spark.read.csv('Files/most_voted_titles_enriched.csv', inferSchema=True, header=True)

display(
    df.limit(3)
)

# MARKDOWN ********************

# There is a small issue with this dataset - besides the expected title types 'tvShow' and 'movie' there are some more:

# CELL ********************

display(df[['titleType']].distinct())

# MARKDOWN ********************

# If you like, you could fix this:
# 
# 1. find out if the CSV file is coherent in designating columns and quotes
# 2. if it is, ensure the correct settings of `read.csv`
# 
# But for now, let's just ignore these seven wrong rows (note that the movies are still in there, it's just that the row has been cut off). Let's filter out the non 'tvSeries' and 'movie' rows:

# CELL ********************

df = df.filter(df['titleType'].isin(['tvSeries', 'movie']))

# MARKDOWN ********************

# Let's see how you make a scatter plot in seaborn and plot runtime vs the average rating.
# 
# The syntax is usually like this:
# 
# * specify the dataframe you want to use
# * specify your x-variable
# * specify your y-variable
# * and if you would like to color certain points, then specify the hue
# 
# In order to use `seaborn`, we need a *real* Pandas DataFrame. So instead of using the Pandas API of PySpark (which suffices for table visualizations inside Spark Notebooks), we'll use the `toPandas()` method now to create a real DataFrame:

# CELL ********************

print(f"""
    Original df: {type(df)} --> this is the SparkSQL API from Spark. Highly efficiënt, distributed, and geared towards Spark
    Pandas API df: {type(df.pandas_api())} --> this it the pandas API from Spark. It tries to bring all Pandas methods with Spark performance
    toPandas df: {type(df.toPandas())} --> this is a "real" pandas Dataframe. Forces a computation, all data will be collected and processed
    """)

# MARKDOWN ********************

# As you can see, we're still in 
#  Here's the seaborn way of doing things:

# CELL ********************

sns.scatterplot(
    data=df.toPandas(), 
    x='runtimeMinutes', 
    y='averageRating', 
    hue='titleType', 
    s=3.
)

# MARKDOWN ********************

#  But plotting interactive plots is just as easy and makes it easier to check outliers. We are using plotly.express for this.

# CELL ********************

px.scatter(
    title='runtime vs average rating',
    # Notice we will filter on the Spark API, and defer the transition to Pandas as late as possible:
    data_frame=df.filter('runtimeMinutes < 400').toPandas(), 
    x='runtimeMinutes', 
    y='averageRating', 
    color='titleType',
    hover_data=['primaryTitle'],
    opacity=0.4,
    height=500,
    width=700,
)

# MARKDOWN ********************

#  There all sorts of plots with plotly.express. Here's an example of a histogram

# CELL ********************

px.histogram(
    title='Histogram of average rating vs titletype',
    data_frame=df.toPandas(),
    x='averageRating',
    color='titleType',
    histnorm='probability density',
    width=700,
)

# MARKDOWN ********************

#  Or a boxplot of rating by titletype

# CELL ********************

px.box(
    title='Comparing average rating by titleType',
    data_frame=df.toPandas(),
    x='titleType',
    y='averageRating',
    width=700,
)

# CELL ********************

