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

import pandas as pd
import seaborn as sns

import plotly.express as px

# some nicer plotly setting
import plotly.io as pio
pio.templates.default = 'plotly_white'

df = pd.read_csv('https://github.com/wortell-smart-learning/python-data-fundamentals/raw/main/data/most_voted_titles_enriched.csv')

df.head(3)

# MARKDOWN ********************

#  Let's see how you make a scatter plot in seaborn and plot runtime vs the average rating.
#  The syntax is usually like this:
#  - specify the dataframe you want to use
#  - specify your x-variable
#  - specify your y-variable
#  - and if you would like to color certain points, then specify the hue

# MARKDOWN ********************

#  Here's the seaborn way of doing things:

# CELL ********************

sns.scatterplot(
    data=df, 
    x='runtimeMinutes', 
    y='averageRating', 
    hue='titleType', 
    s=3.
);

# MARKDOWN ********************

#  But plotting interactive plots is just as easy and makes it easier to check outliers. We are using plotly.express for this.

# CELL ********************

px.scatter(
    title='runtime vs average rating',
    data_frame=df.query('runtimeMinutes < 400'), 
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
    data_frame=df,
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
    data_frame=df,
    x='titleType',
    y='averageRating',
    width=700,
)

# MARKDOWN ********************

#  Sometimes you don't even need to specify the x and y value. This is the case when there's only an index and 1 column

# CELL ********************

px.bar(df['country'].value_counts().head(5), width=700)

# CELL ********************

