# Synapse Analytics notebook source


# MARKDOWN ********************

# # Creating plots of your dataframes

# MARKDOWN ********************

# ### The easiest way to create plots good plot is these 2 options:
# #### - use the seaborn library for plots
# #### - or if you want interactive plots use a library called plotly express

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

# ### Let's see how you make a scatter plot in seaborn and plot runtime vs the average rating.
# ### The syntax is usually like this:
# #### - specify the dataframe you want to use
# #### - specify your x-variable
# #### - specify your y-variable
# #### - and if you would like to color certain points, then specify the hue

# MARKDOWN ********************

# ### Here's the seaborn way of doing things.<br>Let's show a scatter plot of `runtimeMinutes` on the x-axis and `averageRating` on the y-axis and let's color by `titleType`

# CELL ********************


# MARKDOWN ********************

# ### But plotting interactive plots is just as easy and makes it easier to check outliers. We are using plotly.express for this.

# CELL ********************


# MARKDOWN ********************

# ### There all sorts of plots with plotly.express. Here's an example of a histogram of averageRating

# CELL ********************

px.histogram(
    title='Histogram of average rating vs titletype',
    data_frame=df,
    x='averageRating',
    color='titleType',
    histnorm='probability density',
)

# MARKDOWN ********************

# ### Or a boxplot of rating by titletype

# CELL ********************

px.box(
    title='Comparing average rating by titleType',
    data_frame=df,
    x='titleType',
    y='averageRating',
)

# MARKDOWN ********************

# ### Sometimes you don't even need to specify the x and y value. This is the case when there's only an index and 1 column

# CELL ********************

px.bar(df['country'].value_counts().head(5))
