# Synapse Analytics notebook source


# MARKDOWN ********************

# # A first introduction to predictive modeling (aka machine learning)

# MARKDOWN ********************

# ## Let's try to predict (with some hindsight) who will surve the Titanic disaster<br>We need pandas to do the data wrangling and sci-kit learn to do the modeling and predictions

# CELL ********************

import numpy as np
import pandas as pd

import plotly.express as px

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score

# CELL ********************

df = pd.read_csv('https://github.com/wortell-smart-learning/python-data-fundamentals/raw/main/data/titanic.csv')

# CELL ********************

df.info()

# CELL ********************

df.head(2)

# MARKDOWN ********************

# ### Let's pretend we don't know anything. A random model would be predict a 50/50 chance to survive or not. This is the dumbest model we can come up with. Let's create some random predictions:

# CELL ********************


# MARKDOWN ********************

# ### What is our accuracy score when I take this model of random predictions?

# CELL ********************


# MARKDOWN ********************

# ## But we can do better by looking at what the percentage of survivors is? 

# CELL ********************


# MARKDOWN ********************

# ## So 60% did not survive and only 40% did survive. So if we would predict noone to survive. We would have 60% correct. That's already a better model! Let's check the accuracy score.

# CELL ********************


# MARKDOWN ********************

# ## But we can do better of course if look at the data and see what else predicts survival or not<br>Let's see what the effect of passenger class is

# CELL ********************

group_pclass = df.groupby(['pclass', 'survived'], as_index=False)['alive'].count()
group_pclass['perc_of_group'] = group_pclass['alive'] / group_pclass.groupby('pclass')['alive'].transform('sum') * 100.

# create plot here


# MARKDOWN ********************

# ## And gender could also maybe have an effect on chances of survival

# CELL ********************

group_sex = df.groupby(['sex', 'survived'], as_index=False)['alive'].count()
group_sex['perc_of_group'] = group_sex['alive'] / group_sex.groupby(['sex'])['alive'].transform('sum') * 100.

# show table and create plot here


# MARKDOWN ********************

# ## And so on and so on, there could be many variables that have a predictive effect. This is where we need a statistical model to keep of all the effects and come up with good predictions.

# MARKDOWN ********************

# ## Let's try to build a first model with the 2 variables that have a clear effect on survival rates: passenger class and sex.

# MARKDOWN ********************

# ## But statistical models need numbers and our column sex only contains strings `male` and `female`. So we need a numerical column.

# CELL ********************

df['sex_code'] = pd.get_dummies(df['sex'], drop_first=True)

df.head(3)

# MARKDOWN ********************

# ## We need to split what is used as an input to predict and what needs to be predicted: X and y

# CELL ********************


# MARKDOWN ********************

# ## Now we can build our first logistic regression model

# CELL ********************


# MARKDOWN ********************

# ## And now we have almost 79% correct predictions when we check the accuracy :)

# CELL ********************

