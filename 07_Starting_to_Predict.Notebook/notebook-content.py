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

# # A first introduction to predictive modeling (aka machine learning)

# MARKDOWN ********************

#  Let's try to predict (with some hindsight) who will survive the Titanic disaster
#  
#  We'll use PySpark to do the data wrangling and sci-kit learn to do the modeling and predictions

# CELL ********************

import numpy as np

import plotly.express as px

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import rand, when

from pyspark.ml.evaluation import MultiClassClassificationEvaluator
evaluator = MultiClassClassificationEvaluator(metricName="accuracy")

# CELL ********************

df = spark.read.csv('Files/csvresources/titanic.csv')

# CELL ********************

df.info()

# CELL ********************

display(df.head(2))

# MARKDOWN ********************

#  Let's pretend we don't know anything. A random model would be predict a 50/50 chance to survive or not. This is the dumbest model we can come up with. Let's create some random predictions:

# CELL ********************

df_randompredict = df.withColumn("random_prediction", when(rand() > 0.5, 1).otherwise(0))

# MARKDOWN ********************

#  What is our accuracy score when I take this model of random predictions?

# CELL ********************

evaluator.evaluate(df_randompredict, labelCol="survived", predictionCol="random_prediction")


# MARKDOWN ********************

#  But we can do better by looking at what the percentage of survivors is? 

# CELL ********************

px.bar(
    df['survived'].value_counts(dropna=False, normalize=True) * 100.,
    title='Survival rate on the Titanic',
    range_y=[0, 100]
)

# MARKDOWN ********************

#  So 60% did not survive and only 40% did survive. So if we would predict noone to survive. We would have 60% correct. That's already a better model!

# CELL ********************

df_alwayszero = df.withColumn("alwayszero_prediction", 0)
evaluator.evaluate(df_randompredict, labelCol="survived", predictionCol="alwayszero_prediction")

# MARKDOWN ********************

#  But we can do better of course if look at the data and see what else predicts survival or not<br>Let's see what the effect of passenger class is

# CELL ********************

group_pclass = df.groupby(['pclass', 'survived'], as_index=False)['alive'].count()
group_pclass['perc_of_group'] = group_pclass['alive'] / group_pclass.groupby('pclass')['alive'].transform('sum') * 100.

px.bar(group_pclass, 'survived', 'perc_of_group', facet_row='pclass')

# MARKDOWN ********************

#  And gender could also maybe have an effect on chances of survival

# CELL ********************

df_groupby_sex = df.groupby(['sex', 'survived'], as_index=False)['alive'].count()
df_groupby_sex['perc_of_group'] = df_groupby_sex['alive'] / df_groupby_sex.groupby(['sex'])['alive'].transform('sum') * 100.

display(df_groupby_sex)

# CELL ********************

px.bar(df_groupby_sex, 'survived', 'perc_of_group', facet_row='sex')

# MARKDOWN ********************

#  And so on and so on, there could be many variables that have a predictive effect. This is where we need a statistical model to keep of all the effects and come up with good predictions.

# MARKDOWN ********************

#  Let's try to build a first model with the 2 variables that have a clear effect on survival rates: passenger class and sex.

# MARKDOWN ********************

#  But statistical models need numbers and our column sex only contains strings `male` and `female`. So we need a numerical column.

# CELL ********************

from pyspark.ml.feature import StringIndexer, OneHotEncoder

indexer = StringIndexer(inputCol="sex", outputCol="sex_index")
df = indexer.fit(df).transform(df)

encoder = OneHotEncoder(inputCol="sex_index", outputCol="sex_vec")
df = encoder.fit(df).transform(df)

display(df.head(3))

# MARKDOWN ********************

#  We need to split what is used as an input to predict and what needs to be predicted: X and y

# CELL ********************

# X = df[['pclass', 'sex_code']]

# y = df['survived']

# MARKDOWN ********************

# PySpark's `LogisticRegression` requires the input data to be in a specific format. All features should be assembled in a single column (usually named "features") of vectors. You can use the VectorAssembler to do this:

# CELL ********************

assembler = VectorAssembler(
    inputCols=['pclass', 'sex_code'],
    outputCol="survived"
)

df = assembler.transform(df)

# MARKDOWN ********************

#  Now we can build our first model

# CELL ********************

logit_model = LogisticRegression()

model = logit_model.fit(df)

df_logit_predictions = model.transform(df)

evaluator.evaluate(df_randompredict, labelCol="survived", predictionCol="prediction")

# MARKDOWN ********************

#  And now we have almost 79% correct predictions when we check the accuracy :)

# CELL ********************

