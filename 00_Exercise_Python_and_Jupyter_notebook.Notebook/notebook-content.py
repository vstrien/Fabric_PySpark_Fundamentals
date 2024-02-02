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

# # PySpark exercises

# MARKDOWN ********************

#  1) Write a list (the python word for an array) called `my_movies_list` with your 4 favorite movies or tv series
#  Hints:
# - strings can have single quotes `''` or double quotes `""`
# - list have square brackets `[ ]`

# CELL ********************


# MARKDOWN ********************

#  2) Select the second and third element from your list
#  Hints:
# - python starts counting at 0, so the second element gets index number 1

# CELL ********************


# MARKDOWN ********************

#  3) Select the last element from your list
#  Hints:
# - the last element can be selected by using -1

# CELL ********************


# MARKDOWN ********************

# 
#  4) Write a (pythonic) for loop that loops through your movies list and prints them out
#  Hints:
# - use 4 space indentation or a tab as indentation (although Jupyter will give you this automatically)

# CELL ********************


# MARKDOWN ********************

#  5) Create a dictionary (key -  value pairs) called `my_movies_dict` with the same four movies as keys and for each movie add as a value the rating on a scale of 0 to 10 you would give it
#  Hints:
# - dictionaries use angular brackets, like this: `{}`
# - an example would be {'God Father': 3}

# CELL ********************


# MARKDOWN ********************

#  6) Use a for loop to print every movie and it's rating of your dictionary:
#  Hints:
# - using `for key in your_dict:` will iterate over all the keys in your dictionary
# - you can get a value from a dictionary, when you do: `your_dict['key_name']` this will return the value

# CELL ********************


# MARKDOWN ********************

#  7) Write a function called `rating_converter()` that takes as input a movie rating and multiplies it with 10 to convert your movie ratings to a scale of 1 to 100. The function should return the new movie rating.
#  Hints:
# - functions start with `def()`
# - what belongs inside a function should be indented by 4 spaces or a tab

# CELL ********************


# MARKDOWN ********************

#  8) Try tab completion on your rating_converter() function by writing only the first few letters for example `rat` and then pressing <kbd>Ctrl</kbd> + <kbd>space</kbd> 
# If you use Jupyter of Jupyter Lab Google Colab, use here <kbd>Tab</kbd> instead of <kbd>Ctrl</kbd> + <kbd>space</kbd> 

# CELL ********************


# MARKDOWN ********************

# In the next exercises we are going to read in data with `spark.read.csv()`. 
# 
# In order to access the current Spark session, type 
# 
# ```python
# from pyspark.sql import SparkSession
# 
# spark = SparkSession.builder.appName('00_Exercise_Python_and_Jupyter_notebook').getOrCreate()
# ```
# 
# Execute the cell.

# CELL ********************


# MARKDOWN ********************

# 9. Now, type `spark.read.` below and see what happens when you do <kbd>Ctrl</kbd> + <kbd>space</kbd>.

# CELL ********************


# MARKDOWN ********************

#  10) Also try tab completion by only writing `pd.read_` and then pressing <kbd>Ctrl</kbd> + <kbd>space</kbd>.

# CELL ********************

