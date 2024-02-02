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

# # Certificate mania

# MARKDOWN ********************

#  Try to answer the following question using a merge:<br><br>Which colleague has no certificates registered in the SDC database?

# MARKDOWN ********************

#  Hints:
# - use `df.merge()`
# - use datafiles `sdc_certificaten.csv` and `sdc_personeel.csv`
#   * `https://github.com/wortell-smart-learning/python-data-fundamentals/raw/main/data/sdc_certificaten.csv`
#   * `https://github.com/wortell-smart-learning/python-data-fundamentals/raw/main/data/sdc_personeel.csv`
# - for joining/merging don't use argument `on` but use `left_on` and `right_on`
# - you can use `df.unique()` to get the unique nr of values in a column

# CELL ********************


# MARKDOWN ********************

#  1) Looking at `sdc_personeel.csv`, how many colleagues do you have? 

# CELL ********************


# MARKDOWN ********************

#  2) How many Barts en Jeroens do we have in our company?

# CELL ********************


# MARKDOWN ********************

#  3) How many certificates are currently listed in `sdc_certificaten.csv` ?

# CELL ********************


# MARKDOWN ********************

#  4) Which certificate is listed the most?

# CELL ********************


# MARKDOWN ********************

#  5) Which certificates does colleague `LaSo` have?

# CELL ********************


# MARKDOWN ********************

#  6) For figuring out who doesn't have a certificate, we need to merge `sdc_personeel.csv` and `sdc_certifaten.csv`. Think carefully how you join these two. Please create the merge and assign it to a new dataframe variable. Use arguments `left_on` and `right_on` instead on `on` to specify the fields of the two tables to join on.

# CELL ********************


# MARKDOWN ********************

#  7) So we now have the merged file. Now use for example a `.groupby()` with a `.count()` to figure out who doesn't have any certificates.

# CELL ********************


# MARKDOWN ********************

#  8) In which year were the most certificates received? Do a `.groupby()` and as an extra: try to create a nice barplot of the result

# CELL ********************

