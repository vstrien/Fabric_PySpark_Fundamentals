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

# # Exploring Covid vaccinations per country

# MARKDOWN ********************

#  How well does The Netherlands perform in comparison to other countries with Covid vaccinations?

# MARKDOWN ********************

#  Hints:
# - Use dataset `owid-covid-data.csv`
# - Use columns `location`, `date`, `total_vaccinations` and `total_vaccinations_per_hundred`
# - You can filter on multiple countries, like this: `df[df['location'].isin(['Netherlands', 'Italy'])]`
# - You can filter on date pretending it is a string like this: `df.query("date > '2020-12-01'")`

# MARKDOWN ********************

#  The data comes from this site: 
#  https://ourworldindata.org/covid-vaccinations

# MARKDOWN ********************

#  1. Read in the data
# - use dataset `owid-covid-data.csv`

# CELL ********************


# MARKDOWN ********************

#  2. Create a dataframe only use the following columns:
# `['iso_code', 'continent', 'location', 'date', 'total_vaccinations', 'total_vaccinations_per_hundred', 'population']`

# CELL ********************


# CELL ********************


# MARKDOWN ********************

#  3. Now create a subset of your data of only European countries and only dates from 1 dec 2020 on. 
#  From now only use this subset to answer the questions hereafter!!!
# - use column `continent` to get the European countries
# - You can filter on date pretending it is a string like this: `df.filter("date > '2020-12-01'")`

# CELL ********************


# CELL ********************


# MARKDOWN ********************

# From now on, use the dataframe you created in 3.
# 
# 4. What are the datatypes of the columns in the dataframe?

# CELL ********************


# MARKDOWN ********************

#  6. Which countries are in the dataset?

# CELL ********************


# MARKDOWN ********************

#  7. What is the highest number of total vaccinations in this dataset

# CELL ********************


# MARKDOWN ********************

#  8. What is the total number of vaccinations per country?
# 
# Hint: You use `.groupby()` here

# CELL ********************


# MARKDOWN ********************

#  9. What is the top 5 European countries that have the most vaccinations at the moment?

# CELL ********************


# MARKDOWN ********************

#  10. Which countries are in the top 5 when you look at the number of vaccinations per 100 people?

# CELL ********************


# MARKDOWN ********************

#  11. Create a scatter plot comparing of `total_vaccinations_per_hundred` (y-axis) per `date` (x-axis) per `location` (color)

# CELL ********************


# MARKDOWN ********************

#  12. Create the same scatter plot as in question 11, but now only showing the following countries:
# -  `['Netherlands', 'Germany', 'Belgium', 'France', 'Italy', 'Luxembourg', 'United Kingdom']`
# 
# Hint:
# - You can filter on multiple countries, like this: `df.where(df.location.isin(['Netherlands', 'Italy']))`

# CELL ********************


# MARKDOWN ********************

#  13. Create a map of the world with px.choropleth() and visualize the differences per country of `total_vaccinations_per_hundred`
# - documentation: https://plotly.github.io/plotly.py-docs/generated/plotly.express.choropleth.html
# - use arguments `locations` and `locationmode='country names'` instead of `lat` and `lon`

# CELL ********************

