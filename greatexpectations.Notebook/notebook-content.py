# Synapse Analytics notebook source


# MARKDOWN ********************

# # Great Expectations
# 
# Data testen in Python
# 

# CELL ********************

## Prerequisites
%pip install great_expectations

# CELL ********************

import great_expectations as gx

# MARKDOWN ********************

#  Overview
# 
# Great Expectations (gx) is a library to help you write asserts about your data.
# 
# Usage involves three steps:
# 
# 1. Create a "DataContext" object
# 2. Connect to data
# 3. Create expectations (assertions / tests)

# CELL ********************

# Create DataContext
context = gx.get_context()

# CELL ********************

# Connect to data
validator = context.sources.pandas_default.read_csv(
    'https://github.com/wortell-smart-learning/python-data-fundamentals/raw/main/data/most_voted_titles_enriched.csv'
)

# CELL ********************

# Create expectations
validator.expect_column_values_to_not_be_null('primaryTitle')
validator.expect_column_values_to_be_unique('tconst')
validator.expect_column_distinct_values_to_be_in_set('titleType', ['movie', 'tvSeries'])
validator.expect_column_values_to_be_between('startYear', 1900, 2021)

# MARKDOWN ********************

#  More information
# 
# * An overview of GX: [GX Overview](https://docs.greatexpectations.io/docs/conceptual_guides/gx_overview)
# * Types of expectations: [Expectation creation workflow](https://docs.greatexpectations.io/docs/guides/expectations/create_expectations_overview)

# CELL ********************

