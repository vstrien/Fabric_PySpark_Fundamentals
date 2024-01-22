# Synapse Analytics notebook source


# CELL ********************

import requests
from bs4 import BeautifulSoup

# Send a GET request to the product page
url = 'https://www.lego.com/en-us/product/10981'
response = requests.get(url)

# Parse the HTML content using Beautiful Soup
soup = BeautifulSoup(response.content, 'html.parser')

# Extract the availability status of the product
availability = soup.find('meta', {'property': 'product:availability'}).text.strip()
# <meta property="product:availability" content="in stock"/>
# Check if the product is available for sale
if 'out of stock' in availability.lower():
    print('The product is currently out of stock.')
else:
    print('The product is available for sale.')

# CELL ********************

availability = soup.find('meta', {'property': 'product:availability'})

# CELL ********************

boodschappen = [ "apples", "bananas", "bread", "cookies", "milk", "grapes" ]

# CELL ********************

# Reverse the text of each of the items in 'boodschappen'
for item in boodschappen:
    print(item[::-1])

# CELL ********************

import requests
from bs4 import BeautifulSoup

# Send a GET request to the product page
url = 'https://www.lego.com/en-us/product/10981'
response = requests.get(url)

# Parse the HTML content using Beautiful Soup
soup = BeautifulSoup(response.content, 'html.parser')

# Extract the availability status of the product
availability = soup.find('meta', {'property': 'product:availability'})

# Check if the product is available for sale
if availability['content'] == 'in stock':
    print('The product is available for sale.')
else:
    print('The product is currently out of stock.')

# CELL ********************

# Find the tag 'meta' with the property='product:availability'


# CELL ********************

availability['content']

# CELL ********************

