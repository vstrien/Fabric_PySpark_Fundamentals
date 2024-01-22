# Synapse Analytics notebook source


# MARKDOWN ********************

#  Movie scraping script: enrich my basic movie data with some extra info by scraping IMDB
#  Extra movie info such as country, language, budget etc.
#  The scraping is done with beautiful soup

# CELL ********************

import pandas as pd

import requests
from bs4 import BeautifulSoup
import re

import pickle
import os

pd.options.display.max_colwidth = 50
pd.options.display.max_columns = 50

# CELL ********************

df = pd.read_csv('most_voted_titles.csv')

# MARKDOWN ********************

#  Function to get and parse html page

# CELL ********************

def get_soup_of_html_page(url):
    """
    Takes a url and does a request to get the html page.
    The html pages gets processed into a beautiful soup object.
    """
    html_page = requests.get(url).text
    soup = BeautifulSoup(html_page, 'html.parser')
    return soup

# MARKDOWN ********************

#  Small functions to save (and load) intermediate scraping results

# CELL ********************

def save_obj(obj, name):
    with open(name, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
        
def load_obj(name):
    with open(name, 'rb') as f:
        return pickle.load(f)

# MARKDOWN ********************

#  Function to pick out interesting info from a movie page, such as country, language, budget etc.

# CELL ********************

def get_title_details(title_id):
    """Uses a title_id from imdb to scrape the page of that movie and return
    details of that movie such as the summary text, country, language,
    metascore etc."""
    
    soup_page = get_soup_of_html_page(f'https://www.imdb.com/title/{title_id}/')

    try:
        metascore = soup_page.find('div', class_='titleReviewBar').select_one("a[href*=criticreviews]").text.strip()
    except: 
        metascore = ''
    
    try:
        image_url = soup_page.find('img')['src']
    except:
        image_url = ''
        
    try:
        summary = soup_page.find('div', class_='summary_text').text.strip()
    except:
        summary = ''

    try:
        country = soup_page.find('div', id='titleDetails').select_one("a[href*=country]").text.strip()
    except:
        country = ''
        
    try:
        primary_language = soup_page.find('div', id='titleDetails').select_one("a[href*=language]").text.strip()
    except:
        primary_language = ''
        
    try:
        color = soup_page.find('div', id='titleDetails').select_one("a[href*=color]").text.strip()
    except:
        color = ''

        
    try:
        tagline_rawtext = soup_page.find('div', id="titleStoryLine").find('div', class_='txt-block').text
        if 'Taglines:' in tagline_rawtext:
            tagline = re.sub('.*(See more.*)', '', tagline_rawtext).replace('Taglines:', '').strip()
        else:
            tagline = ''
    except:
        tagline = ''
            

    try:
        tagline_rawtext = soup_page.find('div', id="titleDetails").text #.replace('\n', '')
        
        if 'Budget:' in tagline_rawtext:
            budget_text = re.sub('.*Budget:','', tagline_rawtext, flags=re.DOTALL)
            budget_text = re.sub('\n.*', '', budget_text, flags=re.DOTALL).strip()
        else:
            budget_text = ''
            
        if 'Cumulative Worldwide Gross:' in tagline_rawtext:
            cumulative_text = re.sub('.*Cumulative Worldwide Gross:','', tagline_rawtext, flags=re.DOTALL)
            cumulative_text = re.sub('\n.*', '', cumulative_text, flags=re.DOTALL).strip()
            
        else:
            cumulative_text = ''
            
        if 'Opening Weekend USA:' in tagline_rawtext:
            opening_weekend_usa = re.sub('.*Opening Weekend USA:', '', tagline_rawtext, flags=re.DOTALL)
            opening_weekend_usa = re.sub('\n.*', '', opening_weekend_usa, flags=re.DOTALL).strip()
        else:
            opening_weekend_usa = ''
            
        if 'Gross USA:' in tagline_rawtext:
            gross_usa = re.sub('.*Gross USA:','', tagline_rawtext, flags=re.DOTALL)
            gross_usa = re.sub('\n.*', '', gross_usa, flags=re.DOTALL).strip()
        else:
            gross_usa = ''
        
    except:
        budget_text = ''
        cumulative_text = ''
        opening_weekend_usa = ''
        gross_usa = ''
    
    
    
    title_details =  {
        'tconst': title_id,
        'metascore': metascore,
        'country': country,
        'primary_language': primary_language,
        'color': color,
        'budget': budget_text,
        'opening_weekend_usa': opening_weekend_usa,
        'gross_usa': gross_usa,
        'cumulative_worldwide': cumulative_text,
        'tagline': tagline,
        'summary': summary,
        'image_url': image_url,
    }
    
    return title_details

# MARKDOWN ********************

#  Start scraping and save every 100 movies scraped to a dictionary

# CELL ********************

result_dict = {}

for number, title_id in enumerate(df['tconst']):
    result_dict[number] = get_title_details(title_id)
    
    if number%100==0 and number != 0:
        print(number)
        save_obj(result_dict, f'imdb_{str(number).zfill(7)}_scrape.pkl')
        result_dict = {}

print(number)
save_obj(result_dict, f'imdb_{str(number).zfill(7)}_scrape.pkl')

# MARKDOWN ********************

#  Put all intermediate results together again in 1 dataframe

# CELL ********************

pickle_files = [file_name for file_name in os.listdir() if 'pkl' in file_name]

pickled_dfs = [pd.DataFrame.from_dict(load_obj(file_name), orient='index') for file_name in pickle_files]

df_all_movies_scraped = pd.concat(pickled_dfs).sort_index()

df_all_movies_scraped.tail(3)

# CELL ********************

df_all_movies_scraped.shape

# CELL ********************

df.shape

# MARKDOWN ********************

#  Merge the original dataframe with the extra info we scraped

# CELL ********************

df_merged = df.merge(df_all_movies_scraped, on='tconst', how='inner')

# CELL ********************

df_merged.head(3)

# CELL ********************

df_merged['budget'].value_counts(dropna=False)

# MARKDOWN ********************

#  Write the results to a new datafile

# CELL ********************

df_merged.to_csv('most_voted_titles_enriched.csv', header=True, index=False)
