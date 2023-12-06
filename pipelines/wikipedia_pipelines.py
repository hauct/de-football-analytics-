import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
from geopy.geocoders import Nominatim
from retrying import retry
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
from time import time

def get_wikipedia_page(url):
    print(f'Getting wikipedia page...{url}')
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status() # Check if request is successful
        
        return response.text
    except requests.RequestException as e:
        print(f'An error has occured: {e}')
        
def get_wikipedia_data(html):
    soup = BeautifulSoup(html, features='html.parser')
    table = soup.find_all(name='table', attrs={'class':'wikitable sortable'})[0]
    rows = table.find_all('tr')
    return rows

def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    if text.find('[') != -1:
        text = text.split('[')[0]
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]
    return text.replace('\n', '')

def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)
    
    data = []
    
    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images': tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else 'NO IMAGE',
            'home_team': clean_text(tds[6].text)
        } 
        data.append(values)
    
    json_rows = json.dumps(data)
    
    kwargs['ti'].xcom_push(key='rows', value=json_rows)
    
    return 'Ok'
    
@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
def get_lat_long(city, country):
    geolocator = Nominatim(user_agent='new_test_hauct')
    location = geolocator.geocode(f'{city}, {country}', timeout=10)

    if location:
        return location.latitude, location.longitude

    return None

def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')
    data = json.loads(data)

    stadiums_df = pd.DataFrame(data)
    stadiums_df['city'] = stadiums_df['city'].str.split(',').str[0].str.split('.').str[0]
    stadiums_df['location'] = stadiums_df.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    stadiums_df['images'] = stadiums_df['images'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else 'NO_IMAGE')
    stadiums_df['capacity'] = stadiums_df['capacity'].str.replace(',','').str.replace('.','').astype(int)

    # push to xcom
    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())
    return "OK"

def write_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')
    data = json.loads(data)
    stadiums_df = pd.DataFrame(data)


    file_name = ('stadium_cleaned_' + str(datetime.now().date())
                    + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')

    stadiums_df.to_csv('data/' + file_name, index=False)

def ingest_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')
    data = json.loads(data)
    stadiums_df = pd.DataFrame(data)

    # Hyper parameters
    user = 'airflow'
    password = 'airflow'
    host = 'de-football-analytics--postgres-1'
    port = '5432'
    db = 'airflow'
    table = 'stadium'

    # Create engine to connect to pg db    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Ingest to PostgreSQL database
    chunk_size = 100
    for start in range(0, len(stadiums_df), chunk_size):
        t_start = time()

        stadiums_df_chunk = stadiums_df[start:start + chunk_size]
        stadiums_df_chunk.to_sql(table, engine, if_exists='append', index=False)

        t_end = time()

        print('Inserted chunk, took %.3f second' % (t_end-t_start))
        
    print('Finished ingesting data into the postgres database')

