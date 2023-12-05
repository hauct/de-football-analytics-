import requests
from bs4 import BeautifulSoup
import json
from geopy.geocoders import Nominatim
import pandas as pd

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
    
def get_lat_long(country, city):
    geolocator = Nominatim(user_agent='hauct_Test_geopy')
    location = geolocator.geocode(f'{city}, {country}')

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
    data = pd.DataFrame(data)


    file_name = ('stadium_cleaned_' + str(datetime.now().date())
                    + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')

    data.to_csv('data/' + file_name, index=False)
