import requests
from bs4 import BeautifulSoup
import json

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
    print(data)
    # json_rows = json.dumps(data)
    
    return data