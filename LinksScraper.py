import requests
from bs4 import BeautifulSoup
from datetime import datetime
import configparser

config = configparser.RawConfigParser()
config.read('private-config.ini')
config_dict = dict(config.items('General'))
news_api_key= config_dict["news_api_key"]
news_api_base_url = 'https://newsapi.org/v2/everything'


# Returns all links found on page 
def ScrapePage(link):
    response = requests.get(link)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        links = [
                a['href'] for a in soup.find_all('a', href=True)
                if not a['href'].startswith('#')           # Exclude `#` or `#content`
                #and a['href'] != '/'                       # Exclude root path `/`
                and not a['href'].startswith('javascript:')  # Exclude JavaScript links
            ]
        return links
    else:
        return []
    

def QueryNewsAPI(query, additional_params = {}):
    query_params = {
    'q': query,
    'apiKey': news_api_key,
    'sortBy': 'relevancy'
    }
    
    query_params = query_params | additional_params

    response = requests.get(news_api_base_url, params=query_params)
    if response.status_code == 200:
        data = response.json()
        articles = data.get('articles', [])
        urls = {article['url']: datetime.strptime(article['publishedAt'], "%Y-%m-%dT%H:%M:%SZ").date() for article in articles}
        return urls
    else:
        print("NewsAPI access error. Error code: ", response.status_code)
        return []