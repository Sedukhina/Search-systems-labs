import re
import time
import requests
import dns.resolver
from urllib.parse import urlparse
from bs4 import BeautifulSoup

from LinkProcessing import GetDomainAndRelativePath
from DB import *

from geopy.geocoders import Nominatim

from TextProcessing import GetKeywords, ExtractLocations

geo = Nominatim(user_agent="tutorial", timeout=10)
geo_lock = threading.Lock()

def IsBlacklistedDomain(domain):
    try:
        domain_ip = dns.resolver.resolve(domain, 'A')[0].to_text()
        reversed_ip = '.'.join(reversed(domain_ip.split('.')))
        query = f"{reversed_ip}.zen.spamhaus.org"
        # Will throw an exception if not blacklisted
        dns.resolver.resolve(query, 'A')  
        return True
    except dns.resolver.NXDOMAIN:
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
    

def GetLocationHierarchy(location_name, retries=3):
    with geo_lock:
        for attempt in range(retries):
            settlement, state, country = None, None, None
            try:
                location = geo.geocode(location_name, language="en", namedetails=True, addressdetails=True)
                if location:
                    location = location.raw

                    if location['addresstype'] == 'country' or location['addresstype'] == 'state' or location['addresstype'] == 'city' or location['addresstype'] == 'town' or location['addresstype'] == 'village':
                        country = location['address']['country']

                        if location['addresstype'] == 'city':
                            settlement = location['address']["city"] 
                        elif location['addresstype'] == 'town':
                            settlement = location['address']["town"]
                        elif location['addresstype'] == 'village':
                            settlement = location['address']["village"]

                    if 'state' in location['address']:
                            state = location['address']['state']
                    elif location['addresstype'] != country:
                        state = settlement

                    return settlement, state, country
                else:
                    return None, None, None
            except requests.exceptions.ReadTimeout:
                print(f"Timeout occurred. Retrying... ({attempt + 1}/{retries})")
                time.sleep(2 ** attempt)  # Exponential backoff
            except requests.exceptions.RequestException as e:
                return None, None, None


def GetLocationCoords(loc):
    with geo_lock:
        if loc:
            location = geo.geocode(loc, language="en")
            if location:
                return (location.latitude, location.longitude)
        return None

# If url is valid - solves redirections and give final link back, else returns None 
def CheckURLStatus(url):

    # If url is already in db returning id
    url_id = GetLinkID(url)
    if url_id:
        title, description = GetLinkTitleAndDescription(url_id)
        return url_id, title, description

    try:
        response = requests.get(url, allow_redirects=True, timeout=10, verify=True)
        domain, rel = GetDomainAndRelativePath(response.url)

        if GetDomainOpenPageRank(domain):
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, "html.parser")
                title = soup.title.string.strip() if soup.title else None
                description_tag = soup.find("meta", attrs={"name": "description"})
                description = description_tag["content"].strip() if description_tag and "content" in description_tag.attrs else None

                link_id = InsertLink(domain, rel, title, description)

                if link_id:
                    # Adding Keywords
                    keywords = []
                    meta_keywords = soup.find("meta", attrs={"name": "keywords"})
                    if meta_keywords and meta_keywords.get("content"):
                        keywords = meta_keywords["content"]
                        keywords = [keyword.strip() for keyword in keywords.split(",")]
                    if title:    
                        keywords.extend(GetKeywords(title))
                    if description:
                        keywords.extend(GetKeywords(description))
                    for keyword in keywords:
                            keyword = keyword.lower()
                            keyword_id = InsertKeyword(keyword)
                            InsertLinkKeyword(link_id, keyword_id)
                    # Adding location to link connections
                    doc = ""
                    if title:
                        doc += title
                    if description:
                        doc += description
                    locations = ExtractLocations(doc)
                    for loc in locations:
                        settlement, state, country = GetLocationHierarchy(loc)
                        settlement_coords = GetLocationCoords(settlement)
                        state_coords = GetLocationCoords(state)
                        country_coords = GetLocationCoords(country)
                        AddLocationConnection(link_id, settlement, state, country, settlement_coords, state_coords, country_coords)

                    return link_id, title, description 

    except requests.RequestException as e:
        pass
        #print(f"Error: Unable to fetch URL - {e}")
    return None, None, None

