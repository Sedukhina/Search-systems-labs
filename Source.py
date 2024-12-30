# Stub for outer function calls

import re

from numpy import append
from LinkAnalyzer import *
from LinksScraper import *
from DB import *
from urllib.parse import urljoin

# Concurrency
import concurrent.futures

from yake import KeywordExtractor
from geopy.distance import geodesic

import pandas

config = configparser.RawConfigParser()
config.read('config.ini')
config_dict = dict(config.items('Search'))

QUERY_API = float(config_dict["query_api"])
SCRAPE_CUSTOM = float(config_dict["scrape_custom"])
SCRAPE_FROM_CUSTOM = float(config_dict["scrape_from_custom"])

KEYWORD_MATCH_RELEVANCE_VALUE = float(config_dict["keyword_match_relevance_value"])
KEYWORD_IN_TITLE_RELEVANCE_VALUE = float(config_dict["keyword_in_title_relevance_value"])
KEYWORD_IN_DESCRIPTION_RELEVANCE_VALUE = float(config_dict["keyword_in_description_relevance_value"])

HISTORICAL_KEYWORD_MATCH_RELEVANCE_VALUE = float(config_dict["historical_keyword_match_relevance_value"])
HISTORICAL_KEYWORD_IN_TITLE_RELEVANCE_VALUE = float(config_dict["historical_keyword_in_title_relevance_value"])
HISTORICAL_KEYWORD_IN_DESCRIPTION_RELEVANCE_VALUE = float(config_dict["historical_keyword_in_description_relevance_value"])

CONTEXT_SEACRH = float(config_dict["context_search_on"])

RELEVANT_DISTANCE_THRESHOLD  = float(config_dict["relevant_distance_threshold"])
MAX_DISTANCE_RELEVANCE_VALUE = float(config_dict["max_distance_relevance_value"])

STAT = config_dict["statistics"]

def Search_internal(query, context_params = {}):
    # Dictionary contains results and relevance scores
    result_urls = []

    columns = ["url", "valid", "real_relevance_score", "title_match", "keywords_match", "description_match"]
    stat_df = pandas.DataFrame(columns=columns)

    if not CONTEXT_SEACRH:
        context_params = {}

    if QUERY_API:
        # Getting query results via news api
        if 'time' in context_params:
            api_result_urls = QueryNewsAPI(query, context_params['time'])
        else:
            api_result_urls = QueryNewsAPI(query)
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(CheckURLStatus, url): url for url in api_result_urls}
            for future in futures:
                url = futures[future]
                if STAT:
                    stat_df = pandas.concat([stat_df, pandas.DataFrame([{"url" : url, "valid" : 0, "real_relevance_score" : 0, "title_match" : 0, "keywords_match" : 0, "description_match" : 0}])], ignore_index=True)
                url_id, title, description = future.result()  
                
                if url_id:  
                    result_urls.append((url_id, title, description, url))
                    AddPublicationDate(api_result_urls[url], url_id)
                    if STAT:
                        stat_df.loc[stat_df["url"] == url, "valid"] = 1

    if SCRAPE_FROM_CUSTOM:
        # Scraping from custom urls
        with open('CustomURLsToScrapeFrom.txt', 'r') as file:
            custom_urls = [line.strip() for line in file.readlines()]

            for base_url in custom_urls:
                urls = ScrapePage(base_url);
                for url in urls:
                    url = urljoin(base_url, url)
                    if STAT:
                        stat_df = pandas.concat([stat_df, pandas.DataFrame([{"url" : url, "valid" : 0, "real_relevance_score" : 0, "title_match" : 0, "keywords_match" : 0, "description_match" : 0}])], ignore_index=True)
                    url_id, title, description = CheckURLStatus(url)
                    if url_id:
                        result_urls.append((url_id, title, description, url))
                        if STAT:
                            stat_df.loc[stat_df["url"] == url, "valid"] = True

    if SCRAPE_CUSTOM:
        # Scraping custom urls
        with open('CustomURLsToSearchIn.txt', 'r') as file:
            custom_urls = [line.strip() for line in file.readlines()]

            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {executor.submit(CheckURLStatus, url): url for url in custom_urls}
                for future in futures:
                    url_id, title, description = future.result()  
                    url = futures[future]
                    if STAT:
                        stat_df = pandas.concat([stat_df, pandas.DataFrame([{"url" : url, "valid" : 0, "real_relevance_score" : 0, "title_match" : 0, "keywords_match" : 0, "description_match" : 0}])], ignore_index=True)
                    if url_id:  
                        result_urls.append((url_id, title, description, url))
                        if STAT:
                            stat_df.loc[stat_df["url"] == url, "valid"] = True

    #
    # URLs collection finished, filtering relevant results
    #

    relevance_scores = {}
    stat = []
    # Init relevance_score for each url as open page rank
    for url in result_urls:
        relevance_scores[url[0]] = 0

    # Extracting keywords from query
    kw_extractor = KeywordExtractor()
    query_keywords = kw_extractor.extract_keywords(query)
    if query_keywords:
        query_keywords = list(zip(*query_keywords))[0]
    else:
        query_keywords = query

    for url in result_urls:

        title_keywords = []
        if url[1]:  
            title_keywords = kw_extractor.extract_keywords(url[1])
            title_keywords = list(zip(*title_keywords))[0]

        description_keywords = []
        if url[2]:  
            description_keywords = kw_extractor.extract_keywords(url[2])
            if description_keywords:
                description_keywords = list(zip(*description_keywords))[0]
                

        for keyword in query_keywords:
            # Two or more word keywords are more valuable
            keyword_len = len(keyword.split())

            keyword_id = GetKeywordID(keyword)
            if keyword_id:    
                # + relevance for keyword match
                if LinkKeywordConnectionExists(url[0], keyword_id):
                    relevance_scores[url[0]] += KEYWORD_MATCH_RELEVANCE_VALUE * keyword_len
                    if STAT:
                        stat_df.loc[stat_df["url"] == url[3], "keywords_match"] = 1
            
            # + relevance for keywords in title
            if keyword in title_keywords:
                relevance_scores[url[0]] += KEYWORD_IN_TITLE_RELEVANCE_VALUE * keyword_len
                if STAT:
                    stat_df.loc[stat_df["url"] == url[3], "title_match"] = 1
                
            # + relevance for keywords in description        
            if keyword in description_keywords:  
                 relevance_scores[url[0]] += KEYWORD_IN_DESCRIPTION_RELEVANCE_VALUE * keyword_len
                 if STAT:
                    stat_df.loc[stat_df["url"] == url[3], "description_match"] = 1


        if CONTEXT_SEACRH:
            history = []
            kw_extractor = KeywordExtractor()
            with open('search_history', 'r') as file:
                SEARCH_HISTORY_LINES_USED = 3
                for i in range(SEARCH_HISTORY_LINES_USED):
                    query= file.readline()
                    query_keywords = kw_extractor.extract_keywords(query)
                    if query_keywords:
                        query_keywords = list(zip(*query_keywords))[0]
                    else:
                        query_keywords = query
                    history.extend(query_keywords)

            for keyword in history:
                # Two or more word keywords are more valuable
                keyword_len = len(keyword.split())

                keyword_id = GetKeywordID(keyword)
                if keyword_id:    
                    # + relevance for keyword match
                    if LinkKeywordConnectionExists(url[0], keyword_id):
                        relevance_scores[url[0]] += HISTORICAL_KEYWORD_MATCH_RELEVANCE_VALUE * keyword_len
            
                # + relevance for keywords in title
                if keyword in title_keywords:
                    relevance_scores[url[0]] += HISTORICAL_KEYWORD_IN_TITLE_RELEVANCE_VALUE * keyword_len
                
                # + relevance for keywords in description        
                if keyword in description_keywords:  
                     relevance_scores[url[0]] += HISTORICAL_KEYWORD_IN_DESCRIPTION_RELEVANCE_VALUE * keyword_len
                  

    for url in result_urls:
        relevance_scores[url[0]] = relevance_scores[url[0]] * GetDomainOpenPageRank(GetDomainByLinkID(url[0]))
        
    with open("search_history", "a") as file:
        file.write(query + "\n")
    
    # Deleting irrelevant results
    relevance_scores = {key: value for key, value in relevance_scores.items() if value != 0}

    if CONTEXT_SEACRH:
        # User location
        response = requests.get("https://ipinfo.io")
        data = response.json()
        # Latitude and Longitude
        user_location = data.get("loc", "")

        for url in result_urls:
            # Use closest of locations mentioned in article to measure distance
            url_coords = GetURLCoords(url[0]) 
            if url_coords:
                # Max distance between 2 settlements
                min_dist = 20000
                for coords in url_coords:
                    min_dist = min(min_dist, geodesic(user_location, coords).kilometers)
                # Relevant distance threshold
                if min_dist < RELEVANT_DISTANCE_THRESHOLD:
                    relevance_scores[url[0]] += (1 - min_dist/RELEVANT_DISTANCE_THRESHOLD) * MAX_DISTANCE_RELEVANCE_VALUE
    
    
    result = []
    relevance_scores = dict(sorted(relevance_scores.items(), key=lambda item: item[1], reverse=True))
    for key, value in relevance_scores.items():
       url_tuple = [tup for tup in result_urls if tup[0] == key]
       if STAT:
            stat_df.loc[stat_df["url"] == url_tuple[0][3], "relevance_score"] = value
       print(url_tuple[0][3])
       print(value)
       result.append([url_tuple[0][1], url_tuple[0][2], url_tuple[0][3]])


    stat_path = "last_search_stat.csv"
    stat_df.to_csv(stat_path, index=False)

    return result


def Search(query, context_params = {}):
    result = Search_internal(query);
    return result


CreateMainDB()
context_params = {
    'time' : {
        'to' : '2024-11-15',
        'from' : '2024-11-28'
        },

    'location' : 'loc'
}
res = Search('natural disaster')

#url_id, title, description =CheckURLStatus('https://www.wired.com/story/buy-a-car-on-amazon-hyundai/')
#print(title)