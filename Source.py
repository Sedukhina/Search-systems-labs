# Stub for outer function calls

import re

from numpy import append
from LinkAnalyzer import *
from LinksScraper import *
from DB import *
from urllib.parse import urljoin

# Concurrency
import concurrent.futures

from TextProcessing import GetKeywords

from geopy.distance import geodesic

from datetime import datetime, timedelta

import pandas

config = configparser.RawConfigParser()
config.read('config.ini')
config_dict = dict(config.items('Search'))

QUERY_API = float(config_dict["query_api"])

KEYWORD_MATCH_RELEVANCE_VALUE = float(config_dict["keyword_match_relevance_value"])

HISTORICAL_KEYWORD_MATCH_RELEVANCE_VALUE = float(config_dict["historical_keyword_match_relevance_value"])

CONTEXT_SEACRH = float(config_dict["context_search_on"])

RELEVANT_DISTANCE_THRESHOLD  = float(config_dict["relevant_distance_threshold"])
MAX_DISTANCE_RELEVANCE_VALUE = float(config_dict["max_distance_relevance_value"])

STAT = config_dict["statistics"]


def Search_internal(query):
    # Dictionary contains results and relevance scores
    result_urls = []

    columns = ["url", "valid", "relevance_score"]
    stat_df = pandas.DataFrame(columns=columns)

    
    if CONTEXT_SEACRH:
        # Time context
            today = datetime.today()
            current_time = datetime.now().time()
            target_time = datetime.strptime('16:00', '%H:%M').time()
            if current_time > target_time:
                three_days_ago = today - timedelta(days=7)
                time = {'from' : three_days_ago.strftime('%Y-%m-%d')}
            else:
                four_days_ago = today - timedelta(days=8)
                time = {'from' : four_days_ago.strftime('%Y-%m-%d')}
            api_result_urls = QueryNewsAPI(query, time)
    else:
            api_result_urls = QueryNewsAPI(query)
        
    with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(CheckURLStatus, url): url for url in api_result_urls}
            for future in futures:
                url = futures[future]
                if STAT:
                    stat_df = pandas.concat([stat_df, pandas.DataFrame([{"url" : url, "valid" : 0, "relevance_score" : 0}])], ignore_index=True)
                url_id, title, description = future.result()  
                
                if url_id:  
                    result_urls.append((url_id, title, description, url))
                    AddPublicationDate(api_result_urls[url], url_id)
                    if STAT:
                        stat_df.loc[stat_df["url"] == url, "valid"] = 1  

    #
    # URLs collection finished, filtering relevant results
    #

    relevance_scores = {}
    stat = []
    # Init relevance_score 
    for url in result_urls:
        relevance_scores[url[0]] = 0

    # Extracting keywords from query
    query_keywords = GetKeywords(query)

    if CONTEXT_SEACRH:
        # History context
        history = []
        with open('search_history', 'r') as file:
                SEARCH_HISTORY_LINES_USED = 3
                for i in range(SEARCH_HISTORY_LINES_USED):
                    history_query = file.readline()
                    history.extend(GetKeywords(history_query))

    for url in result_urls:
        for keyword in query_keywords:
            # Two or more word keywords are more valuable
            keyword_len = len(keyword.split())

            keyword_id = GetKeywordID(keyword)
            if keyword_id:    
                # + relevance for keyword match
                if LinkKeywordConnectionExists(url[0], keyword_id):
                    relevance_scores[url[0]] += KEYWORD_MATCH_RELEVANCE_VALUE * keyword_len


        if CONTEXT_SEACRH:
            # History context
            for keyword in history:
                keyword_len = len(keyword.split())
                keyword_id = GetKeywordID(keyword)
                if keyword_id:    
                    if LinkKeywordConnectionExists(url[0], keyword_id):
                        relevance_scores[url[0]] += HISTORICAL_KEYWORD_MATCH_RELEVANCE_VALUE * keyword_len
                         
    # Ommiting irrelevant
    for url in result_urls:
        relevance_scores[url[0]] = relevance_scores[url[0]] * GetDomainOpenPageRank(GetDomainByLinkID(url[0]))



    if CONTEXT_SEACRH:
        # User location
        response = requests.get("https://ipinfo.io")
        data = response.json()
        # Latitude and Longitude
        user_location = data.get("loc", "")

        for url in result_urls:
            if url[0] in relevance_scores.keys():
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
       #print(url_tuple[0][3])
       print('Score:', value)
       print('Title:', url_tuple[0][1])
       print('Description:', url_tuple[0][2])
       print("")
       result.append([url_tuple[0][1], url_tuple[0][2], url_tuple[0][3]])


    stat_path = "last_search_stat.csv"
    stat_df.to_csv(stat_path, index=False)

    with open('search_history', 'a') as file:
        file.write(query + "\n")

    return result


def Search(query, context_params = {}):
    result = Search_internal(query);
    return result


CreateMainDB()
res = Search('stock market trends')
#print(GetKeywords('financial crisis'))
#url_id, title, description =CheckURLStatus('https://www.wired.com/story/buy-a-car-on-amazon-hyundai/')
#print(title)