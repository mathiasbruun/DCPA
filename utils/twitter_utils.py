import requests
from bs4 import BeautifulSoup
import re
import json
import pandas as pd
from loguru import logger
from tqdm.notebook import tqdm

import tweepy
from tweepy import Unauthorized, NotFound, Forbidden
from .tweepy_credentials import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True)

def scrape_politician_handles(url = 'https://web.archive.org/web/20220328101917/https://filip.sdu.dk/twitter/politikere/'):
    '''
    Scrapes a web page (default is "twitterpolitikere.dk"),
    extracting Twitter handles from URLs. Returns a list
    of unique handles found.
    
    -----
    url (str): website to scrape -- default is
        web archive of twitterpolitikere at
        "https://web.archive.org/web/20220328101917/
        https://filip.sdu.dk/twitter/politikere/"
    '''
    
    logger.add("logs/politician_handle_scrape_{time}.log")
    
    logger.info(f'Scraping twitter handles...')
    
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    links = soup.find_all('a')

    urls = []

    for link in links:

        try:
            urls.append(link['href'])

        except KeyError:
            logger.debug(f'Link retrieval info: No href found for {link}')

    twitter_url_regex = re.compile(r'^https:\/\/web.archive.org\/web\/20220328101917\/https:\/\/twitter\.com\/[A-zæøå1-9]+')
    extract_username_regex = r'^https:\/\/web.archive.org\/web\/20220328101917\/https?:\/\/(?:www\.)?twitter\.com\/(?:#!\/)?@?([^\/?#]*)(?:[?#].*)?$'

    politician_urls = [url for url in urls if twitter_url_regex.match(url) and len(url) < 80]
    usernames = list(set(re.split(extract_username_regex, url)[1] for url in politician_urls))

    logger.info(f'All done. {len(usernames)} Twitter handles retrieved.')
    
    return(usernames)

def load_handles_from_txt(txt_file):
    '''
    Load handles from .txt file and prepend @ for each.
    '''
    
    with open(txt_file, 'r') as f:
        handles = ['@' + handle for handle in f.read().split('\n')]
        
        return handles

def collect_single_user_tweets(screen_name, since_id = None):
    '''
    TODO...
    '''
    
    tweets = [
        tweet._json for tweet in tweepy.Cursor(
            api.user_timeline,
            screen_name = screen_name,
            include_rts = True,
            tweet_mode = 'extended',
            since_id = since_id
        ).items(3200)
    ]
    
    return(tweets)

def collect_all_user_tweets(twitter_handle_list):
    '''
    TODO...
    '''
    logger.add("logs/twitter_collection_{time}.log")
    
    all_tweets = []
    
    logger.info('Beginning collection...')
    
    for handle in tqdm(twitter_handle_list):
        
        try:
            tweets = collect_single_user_tweets(handle)
            all_tweets.extend([tweet for tweet in tweets])
            
        except (Unauthorized, Forbidden, NotFound) as error:
            logger.debug(f'{error}: Unable to collect tweets for {handle}')
    
    logger.info(f'Collection completed for {len(twitter_handle_list)} twitter handles!')
    
    return(all_tweets)

def construct_tweet_df(all_tweets):
    '''
    TODO...
    '''
    
    cols_to_extract = [
        'screen_name',
        'name',
        'id_str',
        'created_at',
        'full_text'
    ]

    df_tweets = pd.DataFrame([[
            tweet['user'][cols_to_extract[0]],
            tweet['user'][cols_to_extract[1]],
            tweet[cols_to_extract[2]],
            tweet[cols_to_extract[3]],
            tweet[cols_to_extract[4]]
        ] for tweet in all_tweets], columns = cols_to_extract
    )

    df_tweets['created_at'] = pd.to_datetime(df_tweets['created_at'])
    
    return(df_tweets)

def get_most_recent_tweet_id(df, screen_name):
    '''
    TODO...
    '''
    
    df_filtered = df.loc[df['screen_name'] == screen_name]
    
    most_recent_timestamp = df_filtered['created_at'].max()

    most_recent_id = df_filtered.loc[
        df_filtered['created_at'] == most_recent_timestamp,
        'id_str'
    ].iloc[0]
    
    return(most_recent_id)

def get_new_user_tweets(df_tweets):
    '''
    TODO...
    '''
    logger.add("logs/twitter_update_attempt_{time}.log")
    
    all_tweets = []
    
    logger.info('Looking for new tweets since last extraction...')
    
    unique_handles = df_tweets['screen_name'].unique()
    
    for handle in tqdm(unique_handles):
        
        most_recent_id = get_most_recent_tweet_id(df_tweets, handle)
        
        try:
            tweets = collect_single_user_tweets(handle, since_id = most_recent_id)
            all_tweets.extend([tweet for tweet in tweets])
            
        except (Unauthorized, Forbidden, NotFound) as error:
            logger.debug(f'{error}: Unable to collect tweets for {handle}')
    
    logger.info(f'Collection completed for {len(unique_handles)} twitter handles!')
    
    return(all_tweets)