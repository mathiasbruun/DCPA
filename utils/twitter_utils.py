import requests
from bs4 import BeautifulSoup
import re
import json
import numpy as np
import pandas as pd
from loguru import logger
from tqdm.notebook import tqdm
import pickle

import tweepy
from tweepy import Unauthorized, NotFound, Forbidden, TweepyException
from .tweepy_credentials import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True, timeout=120, retry_count=10, retry_delay=1)

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

def divide_chunks(l, n):
    
    for i in range(0, len(l), n):
        yield l[i:i + n]

def collect_all_user_tweets(twitter_handle_list):
    '''
    TODO...
    '''
    logger.add("logs/twitter_collection_{time}.log")
    
    total_chunks = np.ceil(len(twitter_handle_list)/100)
    logger.info(f'Dividing handles into {total_chunks} chunk(s)...')
    
    handle_chunks = divide_chunks(twitter_handle_list, 100)

    logger.info('Beginning collection...')
    
    chunk_no = 1
    
    for chunk in handle_chunks:
        
        chunk_tweets = []
        logger.info(f'--- CHUNK NO. {chunk_no} ---')
        
        for handle in tqdm(chunk):

            try:
                tweets = collect_single_user_tweets(handle)
                chunk_tweets.extend([tweet for tweet in tweets])

            except (Unauthorized, NotFound, Forbidden, TweepyException) as ex:
                logger.error(f'{ex}! Unable to collect tweets for {handle}')
        
        with open(f'data/raw/twitter/chunks/tweet_chunk_{chunk_no}.p', 'wb') as p:
            pickle.dump(chunk_tweets, p)
        
        chunk_no += 1
    
    logger.info(f'Collection completed for {len(twitter_handle_list)} twitter handles!')

def construct_tweet_df(all_tweets):
    '''
    TODO...
    '''
    
    cols_to_extract = [
        'screen_name',
        'name',
        'description',
        'followers_count',
        'id_str',
        'created_at',
        'full_text'
    ]

    df_tweets = pd.DataFrame([[
            tweet['user'][cols_to_extract[0]],
            tweet['user'][cols_to_extract[1]],
            tweet['user'][cols_to_extract[2]],
            tweet['user'][cols_to_extract[3]],
            tweet[cols_to_extract[4]],
            tweet[cols_to_extract[5]],
            tweet[cols_to_extract[6]]
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
            
        except Exception as ex:
            ex_name = type(ex).__name__
            logger.error(f'{ex_name}! Unable to collect tweets for {handle}')
    
    logger.info(f'Collection completed for {len(unique_handles)} twitter handles!')
    
    return(all_tweets)

# Custom data loader here

def tweet_chunks2df(chunks_dir = 'data/raw/twitter/chunks/', no_chunks = 8):
    '''
    TODO...
    '''
    
    df_list = []
    
    for chunk_no in tqdm(range(1, no_chunks+1)):
        
        with open(f'{chunks_dir}tweet_chunk_{chunk_no}.p', 'rb') as p:
            
            chunk_tweets = pickle.load(p)
            df_tweets = construct_tweet_df(chunk_tweets)
            df_list.append(df_tweets)
            
    df_all_tweets = pd.concat(df_list)
    
    return(df_all_tweets) 