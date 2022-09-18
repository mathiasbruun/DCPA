import requests
from bs4 import BeautifulSoup
import re

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
    
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    links = soup.find_all('a')

    urls = []

    for link in links:

        try:
            urls.append(link['href'])

        except KeyError:
            print(f'Link retrieval info: No href found for {link}')

    twitter_url_regex = re.compile(r'^https:\/\/web.archive.org\/web\/20220328101917\/https:\/\/twitter\.com\/[A-zæøå1-9]+')
    extract_username_regex = r'^https:\/\/web.archive.org\/web\/20220328101917\/https?:\/\/(?:www\.)?twitter\.com\/(?:#!\/)?@?([^\/?#]*)(?:[?#].*)?$'

    politician_urls = [url for url in urls if twitter_url_regex.match(url) and len(url) < 80]
    usernames = list(set(re.split(extract_username_regex, url)[1] for url in politician_urls))

    print(f'\nAll done. {len(usernames)} Twitter handles retrieved.\n\nPreview:')
    print('\n'.join(usernames[:10]))
    
    return(usernames)