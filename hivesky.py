import csv
from datetime import datetime
from enum import Enum
import os
import sys

from atproto import Client, client_utils, models
from bs4 import BeautifulSoup
import feedparser
import requests

BEEHIVE_FULL_RSS_FEED = "https://www.beehive.govt.nz/rss.xml"
START_TIME = datetime.strptime("05 Apr 2025 00:00:01 +1300", "%d %b %Y %H:%M:%S %z")

class PostType(Enum):
    RELEASE = 1
    SPEECH = 2
    FEATURE = 3

class Minister:
    def __init__(self, name: str, slug: str, portfolios: list[str] = None):
        self.name = name
        self.slug = slug
        self.portfolios = portfolios if portfolios is not None else []

    def add_portfolio(self, portfolio: str):
        self.portfolios.append(portfolio)
    
    def __str__(self):
        return f"{self.name}, Minister of {self.portfolio}"

    def __repr__(self):
        return self.__str__()

class Post:
    def __init__(self, type: PostType, guid: str, url: str, title: str, ministers: list[Minister] = None):
        self.type = type
        self.guid = guid
        self.url = url
        self.title = title
        self.ministers = ministers if ministers is not None else []
    
    def add_minister(self, minister: Minister):
        self.ministers.append(minister)
    
    def update_title(self, title: str):
        self.title = title
    
    def __str__(self):
        return f"{self.type}: {self.title}"

    def __repr__(self):
        return self.__str__()

def scrape_url(url):
    # Unfortunately, RSS feeds for most government sites are behind an Imperva
    # WAF so this is currently using a Browserless instance that I host to get
    # around the WAF and fetch the latest RSS feed. Trying to do a request
    # curl/requests fetch results in an Imperva response. It should be possible
    # to run a local Browserless container but historically I haven't had any
    # luck doing that.
    browserless_api_token = os.environ.get("BROWSERLESS_API_TOKEN", False)
    if not browserless_api_token:
        print("Please set BROWSERLESS_API_TOKEN env var")
        sys.exit(1)

    scrape_url = os.environ.get("BROWSERLESS_URL", False)
    if not scrape_url:
        print("Please set BROWSERLESS_URL env var")
        sys.exit(1)

    scrape_params = {"token": browserless_api_token, "stealth": True}

    return requests.post(scrape_url, params=scrape_params, json={"url": url})

def fetch_post_metadata(post):
    r = scrape_url(post.url)
    if not r.ok:
        return False
    soup = BeautifulSoup(r.text, 'html.parser')
    # TODO: There is more complexity to actually map ministers to their correct portfolios which will
    # be implemented later. This will do for now.
    metadata = {'title': None, 'description': None, 'ministers': [], 'portfolios': []}

    # Page metadata
    title = soup.find("meta", attrs={"property": "og:title"}).attrs.get('content', '').strip()
    if title == "":
        title = soup.find('h1', class_='article__title').text.strip()
    metadata['title'] = title

    description = soup.find("meta", attrs={"property": "og:description"}).attrs.get('content', '').strip()
    if description == "":
        description = soup.find('meta', attrs={"name": "description"}).attrs.get('content', '').strip()
    metadata['description'] = description

    # Ministers
    ministers = soup.find_all('div', class_='minister__title')
    for minister in ministers:
        metadata['ministers'].append(minister.text.strip())
    # portfolios = soup.find_all('div', class_='taxonomy-term--type-portfolios')
    # for portfolio in portfolios:
    #     metadata['portfolios'].append(portfolio.text.strip())

    return metadata

def format_minister_text(ministers):
    """
    1 Minister: "A new release from X"
    2 Ministers: "A new release from X and Y"
    3+ Ministers: "A new release from X, Y and Z"
    """
    prefix = ' from'
    if len(ministers) == 0:
        return '' # shouldn't be possible normally
    elif len(ministers) == 1:
        return f'{prefix} {ministers[0]}'
    elif len(ministers) == 2:
        ministers = ' and '.join(ministers)
        return f'{prefix} {ministers}'
    else:
        firstBit = ', '.join(ministers[:-1])
        return f'{prefix} {firstBit} and {ministers[-1]}'

def fetch_remote_rss_feed():
    r = scrape_url(BEEHIVE_FULL_RSS_FEED)
    if not r.ok:
        print(f"Received {r.status_code} status code from Browserless")
        sys.exit(1)
    return feedparser.parse(r.text)

def fetch_local_rss_feed():
    with open("example.xml", "r") as file:
        data = file.read()
    return feedparser.parse(data)

def load_feed_history():
    if os.path.exists("history.csv"):
        with open("history.csv") as csvfile:
            return list(csv.DictReader(csvfile))
    return []

def save_feed_history(history, post):
    history.append({
        'guid': post.guid,
        'url': post.url
    })
    with open("history.csv", "w", newline="") as csvfile:
        fieldnames = ['guid', 'url']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in history:
            writer.writerow(row)

def retrieve_published_guids(history):
    guids = []
    for row in history:
        guids.append(row['guid'])
    return guids

def parse_entry(entry):
    url = entry.link
    # NOTE: GUIDs are shaped like https://www.beehive.govt.nz/124729 but to "visit" them, the URL
    # is https://www.beehive.govt.nz/node/124729 which resolves into the canonical URL. Some GUIDs
    # will appear to skip numbers. PDFs and other uploads that don't appear in the RSS feed are
    # allocated a node number as well.
    guid = entry.guid
    title = entry.title.strip()
    entry_type = None

    if '/feature/' in url:
        entry_type = PostType.FEATURE
    if '/release/' in url:
        entry_type = PostType.RELEASE
    if '/speech/' in url:
        entry_type = PostType.SPEECH
    if entry_type is None:
        print("No idea what this entry is!")
        return None

    return Post(entry_type, guid, url, title)

if __name__ == "__main__":
    feed = fetch_local_rss_feed()
    history = load_feed_history()
    guids = retrieve_published_guids(history)
    # Feed items are not 100% strictly time ordered but it's possible for feeds
    # to be backdated so we won't bother with ordering too much. Despite that,
    # we'll still reverse the order so "older" items are published first
    posts = []
    for entry in reversed(feed.entries[3:]):
        parsed_datetime = datetime.strptime(entry.published, "%a, %d %b %Y %H:%M:%S %z")
        # When bootstrapping the feed, we don't want to publish items that are too old.
        if parsed_datetime < START_TIME:
            continue
        # If post is already published, skip parsing it
        if entry.guid in guids:
            continue
        post = parse_entry(entry)
        if post is not None and post.guid not in history:
            # We've never seen this post before so we'll fetch further data about it
            metadata = fetch_post_metadata(post)
            tb = client_utils.TextBuilder()
            
            tb.text('A new ')
            if len(metadata['ministers']) > 1:
                tb.text('joint ')
            if post.type == PostType.RELEASE:
                tb.text('release')
            if post.type == PostType.FEATURE:
                tb.text('feature')
            if post.type == PostType.SPEECH:
                tb.text('speech')
            tb.text(' is available')
            if len(metadata['ministers']):
                tb.text(format_minister_text(metadata['ministers']))
            # if len(metadata['portfolios']):
            #     tb.text(f', Minister for {metadata["portfolios"][0]}')
            tb.text('.')

            embed_title = metadata['title'] if metadata['title'] is not None else post.title
            embed_description = metadata['description'] if metadata['description'] is not None else 'Read more'

            POST_TO_BLUESKY = os.environ.get('POST_TO_BLUESKY', False)

            # By default, we will simply output resolved content to make debugging easier. In order to make a real
            # post, you will need to set `POST_TO_BLUESKY=True` as an env var
            if POST_TO_BLUESKY:
                client = Client()
                client.login(
                    os.environ.get("BLUESKY_USERNAME", False),
                    os.environ.get("BLUESKY_PASSWORD", False)
                )

                # In order to avoid wasted bandwidth, as the image is always the same, we'll just upload
                # a local copy and update it periodically.
                with open('beehive.png', 'rb') as file:
                    img_data = file.read()
                thumb = client.upload_blob(img_data)
                embed = models.AppBskyEmbedExternal.Main(
                    external=models.AppBskyEmbedExternal.External(
                        title=embed_title,
                        description=embed_description,
                        uri=post.url,
                        thumb=thumb.blob,
                    )
                )
                try:
                    client.send_post(tb, embed=embed)
                    save_feed_history(history, post)
                    print(f"Successfully posted {post.url}")
                except Exception:
                    # We failed to publish a post presumably so we don't want to save to history
                    continue
            else:
                print(tb.build_text())
                print('----')
                print(embed_title)
                print(embed_description)
                print(post.url)
                print('----')
                save_feed_history(history, post)