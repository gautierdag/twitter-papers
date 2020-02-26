from datetime import datetime
import os
from typing import Optional, List
import configparser

import pandas as pd
import requests
from tqdm import tqdm

from bs4 import BeautifulSoup
from pydantic import BaseModel, validator
import tweepy

config = configparser.ConfigParser()
config.read("settings.ini")

CACHE_FOLDER = "cache"
HISTORY_CSV = "history.csv"


class Tweet(BaseModel):
    """Parsed Favorited Tweet Class"""

    id: int
    text: str
    created_at: datetime
    url: Optional[str]

    @validator("url", always=True, pre=True)
    def parse_url(cls, v):
        for url_object in v:
            full_url = url_object["expanded_url"]
            if "arxiv" in full_url:
                return full_url
        return None


def get_cached_history() -> pd.DataFrame:
    # create cache folder if non-existent
    if not os.path.exists(CACHE_FOLDER):
        os.makedirs(CACHE_FOLDER)

    # load cached tweets if exists
    if os.path.exists(f"{CACHE_FOLDER}/{HISTORY_CSV}"):
        tweets = pd.read_csv(f"{CACHE_FOLDER}/{HISTORY_CSV}")
        return tweets

    return pd.DataFrame()


def save_cache_history(tweet_history: pd.DataFrame) -> None:
    # load cached tweets if exists
    tweet_history.to_csv(f"{CACHE_FOLDER}/{HISTORY_CSV}", index=False)


def parse_tweet(tweet: tweepy.Status) -> Tweet:
    return Tweet(
        id=tweet.id,
        text=tweet.text,
        created_at=tweet.created_at,
        url=tweet.entities["urls"],
    )


def get_recent_tweets() -> List[Tweet]:
    max_tweets = int(config["twitter"]["max_tweets"])

    auth = tweepy.OAuthHandler(
        config["twitter"]["consumer_key"], config["twitter"]["consumer_secret"]
    )
    auth.set_access_token(
        config["twitter"]["access_token"], config["twitter"]["access_token_secret"]
    )
    api = tweepy.API(auth)
    tweets = []
    for fav in tqdm(tweepy.Cursor(api.favorites).items(max_tweets)):
        tweets.append(parse_tweet(fav))
    return tweets


def get_arvix_title(arvix_link: str) -> str:
    response = requests.get(arvix_link)
    soup = BeautifulSoup(response.text, "html.parser")
    header_title = soup.find("title").text.split("]")[-1].strip()
    return header_title


def download_arvix_pdf(arvix_link: str) -> bool:
    print(f"Downloading link at: {arvix_link}")

    if "pdf" in arvix_link:
        arvix_link = arvix_link.replace(".pdf", "").replace("pdf", "abs")

    # get title of pdf
    title = get_arvix_title(arvix_link)

    # get file path to ibooks folder from settings
    pdf_file_path = f"{config['pdf']['pdf_folder_path']}/{title}.pdf"

    # stream download pdf
    pdf_dl_link = arvix_link.replace("abs", "pdf")
    response = requests.get(pdf_dl_link, stream=True)
    if response.status_code == 200:
        with open(pdf_file_path, "wb") as f:
            for chunk in response:
                f.write(chunk)
        return True
    else:
        return False


def get_full_tweet_history() -> pd.DataFrame:
    recent_tweets = get_recent_tweets()
    recent_tweet_history = pd.DataFrame(map(dict, recent_tweets))

    # automatically process tweets without URL
    recent_tweet_history["processed"] = recent_tweet_history.url.isnull()

    cached_tweet_history = get_cached_history()
    if len(cached_tweet_history) > 0:
        new_tweets = recent_tweet_history.loc[
            ~recent_tweet_history.id.isin(cached_tweet_history.id)
        ].copy()

        print(f"Found {len(new_tweets)} new tweets.")

        return pd.concat([cached_tweet_history, new_tweets])
    else:
        return recent_tweet_history


def process_tweet_history(tweet_history: pd.DataFrame):
    print("Processing tweet history")

    for _, row in tweet_history[~tweet_history.processed].iterrows():
        if not row.processed:
            url = row.url
            success = download_arvix_pdf(url)
            if success:
                tweet_history.loc[row.id == tweet_history.id, "processed"] = True

    print("Saving updated history to cache")
    save_cache_history(tweet_history)


if __name__ == "__main__":
    tweet_history = get_full_tweet_history()
    process_tweet_history(tweet_history)
