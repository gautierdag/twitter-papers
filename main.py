import configparser
import os
import pickle
from typing import Set

from bs4 import BeautifulSoup
import requests
import tweepy

config = configparser.ConfigParser()
config.read("settings.ini")

# cache settings
CACHE_FOLDER = config["cache"]["cache_folder"]
CACHE_FILE = config["cache"]["cache_file"]
CACHE_PATH = f"{CACHE_FOLDER}/{CACHE_FILE}"

# Max number of latest tweets to retrieve per run
MAX_TWEETS = int(config["twitter"]["max_tweets"])

# Set up the twitter api client using the environment variables from config
TWITTER_AUTH = tweepy.OAuthHandler(
    config["twitter"]["consumer_key"], config["twitter"]["consumer_secret"]
)
TWITTER_AUTH.set_access_token(
    config["twitter"]["access_token"], config["twitter"]["access_token_secret"]
)
TWITTER_API = tweepy.API(TWITTER_AUTH)


def read_cache() -> Set[str]:
    # load cached tweets if exists
    if os.path.exists(CACHE_PATH):
        # Return set of already processed urls
        tweets = pickle.load(open(CACHE_PATH, "rb"))
        return tweets
    return set()


def save_cache(processed_tweets: Set[str]):
    # create cache folder if non-existent
    print("Saving cache")
    if not os.path.exists(CACHE_FOLDER):
        os.makedirs(CACHE_FOLDER)
    pickle.dump(processed_tweets, open(CACHE_PATH, "wb"))


def parse_urls(tweet: tweepy.Status) -> Set[str]:
    """
    Parse all the arvix URLs from a tweet object.
    Returns:
        urls (Set[str]): set containing all the urls
    """
    urls = set()
    for url_object in tweet.entities["urls"]:
        full_url = url_object["expanded_url"]

        if "arxiv" in full_url:
            # parse case when tweet already links to pdf link
            if "pdf" in full_url:
                full_url = full_url.replace(".pdf", "").replace("pdf", "abs")

            urls.add(full_url)

    return urls


def get_tweets() -> Set[str]:
    """
    Gets the tweets using tweepy calls
    Args:
        max_tweets (int): the number of latest favorited tweets to retrieve

    Returns set of unique urls from liked tweets containing `arvix`
    """
    tweets = set()
    for liked_tweet in tweepy.Cursor(TWITTER_API.favorites).items(MAX_TWEETS):
        tweets |= parse_urls(liked_tweet)
    return tweets


def get_arvix_title(arvix_link: str) -> str:
    """
    Makes a request to the abstract page of the paper to parse the title of the article
    """
    assert "abs" in arvix_link

    response = requests.get(arvix_link)
    soup = BeautifulSoup(response.text, "html.parser")
    header_title = soup.find("title").text.split("]")[-1].strip()
    return header_title


def download_arvix_pdf(arvix_link: str) -> bool:
    """
    Downloads the arvix link (example https://arxiv.org/abs/2002.12345)
    to pdf in the directory specified by config.
    First makes a requests to the /abs/ url to get title, then to the pdf
    url to get the file. Finally saves the file to location using the title as name.
    """

    print(f"Downloading link at: {arvix_link}")

    # get title of pdf
    title = get_arvix_title(arvix_link)

    # get file path to local folder from settings
    pdf_file_path = f"{config['pdf']['pdf_folder_path']}/{title}.pdf"

    # stream download pdf to desired location
    pdf_dl_link = arvix_link.replace("abs", "pdf")
    response = requests.get(pdf_dl_link, stream=True)
    if response.status_code == 200:
        with open(pdf_file_path, "wb") as f:
            for chunk in response:
                f.write(chunk)
        return True
    else:
        return False


def main() -> None:
    # gets the set of arvix links from recently liked tweets
    recent_tweet_set = get_tweets()
    # gets set of arvix links previously processed
    cached_tweet_set = read_cache()

    # Difference between new set and cached set are tweets to download for
    new_tweets = recent_tweet_set - cached_tweet_set
    print(f"Found {len(new_tweets)} new tweets.")
    for new_tweet in new_tweets:
        success = download_arvix_pdf(new_tweet)

        # if tweet was downloaded sucessfully
        if success:
            cached_tweet_set.add(new_tweet)
        else:
            print(f"Error downloading {new_tweet}")

    # save the new set to cache
    save_cache(cached_tweet_set)


if __name__ == "__main__":
    main()
