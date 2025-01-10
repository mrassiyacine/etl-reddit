import logging
import os
from datetime import datetime

import pandas as pd
from praw import Reddit

logging.basicConfig(level=logging.INFO)


DATA_FOLDER = "src/etl/data/"


def connect_to_reddit(client_id: str, client_secret: str, user_agent: str) -> Reddit:
    """Connect to Reddit API using PRAW
    args:
        client_id: str: client id of the Reddit API
        client_secret: str: client secret of the Reddit API
        user_agent: str: user agent of the Reddit API
    return: praw.Reddit: Reddit object
    """
    try:
        reddit = Reddit(
            client_id=client_id, client_secret=client_secret, user_agent=user_agent
        )
        logging.info("Connected to Reddit !")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise e
    return reddit


def extract_data(
    reddit: Reddit, subreddits: list[str], limit: int = 20, time_filter: str = "week"
) -> list:
    """Get data from multiple subreddits and return as a list of posts
    args:
        reddit: praw.Reddit: Reddit object
        subreddits: list[str]: list of subreddit names
        limit: int: number of posts to fetch from each subreddit
        time_filter: str: time filter for top posts
    return: list: list of posts
    """
    all_posts = []
    try:
        for subreddit in subreddits:
            posts = []
            for post in reddit.subreddit(subreddit).top(
                time_filter=time_filter, limit=limit
            ):
                posts.append(
                    {
                        "title": post.title,
                        "id": post.id,
                        "subreddit": str(post.subreddit),
                        "score": post.score,
                        "num_comments": post.num_comments,
                        "url": post.url,
                        "created": post.created,
                    }
                )
            all_posts.extend(posts)
            logging.info(f"Successfully fetched {len(posts)} posts from {subreddit}")
    except Exception as e:
        logging.error(f"An error occurred while fetching data from subreddits: {e}")
        raise e
    return all_posts


def transform_load_data(posts: list, file_folder: str = DATA_FOLDER) -> None:
    """Transform list of posts into a pandas DataFrame
    args:
        posts: list: list of posts
    return: pandas.DataFrame: DataFrame of posts
    """
    try:
        os.makedirs(DATA_FOLDER, exist_ok=True)
        df = pd.DataFrame(posts)
        df["created"] = pd.to_datetime(df["created"], unit="s")
        current_date = datetime.now().strftime("%Y-%m-%d")
        file_path = file_folder + f"{current_date}.csv"
        df.to_csv(file_path, index=False)
        logging.info("Successfully transformed and loaded data locally")
    except Exception as e:
        logging.error(f"An error occurred while transforming/loading data: {e}")
        raise e
