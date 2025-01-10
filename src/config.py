import os

from dotenv import load_dotenv

load_dotenv()

ENDPOINT_URL = "http://localhost:4566"
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "")
BUCKET_NAME = os.getenv("BUCKET_NAME", "")
