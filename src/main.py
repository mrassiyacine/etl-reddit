#!/usr/bin/env python3
import glob
import os

from etl.get_reddit_data import connect_to_reddit, extract_data, transform_load_data
from src.config import REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT
from utils import s3_utils as s3


def main() -> None:
    """Main function to get data from Reddit and upload to S3"""
    reddit = connect_to_reddit(
        REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT
    )
    posts = extract_data(reddit, ["dataengineering"])
    transform_load_data(posts=posts, file_folder="src/etl/data")

    s3.create_bucket_if_not_exist("reddit-data")
    s3.configure_bucket_versioning("reddit-data", {"Status": "Enabled"})

    local_data_folder = "src/etl/data/"
    for file_path in glob.glob(os.path.join(local_data_folder, "*.csv")):
        file_name = os.path.basename(file_path)
        s3.upload_file_to_s3(
            local_file_path=file_path, bucket_name="reddit-data", s3_key=file_name
        )


if __name__ == "__main__":
    main()
