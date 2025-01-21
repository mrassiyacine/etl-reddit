#!/usr/bin/env python3
import glob
import json
import logging
import os

from src.config import (
    BUCKET_NAME,
    REDDIT_CLIENT_ID,
    REDDIT_CLIENT_SECRET,
    REDDIT_USER_AGENT,
)
from src.etl.get_reddit_data import connect_to_reddit, extract_data, transform_load_data

# from src.etl.load_data_to_redshift import load_data_to_redshift
from src.etl.upload_data_to_s3 import upload_and_cleanup
from src.utils.IAMManager import IAMManager
from src.utils.RedshiftManager import RedshiftManager
from src.utils.S3Manager import S3Manager

DATA_FOLDER = "src/etl/data/"


def setup_iam() -> str:
    """Setup IAM roles and policies
    return:
        str: The ARN of the created role.
    """
    iam_manager = IAMManager()
    with open("src/iam_config.json", "r") as f:
        iam_config = json.load(f)

    role_name = iam_config["roles"][0]["role_name"]

    response = iam_manager.get_role_arn(role_name=role_name)
    if response:
        logging.info(f"Role '{role_name}' already exists.")
        return response

    logging.info("Creating IAM role and policy...")

    role_arn = iam_manager.create_role(
        role_name=iam_config["roles"][0]["role_name"],
        trust_policy=iam_config["roles"][0]["trust_policy"],
        description=iam_config["roles"][0]["description"],
    )
    policy_arn = iam_manager.create_policy(
        policy_name=iam_config["policies"][0]["policy_name"],
        policy_document=iam_config["policies"][0]["policy_document"],
        description=iam_config["policies"][0]["description"],
    )
    iam_manager.attach_policy_to_role(iam_config["roles"][0]["role_name"], policy_arn)
    logging.info("IAM setup completed.")
    return role_arn


def setup_infrastructure():
    """Setup infrastructure for the ETL process"""
    try:
        if not os.path.exists(DATA_FOLDER):
            os.makedirs(DATA_FOLDER)
        s3_manager = S3Manager(bucket_name=BUCKET_NAME)
        s3_manager.create_bucket_if_not_exist()
        redshift = RedshiftManager()
        redshift.create_cluster_if_not_exist()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise e
    logging.info("Infrastructure setup completed.")


def main() -> None:
    """Main function to get data from Reddit, upload to S3 and load to Redshift"""
    reddit = connect_to_reddit(
        REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT
    )
    subreddits = ["dataengineering", "datascience"]
    posts = extract_data(reddit=reddit, subreddits=subreddits)
    transform_load_data(posts=posts, file_folder=DATA_FOLDER)
    files = glob.glob(DATA_FOLDER + "*")
    for file in files:
        upload_and_cleanup(file_path=file)
    arn = setup_iam()
    logging.info(arn)
    # load_data_to_redshift(arn=arn)
    logging.info("ETL process completed.")


if __name__ == "__main__":
    setup_infrastructure()
    main()
