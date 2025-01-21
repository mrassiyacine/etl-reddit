import logging
import os
from datetime import datetime

from src.config import BUCKET_NAME
from src.utils.S3Manager import S3Manager

logging.basicConfig(level=logging.INFO)


def upload_and_cleanup(file_path: str) -> None:
    """Upload the file to S3 and remove the local file
    args:
        file_path: str: path to the file to upload
    """
    try:
        s3_manager = S3Manager(bucket_name=BUCKET_NAME)
        s3_key = datetime.now().strftime("%y/%m/%d") + "/data.csv"
        s3_manager.upload_file_to_s3(local_file_path=file_path, s3_key=s3_key)
        os.remove(file_path)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise e
