import logging
from typing import Any, Dict

import boto3

from src.config import ENDPOINT_URL

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def create_bucket_if_not_exist(bucket_name: str) -> None:
    """Create a bucket in S3 if it does not already exist.
    args:
        bucket_name: str: name of the bucket
    return: None
    """
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id="dummy",
            aws_secret_access_key="dummy",
        )
        existing_buckets = s3_client.list_buckets()
        if bucket_name not in [
            bucket["Name"] for bucket in existing_buckets["Buckets"]
        ]:
            s3_client.create_bucket(Bucket=bucket_name)
            logging.info(f"Bucket {bucket_name} created.")
        else:
            logging.info(f"Bucket {bucket_name} already exists.")
    except boto3.exceptions.Boto3Error as e:
        logging.error(f"Failed to create bucket {bucket_name}: {e}")
        raise e


def configure_bucket_versioning(
    bucket_name: str, versioning_config: Dict[str, str]
) -> Dict[Any, Any]:
    """Configure versioning for a bucket in S3
    args:
        bucket_name: str: name of the bucket
        versioning_config: Dict[str, str]: versioning configuration
    return: Dict[Any, Any]: response from S3
    """
    s3_client = boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="dummy",
        aws_secret_access_key="dummy",
    )

    response = s3_client.put_bucket_versioning(
        Bucket=bucket_name, VersioningConfiguration=versioning_config
    )
    logging.info(f"Bucket {bucket_name} versioning configuration: {versioning_config}")

    return response


def upload_file_to_s3(local_file_path: str, bucket_name: str, s3_key: str) -> None:
    """Uploads a local file to S3 using boto3

    Args:
        local_file_path (str): Local file path
        bucket_name (str): Bucket name
        s3_key (str): Key (Path) in S3 to save the file
    """
    s3_client = boto3.client(
        "s3", aws_access_key_id="dummy", aws_secret_access_key="dummy"
    )
    try:
        s3_client.upload_file(Filename=local_file_path, Bucket=bucket_name, Key=s3_key)
        logging.info(
            f"File at {local_file_path} has been uploaded \
                to s3://{bucket_name}/{s3_key} successfully."
        )
    except Exception as e:
        logging.error(f"Error uploading file to S3: {e}")
        raise e
