import logging
from typing import Any, Dict

import boto3

from src.config import ENDPOINT_URL

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class S3Manager:
    """S3Manager class to manage S3 operations."""

    def __init__(
        self,
        bucket_name: str,
        endpoint_url: str = ENDPOINT_URL,
        aws_access_key_id: str = "dummy",
        aws_secret_access_key: str = "dummy",
    ):
        """
        Initialize S3 client.

        Args:
            bucket_name (str): The name of the bucket.
            endpoint_url (str): The endpoint URL for the S3 service.
            aws_access_key_id (str): The AWS access key ID. Default is "dummy".
            aws_secret_access_key (str): The AWS secret access key. Default is "dummy".
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        logging.info("S3 client initialized.")

    def create_bucket_if_not_exist(self) -> None:
        """
        Create a bucket if it doesn't exist.
        """
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logging.info(f"Bucket {self.bucket_name} already exists.")
        except self.s3_client.exceptions.ClientError:
            logging.info(f"Bucket {self.bucket_name} does not exist. Creating...")
            self.s3_client.create_bucket(Bucket=self.bucket_name)
            logging.info(f"Bucket {self.bucket_name} created successfully.")

    def configure_bucket_versioning(
        self, versioning_config: Dict[str, str]
    ) -> Dict[Any, Any]:
        """
        Configure versioning for a bucket.

        Args:
            versioning_config (Dict[str, str]): The versioning configuration.

        Returns:
            Dict[Any, Any]: The response from the S3 service.
        """
        try:
            response = self.s3_client.put_bucket_versioning(
                Bucket=self.bucket_name, VersioningConfiguration=versioning_config
            )
            logging.info(
                f"Bucket {self.bucket_name} versioning configured: {versioning_config}"
            )
            return response
        except Exception as e:
            logging.error(
                f"Failed to configure versioning for bucket {self.bucket_name}: {e}"
            )
            raise e

    def upload_file_to_s3(self, local_file_path: str, s3_key: str) -> None:
        """
        Upload a local file to S3.

        Args:
            local_file_path (str): The path to the local file to upload.
            s3_key (str): The S3 key for the uploaded file.
        """
        try:
            self.s3_client.upload_file(
                Filename=local_file_path, Bucket=self.bucket_name, Key=s3_key
            )
            logging.info(
                f"File at {local_file_path} uploaded to"
                f"s3://{self.bucket_name}/{s3_key} successfully."
            )
        except Exception as e:
            logging.error(f"Error uploading file to S3: {e}")
            raise e
