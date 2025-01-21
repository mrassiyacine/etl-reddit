import logging
from typing import Any, Dict, List

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
        region_name: str = "us-east-1",
    ):
        """
        Initialize S3 client.

        Args:
            bucket_name (str): The name of the bucket.
            endpoint_url (str): The endpoint URL for the S3 service.
            aws_access_key_id (str): The AWS access key ID. Default is "dummy".
            aws_secret_access_key (str): The AWS secret access key. Default is "dummy"
            region_name (str): The region name. Default is "us-east-1".
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
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
                f"File at {local_file_path} uploaded to "
                f"s3://{self.bucket_name}/{s3_key} successfully."
            )
        except Exception as e:
            logging.error(f"Error uploading file to S3: {e}")
            raise e

    def tag_s3_object(self, s3_key: str) -> None:
        """Tag an S3 object with the given tags.
        args:
            s3_key: str: The key of the S3 object to tag.
        """
        try:
            self.s3_client.put_object_tagging(
                Bucket=self.bucket_name,
                Key=s3_key,
                Tagging={
                    "TagSet": [
                        {"Key": "status", "Value": "loaded"},
                    ]
                },
            )
            logging.info(f"Tagged object {s3_key} in bucket {self.bucket_name}.")
        except Exception as e:
            logging.error(f"Failed to tag object {s3_key}: {e}")
            raise e

    def list_all_keys(self, status_filter: str = "all") -> List[str]:
        """
        List all object keys in the S3 bucket.

        Args:
            status_filter (str): 'all' to get all keys,
                                'not_loaded' to get not loaded files.

        Returns:
            List[str]: A list of all object keys in the bucket.
        """
        keys = []
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            logging.info(
                f"Listing all keys in bucket"
                f"{self.bucket_name} with filter: {status_filter}"
            )

            for page in paginator.paginate(Bucket=self.bucket_name):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        s3_uri = f"s3://{self.bucket_name}/{obj['Key']}"

                        tags = self.s3_client.get_object_tagging(
                            Bucket=self.bucket_name, Key=obj["Key"]
                        )
                        is_loaded = any(
                            tag["Key"] == "status" and tag["Value"] == "loaded"
                            for tag in tags["TagSet"]
                        )
                        if status_filter == "not_loaded":
                            if not is_loaded:
                                keys.append(s3_uri)
                            else:
                                continue
                        else:
                            keys.append(s3_uri)

        except Exception as e:
            logging.error(f"Failed to list keys in bucket {self.bucket_name}: {e}")
            raise e
        return keys
