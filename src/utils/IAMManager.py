import json
import logging
from typing import Dict

import boto3

from src.config import ENDPOINT_URL

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class IAMManager:
    """Class to manage IAM roles and policies"""

    def __init__(
        self,
        endpoint_url: str = ENDPOINT_URL,
        aws_access_key_id: str = "dummy",
        aws_secret_access_key: str = "dummy",
        aws_region: str = "us-east-1",
    ):
        """
        Initialize IAM client.

        Args:
            endpoint_url (str): The endpoint URL for the S3 service.
            aws_access_key_id (str): The AWS access key ID. Default is "dummy".
            aws_secret_access_key (str): The AWS secret access key. Default is "dummy".
        """
        self.iam_client = boto3.client(
            "iam",
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region,
        )
        logging.info("iam client initialized.")

    def create_role(
        self, role_name: str, trust_policy: Dict, description: str = ""
    ) -> str:
        """
        Creates an IAM role with the given trust policy.
        args:
            role_name: str: The name of the role to create.
            trust_policy: dict: The trust policy document for the role.
        return:
            str: The ARN of the created role.
        """
        try:
            response = self.iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description=description or f"IAM Role: {role_name}",
            )
            logging.info(f"Role '{role_name}' created successfully.")
            return response["Role"]["Arn"]
        except self.iam_client.exceptions.EntityAlreadyExistsException:
            logging.info(f"Role '{role_name}' already exists.")
            return self.get_role_arn(role_name)

    def create_policy(
        self,
        policy_name: str,
        policy_document: Dict,
        description: str = "General IAM Policy",
    ) -> str:
        """Creates an IAM policy with the given permissions.
        args:
            policy_name: str: The name of the policy to create.
            policy_document: dict: The policy document with permissions.
            description: str: The description of the policy.
        return:
            str: The ARN of the created policy.
        """
        try:
            response = self.iam_client.create_policy(
                PolicyName=policy_name,
                PolicyDocument=json.dumps(policy_document),
                Description=description,
            )
            logging.info(f"Policy '{policy_name}' created successfully.")
            return response["Policy"]["Arn"]
        except self.iam_client.exceptions.EntityAlreadyExistsException:
            logging.info(f"Policy '{policy_name}' already exists.")
            return self.get_policy_arn(policy_name)

    def attach_policy_to_role(self, role_name: str, policy_arn: str) -> None:
        """Attaches an IAM policy to a role.
        args:
            role_name: str: The name of the role.
            policy_arn: str: The ARN of the policy to attach.

        """
        self.iam_client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
        logging.info(f"Policy '{policy_arn}' attached to role '{role_name}'.")

    def get_role_arn(self, role_name: str) -> str:
        """
        Retrieves the ARN of an IAM role.
        args:
            role_name: str: The name of the role.
        return:
            str: The ARN of the role
        """
        try:
            response = self.iam_client.get_role(RoleName=role_name)
        except self.iam_client.exceptions.NoSuchEntityException:
            logging.info(f"Role '{role_name}' does not exist.")
            return ""
        return response["Role"]["Arn"]

    def get_policy_arn(self, policy_name: str) -> str:
        """
        Retrieves the ARN of an IAM policy.
        args:
            policy_name: str: The name of the policy.
        return:
            str: The ARN of the policy
        """
        account_id = boto3.client("sts").get_caller_identity()["Account"]
        return f"arn:aws:iam::{account_id}:policy/{policy_name}"

    def delete_role(self, role_name: str) -> None:
        """Deletes an IAM role.
        args:
            role_name: str: The name of the role to delete.
        """
        try:
            self.iam_client.delete_role(RoleName=role_name)
            logging.info(f"Role '{role_name}' deleted successfully.")
        except Exception as e:
            logging.info(f"Error deleting role: {str(e)}")

    def delete_policy(self, policy_arn: str) -> None:
        """
        Deletes an IAM policy.
        args:
            policy_arn: str: The ARN of the policy to delete.
        """
        try:
            self.iam_client.delete_policy(PolicyArn=policy_arn)
            logging.info(f"Policy '{policy_arn}' deleted successfully.")
        except Exception as e:
            logging.info(f"Error deleting policy: {str(e)}")
