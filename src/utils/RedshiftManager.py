import logging
from typing import Any, Optional

import boto3
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s",
)


class RedshiftManager:
    """RedshiftManager class to manage Redshift operations."""

    def __init__(
        self,
        cluster_identifier: str = "redshift-cluster-1",
        database: str = "dev",
        user: str = "awsuser",
        password: str = "password",
        host: str = "localhost",
        port: int = 5439,
        aws_access_key_id: str = "dummy",
        aws_secret_access_key: str = "dummy",
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = "http://localhost:4566",
    ):
        """Initialize RedshiftManager with cluster and database information."""
        self.cluster_identifier = cluster_identifier
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None

        self.redshift_client = boto3.client(
            "redshift",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=endpoint_url,
        )

        logging.info("RedshiftManager initialized.")

    def __enter__(self):
        """Enter the runtime context related to this object."""
        try:
            logging.info(
                "Connecting to Redshift database: "
                f"{self.database} on {self.host}:{self.port}"
            )
            self.conn = psycopg2.connect(
                dbname=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )
            logging.info("Connected to Redshift successfully.")
            return self
        except Exception as e:
            logging.error(f"Failed to connect to Redshift: {e}", exc_info=True)
            raise e

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context and close the connection."""
        if self.conn:
            try:
                self.conn.close()
                logging.info("Redshift connection closed.")
            except Exception as e:
                logging.error(f"Failed to close the connection: {e}", exc_info=True)

    def execute_query(self, query: str) -> Optional[Any]:
        """
        Execute a SQL query on Redshift.
        args:
            query: str: SQL query to execute
        """
        if self.conn is None:
            logging.error("No connection to Redshift. Cannot execute query.")
            return None
        try:
            with self.conn.cursor() as cursor:
                logging.debug(f"Executing query: {query}")
                cursor.execute(query)
                self.conn.commit()
                logging.info("Query executed successfully.")
                return cursor.fetchall() if cursor.description else None
        except Exception as e:
            logging.error(f"Error executing query: {e}", exc_info=True)
            raise e

    def copy_from_s3(
        self,
        table_name: str,
        s3_path: str,
        format_as: str = "CSV",
        ignore_header: int = 1,
    ):
        """Load data from S3 into a Redshift table using the COPY command.
        args:
            table_name: str: name of the table to load data into
            s3_path: str: S3 path to the data file
            format_as: str: format of the data file (default: CSV)
            ignore_header: int: number of header lines to ignore (default: 1)
        """
        query = f"""
        COPY {table_name}
        FROM '{s3_path}'
        FORMAT AS {format_as}
        IGNOREHEADER {ignore_header};
        """
        logging.info(f"Loading data from {s3_path} into table {table_name}.")
        self.execute_query(query)
        logging.info(f"Data loaded from {s3_path} into {table_name} successfully.")
