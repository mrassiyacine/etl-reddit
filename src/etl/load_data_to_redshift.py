from src.config import BUCKET_NAME
from src.utils.RedshiftManager import RedshiftManager
from src.utils.S3Manager import S3Manager


def load_data_to_redshift(arn: str) -> None:
    """Load data from S3 to Redshift
    args:
        file_path: str: path to the file to upload
    """
    s3_manager = S3Manager(bucket_name=BUCKET_NAME)
    with RedshiftManager() as redshift:
        query_create_table = """ CREATE TABLE IF NOT EXISTS posts (
            title VARCHAR,
            id VARCHAR,
            subreddit VARCHAR,
            score INT,
            num_comments INT,
            url VARCHAR,
            created TIMESTAMP
            );"""
        redshift.execute_query(query=query_create_table)

        paths = s3_manager.list_all_keys(status_filter="not loaded")

        for path in paths:
            redshift.copy_from_s3(table_name="posts", s3_path=path, iam_role=arn)
            s3_manager.tag_s3_object(s3_key=path)
