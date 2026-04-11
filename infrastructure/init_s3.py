import os
import boto3
import logging
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "retail-data-lake")
FILE_NAME = "sales.csv"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FILE_PATH = os.path.join(BASE_DIR, "data", FILE_NAME)
S3_KEY = f"raw-data/{FILE_NAME}"
s3_client = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT", "http://localstack:4566"),
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

def upload_to_datalake():
    if not os.path.isfile(FILE_PATH):
        logger.error(f"❌ File not found at path: {FILE_PATH}")
        return

    try:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        logger.info(f"✅ Bucket '{BUCKET_NAME}' created or already exists.")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        if error_code in ['BucketAlreadyExists', 'BucketAlreadyOwnedByYou']:
            logger.info(f"✅ Bucket '{BUCKET_NAME}' already exists.")
        else:
            logger.error(f"❌ Error creating bucket: {e}")
            return

    logger.info(f"⏳ Uploading {FILE_NAME} to s3://{BUCKET_NAME}/{S3_KEY}...")
    try:
        s3_client.upload_file(FILE_PATH, BUCKET_NAME, S3_KEY)
        logger.info("🚀 Upload completed successfully!")
    except ClientError as e:
        logger.error(f"❌ Failed to upload file: {e}")

if __name__ == "__main__":
    upload_to_datalake()