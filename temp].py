from datetime import datetime, timezone
import json
import logging
import time
import yaml
from google.cloud import storage
# import pandas as pd
# from pyspark.sql import SparkSession


# GCS configs
GCS_BUCKET = "retailer-project"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/retailer-db/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/retailer-db/archive"
CONFIG_FILE_PATH = "configs/retailer_config.yaml"

GCP_SQL_INSTANCE_IP = "34.118.8.188"


BQ_PROJECT = "project-e6877a1b-1921-4a15-95c"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.meta.audit_logs"
BQ_LOG_TABLE = f"{BQ_PROJECT}.meta.pipeline_logs"


logging.Formatter.converter = time.gmtime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)sZ - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)

logger = logging.getLogger("RetailerPostgresToLanding")


# spark = SparkSession.builder.appName("RetailerPostgesToLanding").getOrCreate()


jdbc_url = f"jdbc:postgresql://{GCP_SQL_INSTANCE_IP}:5432/retailer-db"
props = {"user": "myuser", "password": "mypass", "driver": "org.postgresql.Driver"}

# df = spark.read.jdbc(url=jdbc_url, table="public.products", properties=props)


storage_client = storage.Client()
# bq_client = bigquery.Client()

# Logging mechanism
# log_entries = [] #Stores logs before writing to GCP

# def log_event(event_type, message, table=None):
    # log_entry = {
    #     "timestamp": datetime.now(timezone.utc).isoformat(),
    #     "event_type": event_type,
    #     "message": message,
    #     "table": table
    # }
    # log_entries.append(log_entry)

    # logger.info(f"{'my event'} - {'my message'} - {'my_table'}")


def read_config_file() -> dict:
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(CONFIG_FILE_PATH)
    content = blob.download_as_text()
    logger.info("Successfully read config file")
    return yaml.safe_load(content)


configs = read_config_file()

for table_data in configs['tables']:

