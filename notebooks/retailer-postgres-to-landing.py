from datetime import datetime, timezone
import json
import logging
import time
from runpy import run_path

import yaml
from pathlib import Path
from google.cloud import storage, bigquery
from pyspark.sql import SparkSession, functions as F
from pyspark import StorageLevel
# from data.INGESTION.supplierMysqlToLanding import spark

# GCS configs
GCS_BUCKET = "retailer-project"
LANDING_PATH = f"landing/retailer-db"
ARCHIVE_PATH = f"landing/retailer-db/archive"
CONFIG_FILE_PATH = "configs/retailer_config.yaml"

BQ_PROJECT = "project-e6877a1b-1921-4a15-95c"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.meta.audit_logs"
BQ_LOG_TABLE = f"{BQ_PROJECT}.meta.pipeline_logs"
POSTGRES_SCHEMA = 'retailer'


logging.Formatter.converter = time.gmtime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)sZ - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)

logger = logging.getLogger("RetailerPostgresToLanding")


spark = SparkSession.builder.appName("RetailerPostgesToLanding").getOrCreate()

POSTGRES_CONFIG = {
    'url': "jdbc:postgresql://34.118.8.188:5432/retailer-db",
    "user": "myuser",
    "password": "mypass",
    "driver": "org.postgresql.Driver"
}
# jdbc_url = f"jdbc:postgresql://34.118.8.188:5432/retailer-db"
# props = {"user": "myuser", "password": "mypass", "driver": "org.postgresql.Driver"}


storage_client = storage.Client()
bq_client = bigquery.Client()


def read_config_file() -> dict:
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(CONFIG_FILE_PATH)
    content = blob.download_as_text()
    logger.info("Successfully read config file")
    return yaml.safe_load(content)


# def extract_date_from_filename(filename: str) -> datetime:
#     name = Path(filename).stem
#     date_str = name.rsplit("_", 1)[-1]
#     return datetime.strptime(date_str, "%d%m%Y")


# def archive_folder_contents(table_name: str):
#     bucket = storage_client.bucket(GCS_BUCKET)
#     prefix = f"{LANDING_PATH}{table_name}/"
#     blobs = storage_client.list_blobs(GCS_BUCKET, prefix=prefix)
#
#     found_file = False
#
#     for blob in blobs:
#         if blob.name.endswith("/"):
#             continue  # skip folder placeholders
#
#         found_file = True
#
#         filename = Path(blob.name).name
#
#         try:
#             dt = extract_date_from_filename(filename)
#         except ValueError as e:
#             raise ValueError(f"Bad filename in landing: {blob.name}") from e
#
#         new_name = ARCHIVE_PATH + f"{dt.year}/{dt.month:02d}/{dt.day:02d}/" + filename
#         bucket.copy_blob(blob, bucket, new_name)
#         blob.delete()
#         logger.info(f"Archived file {filename} to {new_name}")
#
#     if not found_file:
#         logger.info(f"No files found in table {table_name} in {prefix}")

# def delete_directory_markers(bucket_name: str, prefix: str):
#     bucket = storage_client.bucket(bucket_name)
#
#     # Directory markers are typically blobs that end with '/'
#     markers = [
#         b for b in storage_client.list_blobs(bucket, prefix=prefix)
#         if b.name.endswith("/") and b.size == 0
#     ]
#
#     for b in markers:
#         b.delete()
#         print("deleted marker:", b.name)

def archive_table_data(table_name: str):
    """
    Copy all objects from LANDING_PATH + table_name to ARCHIVE_PATH within the same bucket.
    """
    bucket = storage_client.bucket(GCS_BUCKET)

    src_prefix = f"{LANDING_PATH.strip('/')}/{table_name}/"

    blobs = [blob for blob in storage_client.list_blobs(bucket, prefix=src_prefix) if blob.name.endswith('.png')]

    if not blobs:
        logger.info(f"No objects found under prefix: gs://{GCS_BUCKET.strip('/')}/{src_prefix}")

    for blob in blobs:
        # Preserve relative path inside the prefix
        rel_name = blob.name[len(src_prefix):]

        new_name = f"{ARCHIVE_PATH.strip('/')}/{rel_name}"
        bucket.copy_blob(blob, bucket, new_name)
        blob.delete()



# print(archive_prefix('products'))


def get_latest_watermark(table_name) -> datetime:
    query = f"""
        SELECT MAX(watermark) as latest_watermark
        FROM {BQ_AUDIT_TABLE}
        WHERE table_name = '{table_name}'
    """
    query_job = bq_client.query(query)  # start job
    result = query_job.result()
    row = next(result, None)

    return (
        row.latest_watermark
        if row and row.latest_watermark
        else datetime(1900, 1, 1, tzinfo=timezone.utc)
    )


def extract_and_save_to_landing(table_name, load_type: str, watermark_col = None):
    if load_type == 'incremental' and watermark_col:
        latest_watermark = get_latest_watermark(table_name)
        latest_watermark_str = latest_watermark.strftime("%Y-%m-%d %H:%M:%S.%f")
        query = f"""
            SELECT *
            FROM {POSTGRES_SCHEMA}.{table_name}
            WHERE {watermark_col} > TIMESTAMP "{latest_watermark_str}"
        """
    else:
        query = f"""
            SELECT *
            FROM {POSTGRES_SCHEMA}.{table_name}
    #     """

    df = (
        spark.read
        .format("jdbc")
        .options("url", POSTGRES_CONFIG['url'])
        .options("user", POSTGRES_CONFIG['user'])
        .options("password", POSTGRES_CONFIG['password'])
        .options("driver", POSTGRES_CONFIG['driver'])
        .options("dbtable", query)
        .load()
    )

    now_utc = datetime.now(timezone.utc)
    run_id = now_utc.strftime("%Y%m%dT%H%M%SZ")
    load_date_str = now_utc.strftime("%Y-%m-%d")

    df_to_write = (
        df
        .withColumn("load_date", F.lit(load_date_str).cast("date"))
        .withColumn("run_id", F.lit(run_id))
    )

    landing_prefix = f"gs://{GCS_BUCKET}/{LANDING_PATH.strip('/')}/{table_name}"

    (
        df_to_write
        .write
        .mode('append')
        .PartitionBy("load_date", "run_id")
        .json(landing_prefix)
    )

    logger.info(f"Successfully extracted data from {POSTGRES_SCHEMA}.{table_name}")

    return {
        "landing_prefix": landing_prefix,
        "load_date": load_date_str,
        "run_id": run_id
    }


def get_written_stats(run_info: dict, watermark_col: str | None) -> dict:
    run_path = f"{run_info['landing_prefix']}/load_date={run_info['load_date']}/run_id={run_info['run_id']}/"
    df_written =  spark.read.json(run_path).persist(StorageLevel.MEMORY_AND_DISK)

    rows_loaded = df_written.count()

    max_watermark = None
    if watermark_col and rows_loaded:
        max_watermark = (
            df_written
            .withColumn(watermark_col, F.to_timestamp(F.col(watermark_col), "yyyy-MM-dd HH:mm:ss.SSSSSS"))
            .agg(F.max(F.col(watermark_col)).alias("max_wm"))
            .collect()[0]["max_wm"]
        )

    return {
        "rows_loaded": rows_loaded,
        "max_watermark": max_watermark,
        "run_path": run_path
    }


def insert_audit_row(
    bq_table: str,
    *,
    run_id: str,
    load_date: str,
    table_name: str,
    load_type: str,
    rows_loaded: int,
    watermark_col_name: str | None,
    watermark: datetime | None
):
    query = f"""
      INSERT INTO {bq_table} (run_id, load_date, table_name, load_type, rows_loaded, watermark_col_name, watermark)
      VALUES (@run_id, @load_date, @table_name, @load_type, @rows_loaded, @watermark_col_name, @watermark)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("load_date", "STRING", load_date),
            bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
            bigquery.ScalarQueryParameter("load_type", "STRING", load_type),
            bigquery.ScalarQueryParameter("rows_loaded", "INT64", rows_loaded),
            bigquery.ScalarQueryParameter("watermark_col_name", "STRING", watermark_col_name),
            bigquery.ScalarQueryParameter("watermark", "TIMESTAMP", watermark)
        ]
    )

    job = bq_client.query(query, job_config=job_config)
    job.result()
    logger.info(f"Successfully added audit entry run_id: {run_id} load_date:{load_date} table_name: {table_name} "
                f"to BQ table: {bq_table}")


# archive_folder_contents('products')


configs = read_config_file()

for table_data in configs['tables']:
    if table_data['is_active']:
        archive_table_data(table_data['name'])
        run_info = extract_and_save_to_landing(
            table_name=table_data['name'],
            load_type=table_data['load_type'],
            watermark_col=table_data['watermark']
        )
        stats = get_written_stats(run_info, table_data['watermark'])
        insert_audit_row(
            BQ_AUDIT_TABLE,
            run_id = run_info['run_id'],
            load_date = run_info['load_date'],
            table_name = table_data['name'],
            load_type = table_data['load_type'],
            rows_loaded = stats["rows_loaded"],
            watermark_col_name = table_data['watermark'],
            watermark = stats["max_watermark"]
        )



