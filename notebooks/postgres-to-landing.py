from datetime import datetime, timezone
import logging
import time
import json
import argparse
from google.cloud import storage, bigquery
from pyspark.sql import SparkSession, functions as F
from pyspark import StorageLevel


POSTGRES_PASSWORD = "mypass"

# Set up logging
logging.Formatter.converter = time.gmtime
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)sZ - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)
logger = logging.getLogger("RetailerPostgresToLanding")

# Set up spark
spark = SparkSession.builder.appName("RetailerPostgesToLanding").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sparkContext.setLogLevel("WARN")

# GCP clients
storage_client = storage.Client()
bq_client = bigquery.Client()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        required=True,
        help="Path to YAML config file (local or gs://)"
    )
    return parser.parse_args()


def read_config_file(config_path: str) -> dict:
    """
    Read configs from the GCS, config_patt should be provided in gs://<bucket>/<path> format.
    """
    if not config_path or not isinstance(config_path, str):
        raise ValueError("config_path must be a non-empty string")

    if not config_path.startswith("gs://"):
        raise ValueError("config_path should be in gs://<bucket>/<path> format")

    path_without_prefix = config_path[5:]  # remove gs://

    parts = path_without_prefix.split("/", 1) # get bucket and blob path
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError("Invalid GCS path. Expected format: gs://<bucket>/<object_path>")

    bucket_name, blob_path = parts

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    content = blob.download_as_text()
    logger.info(f"🚀 Successfully read config file {config_path}")
    # return yaml.safe_load(content)
    return json.loads(content)


def archive_table_data(gcs_bucket: str, landing_path: str, archive_path: str, table_name: str):
    """
    Copy all objects from LANDING_PATH + table_name to ARCHIVE_PATH within the same bucket.
    """
    bucket = storage_client.bucket(gcs_bucket)
    src_prefix = f"{landing_path.strip('/')}/{table_name}/"
    blobs = [blob for blob in storage_client.list_blobs(bucket, prefix=src_prefix) if blob.name.endswith('.json')]

    if not blobs:
        logger.info(f"🚀 No objects found under prefix: gs://{gcs_bucket.strip('/')}/{src_prefix}")

    for blob in blobs:
        # Preserve relative path inside the prefix
        rel_name = blob.name[len(src_prefix):]

        new_name = f"{archive_path.strip('/')}/{table_name}/{rel_name}"
        bucket.copy_blob(blob, bucket, new_name)
        blob.delete()


def generate_run_metadata():
    now_utc = datetime.now(timezone.utc)
    return {
        "run_id": now_utc.strftime("%Y%m%dT%H%M%SZ"),
        "load_date": now_utc.strftime("%Y-%m-%d")
    }


def get_latest_watermark(bq_audit_table: str, table_name: str) -> datetime:
    """
    Get latest watermark from the previous runs for incremental tables.
    """
    query = f"""
        SELECT MAX(watermark) AS latest_watermark
        FROM `{bq_audit_table}`
        WHERE table_name = @table_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("table_name", "STRING", table_name)]
    )
    query_job = bq_client.query(query, job_config=job_config)
    result = query_job.result()
    row = next(result, None)
    return (
        row.latest_watermark
        if row and row.latest_watermark
        else datetime(1900, 1, 1, tzinfo=timezone.utc)
    )


def extract_and_save_to_landing(
        run_id: str,
        load_date: str,
        postgres_url: str,
        postgres_user: str,
        postgres_driver: str,
        postgres_schema: str,
        postgres_password: str,
        bq_audit_table: str,
        gcs_bucket: str,
        landing_path: str,
        table_name: str,
        load_type: str,
        watermark_col = None
) -> dict:
    """
    Extract data from postgres depending on load_type, calculate row count and max_watermark stats.
    Add technical fields load_date and run_id. Write to landing.
    Return run_info in form of dict, which includes information about the run along with stats.
    """

    # Define what data you read from postgres depending on load_type
    if load_type == 'incremental' and watermark_col:
        latest_watermark = get_latest_watermark(bq_audit_table, table_name)
        latest_watermark_str = latest_watermark.isoformat()
        query = f"""
            SELECT *
            FROM {postgres_schema}.{table_name}
            WHERE {watermark_col} > TIMESTAMPTZ '{latest_watermark_str}'
        """
    else:
        query = f"""
            SELECT *
            FROM {postgres_schema}.{table_name}
        """

    df = (
        spark.read
        .format("jdbc")
        # .options(**POSTGRES_CONFIG)
        .option("url", postgres_url)
        .option("user", postgres_user)
        .option("password", postgres_password)
        .option("driver", postgres_driver)
        .option("query", query)
        .load()
    )

    # Persist before calculating stats
    df = df.persist(StorageLevel.MEMORY_AND_DISK)

    rows_loaded = df.count()

    max_watermark = None
    if watermark_col and rows_loaded:
        max_watermark = (
            df
            .agg(F.max(F.col(watermark_col)).alias("max_wm"))
            .collect()[0]["max_wm"]
        )

    landing_prefix = f"gs://{gcs_bucket}/{landing_path.strip('/')}/{table_name}"

    # Add technical fields to data
    df_to_write = (
        df
        .withColumn("load_date", F.lit(load_date).cast("date"))
        .withColumn("run_id", F.lit(run_id))
    )

    (
        df_to_write
        .write
        .mode('append')
        .partitionBy("load_date", "run_id")
        .json(landing_prefix)
    )

    df.unpersist()

    logger.info(f"🚀 Successfully extracted data from {postgres_schema}.{table_name}")

    return {
        "rows_loaded": rows_loaded,
        "max_watermark": max_watermark
    }


def insert_audit_row(
    bq_table: str,
    run_id: str,
    load_date: str,
    table_name: str,
    load_type: str,
    rows_loaded: int,
    watermark_col_name: str | None,
    watermark: datetime | None
) -> None:
    query = f"""
      INSERT INTO `{bq_table}` (run_id, load_date, table_name, load_type, rows_loaded, watermark_col_name, watermark)
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
    logger.info(f"🚀 Successfully added audit entry run_id: {run_id} load_date:{load_date} table_name: {table_name} "
                f"to BQ table: {bq_table}")


if __name__ == "__main__":
    args = parse_args()
    config_path = args.config
    configs = read_config_file(config_path)

    gcs_bucket = configs["gcs"]["bucket"]
    landing_path = configs["gcs"]["landing_base"]
    archive_path = configs["gcs"]["archive_base"]
    bq_audit_table = f"{configs["bigquery"]["project"]}.{configs["bigquery"]["audit_table"]}"

    postgres_data = configs["postgres"]

    run_metadata = generate_run_metadata()

    for table_data in configs['tables']:
        if table_data['is_active']:
            archive_table_data(
                gcs_bucket=gcs_bucket,
                landing_path=landing_path,
                archive_path=archive_path,
                table_name=table_data['name']
            )

            run_info = extract_and_save_to_landing(
                **run_metadata,
                **postgres_data,
                postgres_password=POSTGRES_PASSWORD,
                bq_audit_table=bq_audit_table,
                gcs_bucket=gcs_bucket,
                landing_path=landing_path,
                table_name=table_data['name'],
                load_type=table_data['load_type'],
                watermark_col=table_data.get("watermark")
            )

            insert_audit_row(
                **run_metadata,
                bq_table=bq_audit_table,
                table_name=table_data['name'],
                load_type=table_data['load_type'],
                watermark_col_name=table_data.get("watermark"),
                rows_loaded=run_info["rows_loaded"],
                watermark=run_info["max_watermark"]
            )
