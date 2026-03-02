current:
finish gcp creds to spark. do the testing

export PYSPARK_PYTHON=/Users/artemi_lapitski/Desktop/gcp-project/gcp-retailer-project/venv/bin/python3
export PYSPARK_DRIVER_PYTHON=/Users/artemi_lapitski/Desktop/gcp-project/gcp-retailer-project/venv/bin/python3
spark-submit --master 'local[*]' notebooks/postgres-to-landing.py --config gs://retailer-project/configs/retailer_config.json

when it comes to incrememntal load - for now you take > strictly greater than latest
watermark, but this could be bad if new data added to source for same watermark.
keep ids in audit and use as tie breaker.
(updated_at > last_ts) OR (updated_at = last_ts AND id > last_id)



to set up gcp:
install gcp sdk

 - to authenticate user account:
gcloud init

- to auth application account:
gcloud auth application-default login



delete dataproc cluster:
gcloud dataproc clusters delete ${CLUSTER_NAME} --region ${REGION}


create dataproc cluster:
REGION="europe-central2"
CLUSTER_NAME="my-demo-cluster"
gcloud dataproc clusters create ${CLUSTER_NAME} \

    --region ${REGION} \

    --num-workers=2 \

    --worker-machine-type=n1-standard-2 \

    --worker-boot-disk-size=50 \

    --master-machine-type=n1-standard-2 \

    --master-boot-disk-size=50 \

    --image-version=2.0-debian10 \

    --enable-component-gateway \

    --optional-components=JUPYTER \

    --initialization-actions=gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh \

    --metadata bigquery-connector-version=1.2.0 \

    --metadata spark-bigquery-connector-version=0.21.0