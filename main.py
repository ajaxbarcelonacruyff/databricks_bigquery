# Copy this cell 
%pip install google-cloud-bigquery google-auth
dbutils.library.restartPython()


from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from pyspark.sql import SparkSession
import json

# Service account key 
service_account_info = {
  "type": "service_account",
  "project_id": "project_id",
  "private_key_id": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "private_key": "-----BEGIN PRIVATE KEY-----xxxxxxxxxxxxxxxxxxx\n-----END PRIVATE KEY-----\n",
  "client_email": "databricks@project_id.iam.gserviceaccount.com",
  "client_id": "1234567890",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/databricks%40project_id.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}
credentials = service_account.Credentials.from_service_account_info(service_account_info)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)


from pyspark.sql import functions as F

# Write a query to retrieve the data from BigQuery
query = """
SELECT 
table_id AS table_name,
DATETIME(TIMESTAMP_MILLIS(creation_time),'Pacific/Auckland') AS creation_datetime,
DATETIME(TIMESTAMP_MILLIS(last_modified_time),'Pacific/Auckland') AS last_modified_datetime,
* ,
 FROM project_id.analytics_xxxxxxxx.__TABLES__
WHERE table_id LIKE "%_20241001"
;
"""
# Define the temporary view name that store the result of the query above.
view_name = "temp_bigquery_ga4_tables"

query_job = client.query(query)
results = query_job.result()

# Create a temporary SQL view 
rows = [dict(row) for row in results]
df = pd.DataFrame(rows)
spark = SparkSession.builder.appName("BigQueryToDatabricks").getOrCreate()
spark_df = spark.createDataFrame(df)
#spark_df = spark_df.withColumn("event_datetime", F.to_utc_timestamp(F.from_unixtime(F.col("event_timestamp")/1000000,'yyyy-MM-dd HH:mm:ss'),'UTC'))
spark_df.createOrReplaceTempView(view_name)
