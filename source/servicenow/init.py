import os
import json
from google.cloud import datastore, secretmanager

os.environ["CONFIG_KEY"] = os.environ.get("CONFIG_KEY")

config_key = os.environ.get("CONFIG_KEY")
config_kind = f"{config_key.split('/')[0]}"
config_name = f"{config_key.split('/')[1]}"

datastore_client = datastore.Client()
task_key = datastore_client.key(config_kind, config_name)
entity = datastore_client.get(key=task_key)

credentials_secret_id = entity.get("credentials_secret_id")
client = secretmanager.SecretManagerServiceClient()
response = client.access_secret_version(request={"name": credentials_secret_id})
payload = response.payload.data.decode("UTF-8")
payload = payload.replace("\'", "\"")
secrets = json.loads(payload)

# gcp common variables
bq_project = entity.get("bq_project")
log_name = entity.get("log_name", "reports-automation-application")
dump_gcs_bucket = entity.get("dump_gcs_bucket")
dump_gcs_path = entity.get("dump_gcs_path")

# bigquery variables
dataset_id = entity.get("dataset_id")
dataset_location = entity.get("dataset_location")

# servicenow variables
instance_uri = entity.get("instance_uri")

# secrets
access_token = secrets.get("access_token", None)
refresh_token = secrets.get("refresh_token", None)
client_id = secrets.get("client_id", None)
client_secret = secrets.get("client_secret", None)