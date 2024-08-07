PROJECT_ID=sofia-data-305018
REGION=us-central1
CONNECTION_ID=gemini-pro-llm-vertex-connection

bq mk --connection --location=$REGION --project_id=$PROJECT_ID --connection_type=CLOUD_RESOURCE $CONNECTION_ID
bq show --connection $PROJECT_ID.$REGION.$CONNECTION_ID
