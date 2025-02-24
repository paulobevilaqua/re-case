# Prior:

# gcloud auth application-default login
# Log into browser

# Set the project:
# gcloud config set project re-case

# First run: 
# Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
# Then:
# .\deploy.ps1

# Power Shell Script

# deploy.ps1
# 1. Create Pub/Sub topic & subscription
# 2. Deploy pipeline to Dataflow (streaming)
# 3. Publish sample events

# Write-Host "=== Creating Pub/Sub topic and subscription ==="
# # Try to create the topic. If it already exists, ignore error
# gcloud pubsub topics create backend-events-topic
# # Create the subscription. If it already exists, ignore error
# gcloud pubsub subscriptions create backend-events-topic-sub --topic=backend-events-topic

Write-Host "=== Deploying the streaming pipeline to Dataflow ==="

python .\pipeline\main.py `
  --input_subscription projects/re-case/subscriptions/backend-events-topic-sub `
  --bq_dataset backend_events_data `
  --bq_project re-case `
  --runner DataflowRunner `
  --project re-case `
  --region us-central1 `
  --job_name backend-events-pipeline `
  --temp_location gs://backend_events_data/temp `
  --staging_location gs://backend_events_data/staging `
  --gcs_output_path gs://backend_events_data/backup `
  --streaming