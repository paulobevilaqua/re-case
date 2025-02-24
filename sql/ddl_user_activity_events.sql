CREATE OR REPLACE TABLE `re-case.backend_events_data.user_activity_events` (
  event_type STRING OPTIONS (description="Type of event, e.g. user_activity"),
  user_id STRING OPTIONS (description="Unique identifier for the user (UUID)"),
  activity_type STRING OPTIONS (description="Type of activity: login, logout, view_product, etc."),
  ip_address STRING OPTIONS (description="IP address from where the activity originated"),
  user_agent STRING OPTIONS (description="User agent string representing device/browser info"),
  timestamp TIMESTAMP OPTIONS (description="Datetime of the user activity event"),
  metadata STRUCT<
    session_id STRING,
    platform STRING
  > OPTIONS (description="Additional info about the user session, platform, etc."),
  ingestion_time TIMESTAMP OPTIONS (description="Timestamp when the event was ingested into BigQuery")
)
PARTITION BY DATE(timestamp)
CLUSTER BY user_id, activity_type
OPTIONS(
  description="Stores user activity events for tracking engagement data"
);
