CREATE OR REPLACE TABLE `re-case.backend_events_data.inventory_events` (
  event_type STRING OPTIONS (description="Type of event, e.g. inventory"),
  inventory_id STRING OPTIONS (description="Unique identifier for the inventory event (UUID)"),
  product_id STRING OPTIONS (description="Unique identifier for the product (UUID)"),
  warehouse_id STRING OPTIONS (description="Unique identifier for the warehouse (UUID)"),
  quantity_change INT64 OPTIONS (description="Change in quantity (-100 to 100)"),
  reason STRING OPTIONS (description="Reason for the change: restock, sale, return, damage"),
  timestamp TIMESTAMP OPTIONS (description="Datetime when the inventory change occurred"),
  ingestion_time TIMESTAMP OPTIONS (description="Timestamp when the event was ingested into BigQuery")
)
PARTITION BY DATE(timestamp)
CLUSTER BY product_id, warehouse_id
OPTIONS(
  description="Stores inventory-related events for tracking stock levels"
);
