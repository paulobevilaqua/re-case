CREATE OR REPLACE TABLE `re-case.backend_events_data.order_events` (
  event_type STRING OPTIONS(description="Type of event received (e.g., 'order')"),
  order_id STRING OPTIONS(description="Unique identifier for the order"),
  customer_id STRING OPTIONS(description="Unique identifier for the customer who placed the order"),
  order_date TIMESTAMP OPTIONS(description="Timestamp when the order was placed"),
  status STRING OPTIONS(description="Current status of the order (e.g., 'pending', 'shipped', 'delivered')"),
  items ARRAY<STRUCT<
    product_id STRING OPTIONS(description="Unique identifier for the product"),
    product_name STRING OPTIONS(description="Name of the product"),
    quantity INT64 OPTIONS(description="Number of units of the product in the order"),
    price FLOAT64 OPTIONS(description="Unit price of the product at the time of purchase")
  >> OPTIONS(description="List of items in the order, including product ID, name, quantity, and price"),
  shipping_address STRUCT<
    street STRING OPTIONS(description="Street address for delivery"),
    city STRING OPTIONS(description="City for delivery"),
    country STRING OPTIONS(description="Country for delivery")
  > OPTIONS(description="Shipping address for the order"),
  total_amount FLOAT64 OPTIONS(description="Total value of the order"),
  ingestion_time TIMESTAMP OPTIONS(description="Timestamp when the event was ingested into the data pipeline")
)
PARTITION BY DATE(order_date)
CLUSTER BY status, customer_id;
