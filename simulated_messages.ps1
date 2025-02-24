# Samples of simulated messages:

Write-Host "=== Publishing sample events to Pub/Sub ==="

# A) Order event examples (all JSON in a single line)
gcloud pubsub topics publish backend-events-topic --message='{"event_type":"order","order_id":"test-order-003","customer_id":"cust-xyz","order_date":"2025-02-24T12:30:00Z","status":"processing","items":[{"product_id":"p3","product_name":"AnotherWidget","quantity":5,"price":8.00}],"shipping_address":{"street":"456 Another St","city":"AnotherCity","country":"AnotherCountry"},"total_amount":40.00}'

# B) Inventory events
gcloud pubsub topics publish backend-events-topic --message='{"event_type":"inventory","inventory_id":"inv-002","product_id":"p1","warehouse_id":"wh-1","quantity_change":20,"reason":"restock","timestamp":"2025-02-23T11:00:00Z"}'

# C) User Activity events
gcloud pubsub topics publish backend-events-topic --message='{"event_type":"user_activity","user_id":"user-456","activity_type":"login","ip_address":"192.168.0.10","user_agent":"Mozilla/5.0","timestamp":"2025-02-23T09:15:00Z","metadata":{"session_id":"sess-a1b2c3","platform":"web"}}'
gcloud pubsub topics publish backend-events-topic --message='{"event_type":"user_activity","user_id":"user-321","activity_type":"view_product","ip_address":"10.0.0.55","user_agent":"Mozilla/5.0 (Mobile)","timestamp":"2025-02-24T16:45:00Z","metadata":{"session_id":"sess-d4e5f6","platform":"mobile"}}'