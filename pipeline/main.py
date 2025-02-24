import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime, timezone
import argparse
import os

logging.basicConfig(level=logging.INFO)

class ParsePubSubMessage(beam.DoFn):
    """Parses raw Pub/Sub messages, ensures JSON validity, and adds ingestion_time."""

    def __init__(self):
        self.invalid_messages = beam.metrics.Metrics.counter(self.__class__, 'invalid_messages')

    def process(self, element):
        try:
            decoded_str = element.decode("utf-8")
            logging.info(f"Raw PubSub message: {decoded_str}")
            record = json.loads(decoded_str)
            record['ingestion_time'] = datetime.now(timezone.utc).isoformat()
            logging.info(f" Processed message: {record}")
            yield record

        except json.JSONDecodeError as e:
            logging.error(f"❌ JSON Decode Error: {e} - {element}")
            self.invalid_messages.inc()
            return

        except Exception as e:
            logging.error(f"❌ General Error: {e} - {element}")
            self.invalid_messages.inc()
            return

# Schemas 
ORDER_SCHEMA = {
    "fields": [
        {"name": "event_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "order_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "status", "type": "STRING", "mode": "NULLABLE"},
        {
            "name": "items",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "price", "type": "FLOAT", "mode": "NULLABLE"},
            ],
        },
        {
            "name": "shipping_address",
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [
                {"name": "street", "type": "STRING", "mode": "NULLABLE"},
                {"name": "city", "type": "STRING", "mode": "NULLABLE"},
                {"name": "country", "type": "STRING", "mode": "NULLABLE"},
            ],
        },
        {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "ingestion_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]
}

INVENTORY_SCHEMA = {
    "fields": [
        {"name": "event_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "inventory_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "warehouse_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "quantity_change", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "reason", "type": "STRING", "mode": "NULLABLE"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "ingestion_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]
}

USER_ACTIVITY_SCHEMA = {
    "fields": [
        {"name": "event_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "activity_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ip_address", "type": "STRING", "mode": "NULLABLE"},
        {"name": "user_agent", "type": "STRING", "mode": "NULLABLE"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "metadata", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ingestion_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]
}

def run(argv=None):
    """Runs the Beam pipeline with user-specified parameters."""

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_subscription", required=True)
    parser.add_argument("--bq_dataset", required=True)
    parser.add_argument("--bq_project", required=True)
    parser.add_argument("--runner", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--job_name", required=True)
    parser.add_argument("--temp_location", required=True)
    parser.add_argument("--staging_location", required=True)
    parser.add_argument("--streaming", action="store_true")
    parser.add_argument("--gcs_output_path", required=True)

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(
        pipeline_args,
        streaming=known_args.streaming,
        save_main_session=True,
        runner=known_args.runner,
        project=known_args.project,
        region=known_args.region,
        temp_location=known_args.temp_location,
        staging_location=known_args.staging_location,
        job_name=known_args.job_name,
    )

    bq_dataset = known_args.bq_dataset
    order_bq_table = f"{known_args.bq_project}.{bq_dataset}.order_events"
    inventory_bq_table = f"{known_args.bq_project}.{bq_dataset}.inventory_events"
    user_activity_bq_table = f"{known_args.bq_project}.{bq_dataset}.user_activity_events"

    gcs_output_path = known_args.gcs_output_path

    p = beam.Pipeline(options=pipeline_options)

    raw_events = (
        p
        | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
            subscription=known_args.input_subscription,
            with_attributes=False
        )
    )

    parsed_events = (
        raw_events
        | "ParseJSON" >> beam.ParDo(ParsePubSubMessage())
    )

    order_events = parsed_events | "FilterOrder" >> beam.Filter(lambda x: x.get('event_type') == 'order')
    inventory_events = parsed_events | "FilterInventory" >> beam.Filter(lambda x: x.get('event_type') == 'inventory')
    user_activity_events = parsed_events | "FilterUserActivity" >> beam.Filter(lambda x: x.get('event_type') == 'user_activity')

    def format_gcs_path(element, event_type, base_path):
        """Formats the GCS path based on event type and ingestion time."""
        ingestion_time = datetime.fromisoformat(element['ingestion_time'])
        file_name = f"{event_type}_{ingestion_time.strftime('%Y%m%d%H%M%S%f')}.json"
        path = os.path.join(
            base_path,
            event_type,
            str(ingestion_time.year),
            str(ingestion_time.month).zfill(2),
            str(ingestion_time.day).zfill(2),
            str(ingestion_time.hour).zfill(2),
            str(ingestion_time.minute).zfill(2),
        )
        return os.path.join(path, file_name)

    def write_to_gcs(events, event_type, base_path):
        def write_file(element, path):
            with filesystems.FileSystems.create(path) as f:
                f.write(json.dumps(element).encode('utf-8'))

        return (events
            | f"Format {event_type} GCS Path" >> beam.Map(lambda element: (element, format_gcs_path(element, event_type, base_path)))
            | f"Write {event_type} to GCS" >> beam.MapTuple(write_file))

    # Storing in GCS
    write_to_gcs(order_events, "order", gcs_output_path)
    write_to_gcs(inventory_events, "inventory", gcs_output_path)
    write_to_gcs(user_activity_events, "user_activity", gcs_output_path)

    # Writing in BigQuery
    order_events | "WriteOrderToBQ" >> beam.io.WriteToBigQuery(
        table=order_bq_table,
        schema=ORDER_SCHEMA,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )

    inventory_events | "WriteInventoryToBQ" >> beam.io.WriteToBigQuery(
        table=inventory_bq_table,
        schema=INVENTORY_SCHEMA,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )

    user_activity_events | "WriteUserActivityToBQ" >> beam.io.WriteToBigQuery(
        table=user_activity_bq_table,
        schema=USER_ACTIVITY_SCHEMA,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )

    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    run()