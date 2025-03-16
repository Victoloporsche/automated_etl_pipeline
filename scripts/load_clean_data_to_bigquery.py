from google.cloud import bigquery
import logging
from dotenv import load_dotenv
import os
from scripts.custom_errors import UploadToBigQueryFailedError

script_dir = os.path.dirname(os.path.abspath(__file__))
base_path = os.path.dirname(os.path.dirname(script_dir))
env_path = os.path.join(base_path, "automated_etl_pipeline/.env.local")

#load env variables
load_dotenv(env_path)
project_id = os.getenv("project")
clean_dataset = os.getenv("clean_dataset")
shopify = os.getenv("shopify_table")
facebook = os.getenv("facebook_table")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

schema_shopify = [
    bigquery.SchemaField("order_id", "STRING", mode="NULLABLE", description="Unique identifier for the order"),
    bigquery.SchemaField("customer_id", "STRING", mode="NULLABLE", description="Unique identifier for the customer"),
    bigquery.SchemaField("customer_name", "STRING", mode="NULLABLE", description="Name of the customer"),
    bigquery.SchemaField("email", "STRING", mode="NULLABLE", description="Customer's email address"),
    bigquery.SchemaField("order_date", "DATE", mode="NULLABLE", description="Date the order was placed"),
    bigquery.SchemaField("total_price", "FLOAT", mode="NULLABLE", description="Total price of the order in original currency"),
    bigquery.SchemaField("currency", "STRING", mode="NULLABLE", description="Currency code of the order"),
    bigquery.SchemaField("status", "STRING", mode="NULLABLE", description="Current status of the order (e.g., pending, canceled, shipped)"),
    bigquery.SchemaField("financial_status", "STRING", mode="NULLABLE", description="Financial status of the order (e.g., paid, refunded)"),
    bigquery.SchemaField("fulfillment_status", "STRING", mode="NULLABLE", description="Fulfillment status of the order (e.g., fulfilled, partial)"),
    bigquery.SchemaField("payment_method", "STRING", mode="NULLABLE", description="Method used for payment"),
    bigquery.SchemaField("discount_code", "STRING", mode="NULLABLE", description="Discount code applied to the order"),
    bigquery.SchemaField("discount_amount", "FLOAT", mode="NULLABLE", description="Amount discounted from the order"),
    bigquery.SchemaField("shipping_address", "STRING", mode="NULLABLE", description="Full shipping address for the order"),
    bigquery.SchemaField("shipping_country", "STRING", mode="NULLABLE", description="Country code of the shipping destination"),
    bigquery.SchemaField("tracking_number", "STRING", mode="NULLABLE", description="Tracking number for the shipment"),
    bigquery.SchemaField("tracking_company", "STRING", mode="NULLABLE", description="Company handling the shipment"),
    bigquery.SchemaField("refunds", "STRING", mode="NULLABLE", description="JSON string containing refund details"),
    bigquery.SchemaField("line_items", "STRING", mode="NULLABLE", description="Description of items in the order (e.g., number of products)"),
    bigquery.SchemaField("total_price_eur", "FLOAT", mode="NULLABLE", description="Total price of the order in EUR")
]
schema_facebook = [
    bigquery.SchemaField("ad_id", "STRING", mode="NULLABLE", description="Unique identifier for the ad"),
    bigquery.SchemaField("adset_id", "STRING", mode="NULLABLE", description="Unique identifier for the ad set"),
    bigquery.SchemaField("campaign_id", "STRING", mode="NULLABLE", description="Unique identifier for the campaign"),
    bigquery.SchemaField("ad_name", "STRING", mode="NULLABLE", description="Name of the advertisement"),
    bigquery.SchemaField("adset_name", "STRING", mode="NULLABLE", description="Name of the ad set"),
    bigquery.SchemaField("date_start", "DATE", mode="NULLABLE", description="Start date of the ad campaign"),
    bigquery.SchemaField("date_end", "DATE", mode="NULLABLE", description="End date of the ad campaign"),
    bigquery.SchemaField("impressions", "INTEGER", mode="NULLABLE", description="Number of times the ad was shown"),
    bigquery.SchemaField("clicks", "INTEGER", mode="NULLABLE", description="Number of clicks on the ad"),
    bigquery.SchemaField("conversions", "INTEGER", mode="NULLABLE", description="Number of conversions from the ad"),
    bigquery.SchemaField("spend", "FLOAT", mode="NULLABLE", description="Amount spent on the ad in original currency"),
    bigquery.SchemaField("ctr", "FLOAT", mode="NULLABLE", description="Click-through rate (percentage)"),
    bigquery.SchemaField("cpc", "FLOAT", mode="NULLABLE", description="Cost per click"),
    bigquery.SchemaField("roas", "FLOAT", mode="NULLABLE", description="Return on ad spend"),
    bigquery.SchemaField("age", "STRING", mode="NULLABLE", description="Age range of the target audience"),
    bigquery.SchemaField("gender", "STRING", mode="NULLABLE", description="Gender of the target audience"),
    bigquery.SchemaField("country", "STRING", mode="NULLABLE", description="Country code of the target audience"),
    bigquery.SchemaField("device", "STRING", mode="NULLABLE", description="Device type used to view the ad"),
    bigquery.SchemaField("spend_eur", "FLOAT", mode="NULLABLE", description="Amount spent on the ad in EUR")
]

uri_facebook = 'gs://orders-and-ads/raw/clean_facebook_ads.csv'
uri_shopify = 'gs://orders-and-ads/raw/clean_shopify_orders.csv'
dataset_id = f'{project_id}.{clean_dataset}'
table_id_shopify = f'{dataset_id}.{shopify}'
table_id_facebook = f'{dataset_id}.{facebook}'

def load_to_bigquery(schema: list,
                     uri: str,
                     table_id: str)-> None:
    """
    This function loads the clean data from GCS to BigQuery
    :param schema: schema of the columns
    :param uri: the uri of the gcs cleaned data
    :param table_id: the id of the table in big query
    """
    try:
        client = bigquery.Client(project=project_id)
        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            autodetect=False,  # We are providing the schema explicitly
        )
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()  # Wait for the load job to finish
        logger.info(f"Data successfully loaded into BigQuery: {dataset_id}.{table_id}")
    except Exception as e:
        logger.error(f"Error uploading clean data to from GCS to Big query: {e}")
        raise UploadToBigQueryFailedError


def load_shopify_facebook_clean_data()->None:
    """
    This function loads both the cleaned shopify and facebook data to big query
    """
    load_to_bigquery(schema=schema_shopify,
                     uri=uri_shopify,
                     table_id=table_id_shopify)
    load_to_bigquery(schema=schema_facebook,
                     uri=uri_facebook,
                     table_id=table_id_facebook)

if __name__ == "__main__":
    load_shopify_facebook_clean_data()

