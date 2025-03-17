import pandas as pd
import os
from google.cloud import storage
import logging
from typing import Optional
from dotenv import load_dotenv
from scripts.custom_errors import UploadToGCSFailedError

script_dir = os.path.dirname(os.path.abspath(__file__))
base_path = os.path.dirname(os.path.dirname(script_dir))
env_path = os.path.join(base_path, "automated_etl_pipeline/.env.local")

#load env variables
load_dotenv(env_path)

project_id = os.getenv("project")
bucket_name = os.getenv("bucket_name")

# read the shopify and facebook data with pandas
shopify_df = pd.read_csv(os.path.join(base_path, "automated_etl_pipeline/data/intermate_shop_orders.csv")
)
facebook_df = pd.read_csv(os.path.join(base_path, "automated_etl_pipeline/data/facebook_ads_insights.csv"))

#use the current currency conversion rates
currency_conversion = {
    "USD": 0.85,
    "GBP": 1.16,
    "EUR": 1.0
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def get_missing_columns_from_dict(data: dict)-> dict[str, int]:
    """
    This function returns the columns which has missing values
    :param data: keys of dataframe column and values are the number of missing values
    :return: This function returns the columns with missing values
    """
    column_value = {key: value for key, value in data.items() if value > 0}
    return column_value

def remove_duplicates(df:pd.DataFrame)-> pd.DataFrame:
    """
    This function removes duplicates from the dataframe
    :param df: is the dataframe of the data
    :return: It returns dataframe without duplicates
    """
    if df.duplicated().any():
        logging.info('Duplicates found in input data')
        df.drop_duplicates(inplace=True)
        logger.info('Duplicates removed from input data')
    return df


def get_missing_values_columns(df: pd.DataFrame)-> Optional[dict[str, int]]:
    """
    This function returns the columns which has missing values
    :param df: This is the pandas dataframe
    :return: This function returns the columns with missing values
    """
    if df.isnull().values.any():
        logging.info('Missing values found in input data')
        df_columns = df.isna().sum().to_dict()

        df_missing_values_column = get_missing_columns_from_dict(df_columns)
        return df_missing_values_column
    logger.info('No missing values found in input data')
    return None

def replace_missing_values(df:pd.DataFrame,
                           missing_columns: dict[str, int])-> pd.DataFrame:
    """
    This function replaces the missing values with the mode of the column
    :param df: this is the dataframe of the data
    :param missing_columns: The column which has missing data
    :return: This function returns the dataframe with missing values replaced
    """
    if missing_columns:
        for column in missing_columns.keys():
            dtype = df[column].dtype

            if dtype == 'object':
                df[column] = df[column].fillna('NONE')
            elif dtype in ['float64', 'float32']:
                df[column] = df[column].fillna(0.0)
            else:
                logger.error(f"Unhandled type {dtype} for column {column}")
    return df

def upload_clean_data_to_gcs(bucket_name: str,
                             source_file_name: str,
                             destination_blob_name: str)-> None:
    """
    This function uploads the clean data to GCS
    :param bucket_name: the name of the bucket is GCS
    :param source_file_name: the location of the clean data
    :param destination_blob_name: the destination of the gcs raw path
    :return: This function uploads the clean data to GCS bucket
    """
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    logger.info(f"Uploaded {source_file_name} to {destination_blob_name} successfully")


def preprocess_and_upload_to_gcs(shopify: pd.DataFrame,
                                 facebook: pd.DataFrame)-> None:
    """
    This is the main function which preprocess the data and uploads it to GCS
    :param shopify: the df of the shopify dataframe
    :param facebook: the df of the facebook dataframe
    """
    shopify = remove_duplicates(df=shopify)
    facebook = remove_duplicates(df=facebook)

    shopify['total_price_eur'] = shopify.apply(
        lambda row: row['total_price'] * currency_conversion.get(row['currency'], 1), axis=1
    )
    shopify['total_price_eur'] = shopify['total_price_eur'].round(2)

    facebook['spend_eur'] = facebook['spend'] * currency_conversion["USD"]  # Assuming spend is in USD

    facebook['spend_eur'] = facebook['spend_eur'].round(2)

    shopify_missing_values_columns = get_missing_values_columns(shopify)
    facebook_missing_values_columns = get_missing_values_columns(facebook)

    shopify = replace_missing_values(df=shopify,
                                     missing_columns=shopify_missing_values_columns)
    facebook = replace_missing_values(df=facebook,
                                     missing_columns=facebook_missing_values_columns)

    shopify.to_csv(os.path.join(base_path, "automated_etl_pipeline/data/clean_shopify_orders.csv"), index=False)
    facebook.to_csv(os.path.join(base_path, "automated_etl_pipeline/data/clean_facebook_ads.csv"), index=False)

    try:
        upload_clean_data_to_gcs(bucket_name=bucket_name,
                                 source_file_name= os.path.join(base_path, "automated_etl_pipeline/data/clean_shopify_orders.csv"),
                                 destination_blob_name="raw/clean_shopify_orders.csv")

        upload_clean_data_to_gcs(bucket_name=bucket_name,
                                 source_file_name= os.path.join(base_path, "automated_etl_pipeline/data/clean_facebook_ads.csv"),
                                 destination_blob_name="raw/clean_facebook_ads.csv")
    except Exception as e:
        logger.error(f"Error uploading clean data to GCS: {e}")
        raise UploadToGCSFailedError

# Define a function called save_to_gcs
def save_to_gcs():
    # Call the preprocess_and_upload_to_gcs function and pass in the shopify_df and facebook_df variables
    preprocess_and_upload_to_gcs(shopify_df, facebook_df)


if __name__ == "__main__":
    save_to_gcs()





