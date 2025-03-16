import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import logging
from scripts.custom_errors import UploadToGCSFailedError
import os

from scripts.extract import (
    get_missing_columns_from_dict,
    remove_duplicates,
    get_missing_values_columns,
    replace_missing_values,
    upload_clean_data_to_gcs,
    preprocess_and_upload_to_gcs
)


class TestDataProcessing(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger()

        # Sample shopify dataframes for testing
        self.shopify_df = pd.DataFrame({
            'order_id': [1, 1, 2],
            'customer_id': [101, 101, 102],
            'order_date': ['2023-01-01', '2023-01-01', '2023-01-02'],
            'total_price': [100, 100, 200],
            'currency': ['USD', 'USD', 'EUR'],
            'discount_code': ['DISC1', 'DISC1', 'DISC2']
        })

        # Sample facebook dataframes for testing
        self.facebook_df = pd.DataFrame({
            'ad_id': [1, 1, 2],
            'campaign_id': [1001, 1001, 1002],
            'date_start': ['2023-01-01', '2023-01-01', '2023-01-02'],
            'spend': [50, 50, 75],
            'conversions': [5, 5, 8],
            'country': ['US', 'US', 'UK']
        })

        # Sample df with missing values
        self.df_with_missing = pd.DataFrame({
            'col1': [1, None, 3],
            'col2': ['A', 'B', None],
            'col3': [1.0, 2.0, None]
        })

    def test_get_missing_columns_from_dict(self):
        data = {'col1': 0, 'col2': 2, 'col3': 1}
        result = get_missing_columns_from_dict(data)
        expected = {'col2': 2, 'col3': 1}
        self.assertEqual(result, expected)

    def test_remove_duplicates(self):
        df = remove_duplicates(self.shopify_df.copy())
        self.assertEqual(len(df), 2)  # Should remove one duplicate
        self.assertFalse(df.duplicated().any())

    def test_get_missing_values_columns(self):
        result = get_missing_values_columns(self.df_with_missing)
        expected = {'col1': 1, 'col2': 1, 'col3': 1}
        self.assertEqual(result, expected)

        # Test with no missing values
        no_missing_df = pd.DataFrame({'col1': [1, 2, 3]})
        self.assertIsNone(get_missing_values_columns(no_missing_df))

    def test_replace_missing_values(self):
        missing_columns = {'col1': 1, 'col2': 1, 'col3': 1}
        df = replace_missing_values(self.df_with_missing.copy(), missing_columns)

        self.assertEqual(df['col1'].tolist(), [1, 0.0, 3])
        self.assertEqual(df['col2'].tolist(), ['A', 'B', 'NONE'])
        self.assertEqual(df['col3'].tolist(), [1.0, 2.0, 0.0])

    @patch('scripts.extract.storage.Client')
    def test_upload_clean_data_to_gcs_success(self, mock_storage_client):
        # Mock the GCS client and bucket
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        mock_storage_client.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        upload_clean_data_to_gcs('test_bucket', 'test_file.csv', 'raw/test_file.csv')
        mock_blob.upload_from_filename.assert_called_once_with('test_file.csv')

    @patch('scripts.extract.storage.Client')
    def test_upload_clean_data_to_gcs_failure(self, mock_storage_client):
        # Mock the GCS client to raise an exception
        mock_client = MagicMock()
        mock_storage_client.return_value = mock_client
        mock_client.bucket.side_effect = Exception("Upload failed")

        with self.assertRaises(UploadToGCSFailedError):
            preprocess_and_upload_to_gcs(self.shopify_df, self.facebook_df)

    @patch('scripts.extract.upload_clean_data_to_gcs')
    def test_preprocess_and_upload_to_gcs(self, mock_upload):
        preprocess_and_upload_to_gcs(self.shopify_df.copy(), self.facebook_df.copy())

        # Check if files are created
        self.assertTrue(os.path.exists('../data/clean_shopify_orders.csv'))
        self.assertTrue(os.path.exists('../data/clean_facebook_ads.csv'))

        # Read the processed files and verify data cleaning
        shopify_clean = pd.read_csv('../data/clean_shopify_orders.csv')
        facebook_clean = pd.read_csv('../data/clean_facebook_ads.csv')

        # Verify currency conversion (USD: 0.85, EUR: 1.0)
        self.assertEqual(shopify_clean['total_price_eur'].iloc[0], 85.0)  # 100 * 0.85
        self.assertEqual(shopify_clean['total_price_eur'].iloc[1], 200.0)  # 200 * 1.0
        self.assertEqual(facebook_clean['spend_eur'].iloc[0], 42.5)  # 50 * 0.85

        # Verify upload calls
        mock_upload.assert_any_call(
            bucket_name='orders-and-ads',
            source_file_name='/Users/victoruwaje/Desktop/Personal_projects/automated_etl_pipeline/data/clean_shopify_orders.csv',
            destination_blob_name='raw/clean_shopify_orders.csv'
        )
        mock_upload.assert_any_call(
            bucket_name='orders-and-ads',
            source_file_name='/Users/victoruwaje/Desktop/Personal_projects/automated_etl_pipeline/data/clean_facebook_ads.csv',
            destination_blob_name='raw/clean_facebook_ads.csv'
        )

    def tearDown(self):
        # Clean up generated files
        for file in ['../data/clean_shopify_orders.csv', '../data/clean_facebook_ads.csv']:
            if os.path.exists(file):
                os.remove(file)


if __name__ == '__main__':
    unittest.main()