import unittest
from unittest.mock import Mock, patch
from google.cloud import bigquery
from scripts.custom_errors import UploadToBigQueryFailedError
from scripts.load_clean_data_to_bigquery import (load_to_bigquery, schema_shopify, schema_facebook,
                         uri_shopify, uri_facebook, table_id_shopify,
                         table_id_facebook)

class TestBigQueryLoader(unittest.TestCase):

    def setUp(self):
        # Setup patchers for mocking
        self.client_patcher = patch('google.cloud.bigquery.Client')
        self.logger_patcher = patch('logging.getLogger')

        # Start the patches
        self.mock_client = self.client_patcher.start()
        self.mock_logger = self.logger_patcher.start()

        # Configure mock returns
        self.mock_client_instance = self.mock_client.return_value
        self.mock_load_job = Mock()
        self.mock_client_instance.load_table_from_uri.return_value = self.mock_load_job
        self.mock_logger_instance = self.mock_logger.return_value

    def tearDown(self):
        # Stop the patches
        self.client_patcher.stop()
        self.logger_patcher.stop()

    def test_load_to_bigquery_failure(self):
        # Arrange
        self.mock_load_job.result.side_effect = Exception("BigQuery error")

        # Act & Assert
        with self.assertRaises(UploadToBigQueryFailedError):
            load_to_bigquery(
                schema=schema_facebook,
                uri=uri_facebook,
                table_id=table_id_facebook
            )

    def test_load_to_bigquery_job_config(self):
        # Arrange
        self.mock_load_job.result.return_value = None

        # Act
        load_to_bigquery(
            schema=schema_shopify,
            uri=uri_shopify,
            table_id=table_id_shopify
        )

        # Assert
        call_args = self.mock_client_instance.load_table_from_uri.call_args
        job_config = call_args[1]['job_config']

        self.assertEqual(job_config.schema, schema_shopify)
        self.assertEqual(job_config.skip_leading_rows, 1)
        self.assertEqual(job_config.source_format, bigquery.SourceFormat.CSV)
        self.assertFalse(job_config.autodetect)

    def test_load_to_bigquery_shopify_schema(self):
        # Arrange
        self.mock_load_job.result.return_value = None

        # Act
        load_to_bigquery(
            schema=schema_shopify,
            uri=uri_shopify,
            table_id=table_id_shopify
        )

        # Assert
        self.mock_client_instance.load_table_from_uri.assert_called_once_with(
            uri_shopify,
            table_id_shopify,
            job_config=self.mock_client_instance.load_table_from_uri.call_args[1]['job_config']
        )

    def test_load_to_bigquery_facebook_schema(self):
        # Arrange
        self.mock_load_job.result.return_value = None

        # Act
        load_to_bigquery(
            schema=schema_facebook,
            uri=uri_facebook,
            table_id=table_id_facebook
        )

        # Assert
        self.mock_client_instance.load_table_from_uri.assert_called_once_with(
            uri_facebook,
            table_id_facebook,
            job_config=self.mock_client_instance.load_table_from_uri.call_args[1]['job_config']
        )


if __name__ == '__main__':
    unittest.main()