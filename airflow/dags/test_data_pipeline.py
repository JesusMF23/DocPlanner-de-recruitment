from datetime import datetime
import json
from io import BytesIO
import pytest
import polars as pl
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowException
from data_pipeline import fetch_climate_data, process_climate_data, load_data_to_postgres

def test_fetch_climate_data_success():
    kwargs = {
        'execution_date': datetime(2024, 8, 26),
        'ti': MagicMock()
    }
    with patch('data_pipeline.requests.get') as mock_get, \
         patch('data_pipeline.S3Hook') as mock_s3hook:

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'results': [{'station': 'STATION1', 'date': '2024-08-25T00:00:00', 'value': 23.5, 'datatype': 'TAVG'}]}
        mock_get.return_value = mock_response

        fetch_climate_data(**kwargs)
        
        mock_s3hook.return_value.load_string.assert_called_once()

def test_fetch_climate_data_failure():
    kwargs = {
        'execution_date': datetime(2024, 8, 26),
        'ti': MagicMock()
    }
    with patch('data_pipeline.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = 'Server error'
        mock_get.return_value = mock_response

        with pytest.raises(AirflowException):
            fetch_climate_data(**kwargs)

def test_process_climate_data():
    kwargs = {
        'ti': MagicMock()
    }
    with patch('data_pipeline.S3Hook') as mock_s3hook:
        mock_s3hook.return_value.read_key.return_value = json.dumps({
            "results": [
                {"station": "STATION1", "date": "2024-08-25T00:00:00", "value": 23.5, "datatype": "TAVG"},
                {"station": "STATION1", "date": "2024-08-25T00:00:00", "value": 15.2, "datatype": "TMIN"},
                {"station": "STATION1", "date": "2024-08-25T00:00:00", "value": 30.1, "datatype": "TMAX"},
                {"station": "STATION1", "date": "2024-08-25T00:00:00", "value": 1.2, "datatype": "PRCP"}
            ]
        })
        process_climate_data(**kwargs)
        
        mock_s3hook.return_value.load_bytes.assert_called_once()

def test_load_data_to_postgres():
    kwargs = {
        'ti': MagicMock()
    }
    with patch('data_pipeline.S3Hook') as mock_s3hook, \
         patch('data_pipeline.PostgresHook') as mock_pg_hook:

        df = pl.DataFrame({
            "station": ["STATION1"],
            "date": [datetime(2024, 8, 25)],
            "TAVG": [23.5],
            "TMIN": [15.2],
            "TMAX": [30.1],
            "PRCP": [1.2],
        })
        parquet_buffer = BytesIO()
        df.write_parquet(parquet_buffer)
        parquet_buffer.seek(0)

        mock_s3hook.return_value.get_key.return_value.get.return_value = {
            "Body": parquet_buffer
        }
        mock_pg_hook.return_value.get_conn.return_value.cursor.return_value = MagicMock()

        load_data_to_postgres(**kwargs)

        mock_pg_hook.return_value.get_conn.return_value.cursor.return_value.execute.assert_called()