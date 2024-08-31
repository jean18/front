"""Script to test DAG utils functions."""
from datetime import datetime, timedelta
from typing import Any, Dict
from unittest import TestCase

import include.scripts.commons.dag_utils as dag_utils


class TestDagUtils(TestCase):
    """Test DAG Utils functions."""

    def setUp(self) -> None:
        """Set up test properties."""
        self.start_date: datetime = datetime(2024, 8, 29)

    def test_get_template_searchpath(self) -> None:
        """Test for get_template_searchpath function."""
        response: str = dag_utils.get_template_searchpath()
        expected_response: str = "/usr/local/airflow/include"

        assert response == expected_response

    def test_get_default_args(self) -> None:
        """Test for get_default_args function."""
        response: Dict[str, Any] = dag_utils.get_default_args(
            start_date=self.start_date
        )
        expected_response: Dict[str, Any] = {
            "start_date": self.start_date,
            "retries": 3,
            "retry_delay": timedelta(minutes=1),
            "retry_exponential_backoff": True,
        }

        assert response.keys() == expected_response.keys()
        assert all(response[key] == expected_response[key] for key in response)
