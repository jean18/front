"""Utils functions for DAGs."""
from datetime import datetime, timedelta
from typing import Any, Dict


def get_template_searchpath() -> str:
    """Get the template searchpath arg for DAGs.

    Returns:
        The path where the templates are located.
    """
    return "/usr/local/airflow/include"


def get_default_args(start_date: datetime) -> Dict[str, Any]:
    """Get default args for DAGs.

    Args:
        `start_date`: Initial date when the DAG was created.

    Returns:
        The default args to use in DAGs.
    """
    return {
        "start_date": start_date,
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    }
