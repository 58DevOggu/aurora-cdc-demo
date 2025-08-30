"""Aurora CDC DataSource for Databricks Structured Streaming."""

__version__ = "0.1.0"

from .datasource.aurora_datasource import AuroraCDCDataSource
from .config.settings import Settings

__all__ = ["AuroraCDCDataSource", "Settings"]