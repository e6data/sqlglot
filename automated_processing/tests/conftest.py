"""
pytest configuration for worker integration tests
"""

import pytest
import sys
import os

# Add automated_processing directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


@pytest.fixture(scope="session")
def parquet_file_path():
    """Path to the combined parquet statistics file"""
    current_dir = os.path.dirname(__file__)
    project_root = os.path.dirname(os.path.dirname(current_dir))
    return os.path.join(project_root, "results", "combined_batch_statistics.parquet")
