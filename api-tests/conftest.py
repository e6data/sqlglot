import sys
import os
import pytest
from fastapi.testclient import TestClient

# Add parent directory to path to import converter_api
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from converter_api import app


@pytest.fixture
def client():
    """FastAPI test client fixture"""
    return TestClient(app)


@pytest.fixture
def sample_queries():
    """Sample SQL queries for testing"""
    return {
        "simple_select": "SELECT * FROM users",
        "select_with_where": "SELECT * FROM users WHERE id > 100",
        "select_with_join": "SELECT u.*, o.total FROM users u JOIN orders o ON u.id = o.user_id",
        "select_with_cte": "WITH user_orders AS (SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id) SELECT * FROM user_orders",
        "complex_query": "SELECT u.name, COUNT(o.id) as order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.created_at > '2024-01-01' GROUP BY u.name HAVING COUNT(o.id) > 5",
        "invalid_sql": "SELECT FROM WHERE",
    }
