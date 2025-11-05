import pytest


class TestInlineTranspile:
    """Tests for /api/v1/inline/transpile endpoint"""

    def test_transpile_valid_request(self, client, sample_queries):
        """Test transpiling a valid simple query"""
        response = client.post(
            "/api/v1/inline/transpile",
            json={
                "query": sample_queries["simple_select"],
                "from_sql": "databricks",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert "transpiled_query" in data
        assert data["source_dialect"] == "databricks"
        assert data["target_dialect"] == "e6"
        assert len(data["transpiled_query"]) > 0

    def test_transpile_databricks_to_e6(self, client, sample_queries):
        """Test Databricks to E6 transpilation"""
        response = client.post(
            "/api/v1/inline/transpile",
            json={
                "query": sample_queries["select_with_where"],
                "from_sql": "databricks",
                "to_sql": "e6",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert "SELECT" in data["transpiled_query"]
        assert "FROM" in data["transpiled_query"]

    def test_transpile_snowflake_to_e6(self, client, sample_queries):
        """Test Snowflake to E6 transpilation"""
        response = client.post(
            "/api/v1/inline/transpile",
            json={
                "query": sample_queries["simple_select"],
                "from_sql": "snowflake",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["source_dialect"] == "snowflake"

    def test_transpile_bigquery_to_e6(self, client, sample_queries):
        """Test BigQuery to E6 transpilation"""
        response = client.post(
            "/api/v1/inline/transpile",
            json={
                "query": sample_queries["simple_select"],
                "from_sql": "bigquery",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["source_dialect"] == "bigquery"

    def test_transpile_with_pretty_print_enabled(self, client, sample_queries):
        """Test transpilation with pretty_print option"""
        response = client.post(
            "/api/v1/inline/transpile",
            json={
                "query": sample_queries["select_with_join"],
                "from_sql": "databricks",
                "options": {"pretty_print": True},
            },
        )

        assert response.status_code == 200
        data = response.json()
        # Pretty printed SQL should have newlines
        assert "\n" in data["transpiled_query"]

    def test_transpile_with_pretty_print_disabled(self, client, sample_queries):
        """Test transpilation without pretty_print"""
        response = client.post(
            "/api/v1/inline/transpile",
            json={
                "query": sample_queries["simple_select"],
                "from_sql": "databricks",
                "options": {"pretty_print": False},
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert "transpiled_query" in data

    def test_transpile_with_custom_query_id(self, client, sample_queries):
        """Test transpilation with custom query_id"""
        response = client.post(
            "/api/v1/inline/transpile",
            json={
                "query": sample_queries["simple_select"],
                "from_sql": "databricks",
                "query_id": "test-query-123",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["query_id"] == "test-query-123"

    def test_transpile_missing_query_field(self, client):
        """Test that missing query field returns 422"""
        response = client.post(
            "/api/v1/inline/transpile",
            json={
                "from_sql": "databricks",
            },
        )

        assert response.status_code == 422
        data = response.json()
        assert "detail" in data
        # Check error mentions missing query field
        errors = data["detail"]
        assert any("query" in str(error).lower() for error in errors)

    def test_transpile_missing_from_sql_field(self, client, sample_queries):
        """Test that missing from_sql field returns 422"""
        response = client.post(
            "/api/v1/inline/transpile",
            json={
                "query": sample_queries["simple_select"],
            },
        )

        assert response.status_code == 422
        data = response.json()
        assert "detail" in data
        errors = data["detail"]
        assert any("from_sql" in str(error).lower() for error in errors)

    def test_transpile_empty_query_string(self, client):
        """Test that empty query string returns 422"""
        response = client.post(
            "/api/v1/inline/transpile",
            json={
                "query": "",
                "from_sql": "databricks",
            },
        )

        # Pydantic validation should return 422 for empty string (min_length=1)
        assert response.status_code == 422

    def test_transpile_whitespace_only_query(self, client):
        """Test that whitespace-only query returns 400"""
        response = client.post(
            "/api/v1/inline/transpile",
            json={
                "query": "   \n\t  ",
                "from_sql": "databricks",
            },
        )

        assert response.status_code in [400, 500]

    def test_transpile_complex_query(self, client, sample_queries):
        """Test transpiling a complex query with CTEs"""
        response = client.post(
            "/api/v1/inline/transpile",
            json={
                "query": sample_queries["select_with_cte"],
                "from_sql": "databricks",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert "WITH" in data["transpiled_query"] or "with" in data["transpiled_query"]


class TestInlineAnalyze:
    """Tests for /api/v1/inline/analyze endpoint"""

    def test_analyze_valid_request(self, client, sample_queries):
        """Test analyzing a valid query"""
        response = client.post(
            "/api/v1/inline/analyze",
            json={
                "query": sample_queries["simple_select"],
                "from_sql": "databricks",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert "transpiled_query" in data
        assert "executable" in data
        assert "functions" in data
        assert "metadata" in data

    def test_analyze_returns_functions_analysis(self, client, sample_queries):
        """Test that analyze returns function compatibility info"""
        response = client.post(
            "/api/v1/inline/analyze",
            json={
                "query": sample_queries["complex_query"],
                "from_sql": "databricks",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert "functions" in data
        assert "supported" in data["functions"]
        assert "unsupported" in data["functions"]
        assert isinstance(data["functions"]["supported"], list)
        assert isinstance(data["functions"]["unsupported"], list)

    def test_analyze_returns_metadata(self, client, sample_queries):
        """Test that analyze returns query metadata"""
        response = client.post(
            "/api/v1/inline/analyze",
            json={
                "query": sample_queries["select_with_join"],
                "from_sql": "databricks",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert "metadata" in data
        metadata = data["metadata"]
        assert "tables" in metadata
        assert "joins" in metadata
        assert "ctes" in metadata
        assert "udfs" in metadata
        assert isinstance(metadata["tables"], list)
        assert isinstance(metadata["joins"], list)

    def test_analyze_detects_tables(self, client, sample_queries):
        """Test that analyze correctly identifies tables"""
        response = client.post(
            "/api/v1/inline/analyze",
            json={
                "query": sample_queries["select_with_join"],
                "from_sql": "databricks",
            },
        )

        assert response.status_code == 200
        data = response.json()
        tables = data["metadata"]["tables"]
        # Should detect both users and orders tables
        assert len(tables) >= 2

    def test_analyze_detects_joins(self, client, sample_queries):
        """Test that analyze correctly identifies joins"""
        response = client.post(
            "/api/v1/inline/analyze",
            json={
                "query": sample_queries["select_with_join"],
                "from_sql": "databricks",
            },
        )

        assert response.status_code == 200
        data = response.json()
        joins = data["metadata"]["joins"]
        # Should detect the JOIN
        assert len(joins) > 0

    def test_analyze_detects_ctes(self, client, sample_queries):
        """Test that analyze correctly identifies CTEs"""
        response = client.post(
            "/api/v1/inline/analyze",
            json={
                "query": sample_queries["select_with_cte"],
                "from_sql": "databricks",
            },
        )

        assert response.status_code == 200
        data = response.json()
        ctes = data["metadata"]["ctes"]
        # Should detect the CTE
        assert len(ctes) > 0

    def test_analyze_executable_flag(self, client, sample_queries):
        """Test that executable flag is set correctly"""
        response = client.post(
            "/api/v1/inline/analyze",
            json={
                "query": sample_queries["simple_select"],
                "from_sql": "databricks",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data["executable"], bool)

    def test_analyze_missing_query_field(self, client):
        """Test that missing query field returns 422"""
        response = client.post(
            "/api/v1/inline/analyze",
            json={
                "from_sql": "databricks",
            },
        )

        assert response.status_code == 422

    def test_analyze_missing_from_sql_field(self, client, sample_queries):
        """Test that missing from_sql field returns 422"""
        response = client.post(
            "/api/v1/inline/analyze",
            json={
                "query": sample_queries["simple_select"],
            },
        )

        assert response.status_code == 422
