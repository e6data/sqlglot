from unittest import mock

from sqlglot import UnsupportedError, exp, parse_one
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify_columns import quote_identifiers
from tests.dialects.test_dialect import Validator


class TestSnowflake(Validator):
    maxDiff = None
    dialect = "E6"

    def test_E6(self):
        self.validate_all(
            "TO_CHAR(column1, 'YYYY-MM-DD')",
            read={
                "snowflake": "TO_VARCHAR(column1, 'YYYY-MM-DD')",
                "databricks": "DATE_FORMAT(column1, 'yyyy-MM-dd')",
            },
            write={
                "E6": "TO_CHAR(column1, 'YYYY-MM-DD')",
                "snowflake": "TO_VARCHAR(column1, 'YYYY-MM-DD')",
                "databricks": "DATE_FORMAT(column1, 'yyyy-MM-dd')",
            }
        )