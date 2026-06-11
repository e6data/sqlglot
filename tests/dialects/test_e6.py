from tests.dialects.test_dialect import Validator


class TestE6(Validator):
    dialect = "E6"

    def test_variant_bracket_json_path(self):
        # A leading array index on a PARSE_JSON/variant root must fold into the
        # JSON path, NOT become ELEMENT_AT over the PARSE_JSON variant struct
        # (which throws IllegalStateException: ElementAt in the planner).
        self.validate_all(
            "SELECT CAST(JSON_EXTRACT(PARSE_JSON(c), '$[0].id') AS VARCHAR) AS x FROM t",
            read={
                "snowflake": "SELECT PARSE_JSON(c)[0]:id::TEXT AS x FROM t",
            },
        )

        # Standalone bracket on a variant root (no trailing colon path).
        self.validate_all(
            "SELECT JSON_EXTRACT(PARSE_JSON(c), '$[0]') AS x FROM t",
            read={
                "snowflake": "SELECT PARSE_JSON(c)[0] AS x FROM t",
            },
        )

        # Colon-only navigation must remain unchanged.
        self.validate_all(
            "SELECT CAST(JSON_EXTRACT(PARSE_JSON(c), '$.id') AS VARCHAR) AS x FROM t",
            read={
                "snowflake": "SELECT PARSE_JSON(c):id::TEXT AS x FROM t",
            },
        )

        # Bracket nested inside a colon path already folded correctly; keep it green.
        self.validate_all(
            "SELECT JSON_EXTRACT(c, '$.arr[0].id') FROM t",
            read={
                "snowflake": "SELECT c:arr[0]:id FROM t",
            },
        )
