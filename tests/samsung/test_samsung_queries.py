"""Samsung Tableau prod queries through the three multidialect modes of /convert-query.

queries.csv.gz holds all 704 prod queries (queries_with_backticks + queries_without_backticks),
each a Postgres/Tableau outer around a Databricks "Custom SQL Query" inner, with columns:

- query_id, group (backtick / no_backtick), sql;
- e6: the output of subtree and multidialect (identical on all 704);
- samsung_e6: the Samsung output where it differs from e6, else empty;
- bucket: all_match (subtree = multidialect = samsung, 674) or samsung_differs (30: Samsung
  emits CAST(x AS DATE) where the others keep DATE(x)).

Every all_match query must keep returning e6 exactly under each mode:

- subtree: SUBQUERY_TREE on (apis.utils.subquery_tree);
- multidialect: the MULTIDIALECT feature flag (split_pg_outer);
- samsung: SAMSUNG_TABLEAU_DASHBOARD on (split_custom_sql_alias).
"""

import csv
import gzip
import json
import sys
import unittest
from pathlib import Path
from unittest import mock

QUERIES = Path(__file__).parent / "queries.csv.gz"

# name: (SUBQUERY_TREE, SAMSUNG_TABLEAU_DASHBOARD, MULTIDIALECT flag)
MODES = {
    "subtree": (True, "False", False),
    "multidialect": (False, "False", True),
    "samsung": (False, "true", False),
}


class TestSamsungQueries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from fastapi.testclient import TestClient

        import converter_api

        cls.converter_api = converter_api
        cls.client = TestClient(converter_api.app)
        csv.field_size_limit(sys.maxsize)
        with gzip.open(QUERIES, "rt", encoding="utf-8", newline="") as fh:
            cls.queries = list(csv.DictReader(fh))

    def convert(self, query, subquery_tree, samsung, multidialect):
        with mock.patch.object(self.converter_api, "SUBQUERY_TREE", subquery_tree), mock.patch.object(
            self.converter_api, "SAMSUNG_TABLEAU_DASHBOARD", samsung
        ):
            response = self.client.post(
                "/convert-query",
                data={
                    "query": query,
                    "from_sql": "postgres",
                    "feature_flags": json.dumps(
                        {"PRETTY_PRINT": False, "MULTIDIALECT": multidialect}
                    ),
                },
            )
        return response.json().get("converted_query")

    def test_all_match(self):
        cases = [q for q in self.queries if q["bucket"] == "all_match"]
        self.assertEqual((len(self.queries), len(cases)), (704, 674))
        for mode, args in MODES.items():
            for case in cases:
                with self.subTest(mode=mode, query_id=case["query_id"]):
                    self.assertEqual(self.convert(case["sql"], *args), case["e6"])


if __name__ == "__main__":
    unittest.main()
