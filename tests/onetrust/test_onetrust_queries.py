"""OneTrust AI-governance Power BI prod queries through the two multidialect modes of /convert-query.

queries.csv.gz holds all 108 prod queries (99 distinct texts), each a Power BI ``"$Table"`` outer
wrapping Databricks SQL (no backticks), sent with from_sql=databricks, with columns:

- query_id (the query alias), group (backtick / no_backtick), sql;
- e6: the output of multidialect and subtree (identical on all 108);
- bucket: all_match (multidialect = subtree, 108).

OneTrust runs the old method, the MULTIDIALECT feature flag (split_pg_outer), so its output is
the expected e6. Every query must keep returning e6 exactly under each mode:

- multidialect: the MULTIDIALECT feature flag (split_pg_outer);
- subtree: SUBQUERY_TREE on with the MULTIDIALECT feature flag (apis.utils.subquery_tree).
"""

import csv
import gzip
import json
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

QUERIES = Path(__file__).parent / "queries.csv.gz"

# name: (SUBQUERY_TREE, MULTIDIALECT flag)
MODES = {
    "multidialect": (False, True),
    "subtree": (True, True),
}


@unittest.skipUnless(
    os.environ.get("MULTIDIALECT_CORPUS") == "1", "run with make test-multidialect"
)
class TestOnetrustQueries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from fastapi.testclient import TestClient

        import converter_api

        cls.converter_api = converter_api
        cls.client = TestClient(converter_api.app)
        csv.field_size_limit(sys.maxsize)
        with gzip.open(QUERIES, "rt", encoding="utf-8", newline="") as fh:
            cls.queries = list(csv.DictReader(fh))

    def convert(self, query, subquery_tree, multidialect):
        with mock.patch.object(self.converter_api, "SUBQUERY_TREE", subquery_tree):
            response = self.client.post(
                "/convert-query",
                data={
                    "query": query,
                    "from_sql": "databricks",
                    "feature_flags": json.dumps(
                        {"PRETTY_PRINT": False, "MULTIDIALECT": multidialect}
                    ),
                },
            )
        return response.json().get("converted_query")

    def test_all_match(self):
        cases = [q for q in self.queries if q["bucket"] == "all_match"]
        self.assertEqual((len(self.queries), len(cases)), (108, 108))
        for mode, args in MODES.items():
            for case in cases:
                with self.subTest(mode=mode, query_id=case["query_id"]):
                    self.assertEqual(self.convert(case["sql"], *args), case["e6"])


if __name__ == "__main__":
    unittest.main()
