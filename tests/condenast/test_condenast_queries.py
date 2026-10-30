"""Conde Nast Qlik prod queries through the two multidialect modes of /convert-query.

queries.csv.gz holds all 924 distinct prod queries of the Qlik comparison runs (old and new
Qlik), each plain Databricks SQL with no Postgres wrapper, with columns:

- query_id (tag::hash#n -- a hash is reused across runs, n numbers its distinct texts),
  group (backtick / no_backtick), sql;
- e6: the output of subtree and multidialect (identical on all 858 both convert); for
  subtree_fixes the subtree output, for both_fail empty;
- bucket: all_match (subtree = multidialect, 858), subtree_fixes (48) or both_fail (18).

Every all_match query must keep returning e6 exactly under each mode:

- subtree: SUBQUERY_TREE on with the MULTIDIALECT feature flag (apis.utils.subquery_tree);
- multidialect: the MULTIDIALECT feature flag (split_pg_outer).

subtree_fixes are the queries subtree fixes: their Databricks-only syntax (a backtick name, a
column starting with a digit) sits in the outermost query, where split_pg_outer has no subquery
to cut out and raises "parse error not inside a subquery". Subtree reads that outer query as
Databricks and must keep returning e6, which is also the plain Databricks transpile.

SAMSUNG_TABLEAU_DASHBOARD is not a mode here: no query has the "Custom SQL Query" alias.
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
    "subtree": (True, True),
    "multidialect": (False, True),
}


@unittest.skipUnless(
    os.environ.get("MULTIDIALECT_CORPUS") == "1", "run with make test-multidialect"
)
class TestCondenastQueries(unittest.TestCase):
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
        self.assertEqual((len(self.queries), len(cases)), (924, 858))
        for mode, args in MODES.items():
            for case in cases:
                with self.subTest(mode=mode, query_id=case["query_id"]):
                    self.assertEqual(self.convert(case["sql"], *args), case["e6"])

    def test_subtree_fixes(self):
        cases = [q for q in self.queries if q["bucket"] == "subtree_fixes"]
        self.assertEqual(len(cases), 48)
        for case in cases:
            with self.subTest(query_id=case["query_id"]):
                with self.assertRaises(ValueError):
                    self.convert(case["sql"], *MODES["multidialect"])
                self.assertEqual(self.convert(case["sql"], *MODES["subtree"]), case["e6"])


if __name__ == "__main__":
    unittest.main()
