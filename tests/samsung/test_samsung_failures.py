"""Samsung Tableau prod queries that failed with PARSING_FAILED, through /convert-query with the
Samsung cluster flags (SUBQUERY_TREE + STRIP_E_STRING_PREFIX + STRIP_STRING_NEWLINES, native
executor).

failures.csv.gz holds the 30 distinct queries (858 failures) of the 23 Oct, 3 Nov, 19 Nov and
20 Nov query-history exports, with columns:

- query_id (one failing run), day, failures (runs of that text), error (the engine error), sql;
- e6: the expected output.

The failures were:

- Tableau parameters bound as Postgres escape strings E'...\\n' (23 Oct, 19 Nov: the engine got
  raw backticks; 20 Nov: the parameter came out bare with a raw \\n);
- CURRENT_DATE + -2 * INTERVAL '1 DAY' emitted as DATE_ADD('DAY', <interval>, ...) (3 Nov).

Every query must convert to e6 exactly, with no \\n escape and no E'...' left. The flags are read
at import, so the queries run in a subprocess.
"""

import csv
import gzip
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

FAILURES = Path(__file__).parent / "failures.csv.gz"

FLAGS = {
    "E6_EXECUTOR_TYPE": "native",
    "SUBQUERY_TREE": "true",
    "STRIP_E_STRING_PREFIX": "true",
    "STRIP_STRING_NEWLINES": "true",
}

CODE = """
import json, logging, sys
logging.disable(logging.CRITICAL)
from fastapi.testclient import TestClient
import converter_api
client = TestClient(converter_api.app)
out = []
for query in json.load(sys.stdin):
    response = client.post("/convert-query", data={"query": query, "from_sql": "postgres",
        "feature_flags": json.dumps({"PRETTY_PRINT": False})})
    out.append(response.json().get("converted_query"))
print(json.dumps(out))
"""


@unittest.skipUnless(
    os.environ.get("MULTIDIALECT_CORPUS") == "1", "run with make test-multidialect"
)
class TestSamsungFailures(unittest.TestCase):
    def test_failures(self):
        csv.field_size_limit(sys.maxsize)
        with gzip.open(FAILURES, "rt", encoding="utf-8", newline="") as fh:
            cases = list(csv.DictReader(fh))
        self.assertEqual((len(cases), sum(int(case["failures"]) for case in cases)), (30, 858))

        env = {
            **{k: v for k, v in os.environ.items() if k != "SAMSUNG_TABLEAU_DASHBOARD"},
            **FLAGS,
        }
        result = subprocess.run(
            [sys.executable, "-c", CODE],
            input=json.dumps([case["sql"] for case in cases]),
            capture_output=True,
            text=True,
            env=env,
            cwd=Path(__file__).parents[2],
        )
        # (importing converter_api prints a storage-client banner first, so take the last line)
        outputs = json.loads(result.stdout.splitlines()[-1]) if result.stdout else None
        self.assertIsNotNone(outputs, result.stderr)

        for case, output in zip(cases, outputs):
            with self.subTest(day=case["day"], query_id=case["query_id"]):
                self.assertEqual(output, case["e6"])
                self.assertNotIn("\\n", output)
                self.assertNotIn("E'", output)


if __name__ == "__main__":
    unittest.main()
