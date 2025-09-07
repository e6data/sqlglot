#!/usr/bin/env python3
"""
Step 1: Extract original queries from local parquet for testing
"""

import pyarrow.parquet as pq
import json
import os

# Read the parquet file
print("Loading parquet file...")
table = pq.read_table("results/combined_batch_statistics.parquet")
df = table.to_pandas()

print(f"Total records: {len(df)}")

# Filter for successful queries only
successful = df[df["status"] == "success"].copy()
print(f"Successful records: {len(successful)}")

# Take 15 queries for testing
sample = successful.head(15)
print(f"Selected {len(sample)} test queries")

# Extract original queries + expected results
test_data = []
for i, row in sample.iterrows():
    test_case = {
        # Input for worker
        "original_query": row["original_query"],
        "from_dialect": row.get("from_dialect", "snowflake"),
        "to_dialect": row.get("to_dialect", "e6"),
        # Expected results from parquet
        "expected_supported_functions": row["supported_functions"]
        if isinstance(row["supported_functions"], list)
        else [],
        "expected_unsupported_functions": row["unsupported_functions"]
        if isinstance(row["unsupported_functions"], list)
        else [],
        "expected_converted_query": row["converted_query"],
        "expected_executable": row["executable"],
        "expected_tables_list": row["tables_list"] if isinstance(row["tables_list"], list) else [],
        # For debugging
        "test_id": i + 1,
    }
    test_data.append(test_case)

# Create directory and save
os.makedirs("automated_processing/tests/fixtures", exist_ok=True)

with open("automated_processing/tests/fixtures/test_queries.json", "w") as f:
    json.dump(test_data, f, indent=2, default=str)

print(f"✅ Saved {len(test_data)} test cases")
print(f"✅ File: automated_processing/tests/fixtures/test_queries.json")

# Show sample
print(f"\nSample query preview:")
print(f"Query: {test_data[0]['original_query'][:100]}...")
print(f"Expected executable: {test_data[0]['expected_executable']}")
print(f"Expected functions: {test_data[0]['expected_supported_functions'][:3]}...")
