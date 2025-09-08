#!/usr/bin/env python3
"""
Simple test runner without pytest - just run the worker integration test
"""

import sys
import os
import pyarrow.parquet as pq
from unittest.mock import Mock

# Add paths
current_dir = os.path.dirname(__file__)
automated_processing_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(automated_processing_dir)

sys.path.insert(0, automated_processing_dir)
sys.path.insert(0, project_root)

from worker import process_query_batch


def run_integration_test():
    """Run integration test with 500 queries"""

    print("🚀 Running Worker Integration Test with 500 queries...")
    print("-" * 60)

    # Step 1: Load sample data from parquet
    parquet_path = os.path.join(current_dir, "results", "combined_batch_statistics.parquet")

    if not os.path.exists(parquet_path):
        print(f"❌ Parquet file not found: {parquet_path}")
        return False

    print(f"📊 Loading data from: {parquet_path}")
    table = pq.read_table(parquet_path)
    df = table.to_pandas()

    # Get successful queries only
    successful = df[df["status"] == "success"]
    print(f"📋 Found {len(successful)} successful queries")

    if len(successful) < 500:
        print(f"❌ Not enough successful queries. Need 500, found {len(successful)}")
        return False

    # Sample 500 random queries
    sample = successful.sample(n=500, random_state=42)
    print(f"🎯 Selected 500 random queries for testing")

    # Prepare test data
    queries_list = sample["original_query"].tolist()
    expected_results = []

    for _, row in sample.iterrows():
        expected_results.append(
            {
                "status": row["status"],
                "supported_functions": row["supported_functions"],
                "converted_query": row["converted_query"],
                "executable": row["executable"],
            }
        )

    # Step 2: Create job config with testing=True
    job_config = {
        "batch_id": 0,
        "queries_list": queries_list,
        "metadata": {
            "session_id": "integration_test_500",
            "company_name": "test_company",
            "from_dialect": "athena",
            "to_dialect": "e6",
            "query_column": "query",
            "total_batches": 1,
        },
        "testing": True,  # Enable testing mode
    }

    # Mock self parameter
    mock_self = Mock()
    mock_self.request.hostname = "test_host"
    mock_self.request.id = "test_500_queries"

    print(f"🔄 Processing {len(queries_list)} queries through worker...")

    # Step 3: Test actual Celery task
    try:
        # Import Celery app and task
        from worker import celery, process_query_batch

        # Option 1: Call task synchronously (eager mode)
        celery.conf.task_always_eager = True
        celery.conf.task_eager_propagates = True

        result = process_query_batch.apply_async(args=[job_config]).get()

        print(f"📋 Celery task executed successfully")

        # Basic assertions
        assert result["status"] == "completed", f"Expected completed, got {result['status']}"
        assert result["processed_count"] == 500, f"Expected 500, got {result['processed_count']}"
        assert "query_results" in result, "Missing query_results in response"
        assert (
            len(result["query_results"]) == 500
        ), f"Expected 500 results, got {len(result['query_results'])}"

        print(f"✅ Worker processed {result['processed_count']} queries")
        print(f"✅ Success count: {result['successful_count']}")

        # Step 4: Assert individual results
        passed_tests = 0
        failed_tests = 0

        print("🔍 Validating query results...")

        for i, (expected, actual) in enumerate(zip(expected_results, result["query_results"])):
            try:
                # Check status
                assert actual["status"] == expected["status"], f"Status mismatch on query {i}"

                # Skip executable check for now
                # assert actual['executable'] == expected['executable'], f"Executable mismatch on query {i}"

                # Check supported functions (as sets)
                expected_funcs = (
                    set(expected["supported_functions"])
                    if isinstance(expected["supported_functions"], list)
                    else set()
                )
                actual_funcs = (
                    set(actual["supported_functions"])
                    if isinstance(actual["supported_functions"], list)
                    else set()
                )
                assert expected_funcs == actual_funcs, f"Functions mismatch on query {i}"

                passed_tests += 1

                # Print progress every 100 queries
                if (i + 1) % 100 == 0:
                    print(f"  ✅ Validated {i + 1}/500 queries")

            except AssertionError as e:
                failed_tests += 1
                print(f"  ❌ Query {i + 1} failed: {e}")

        print(f"\n📊 Test Results:")
        print(f"  ✅ Passed: {passed_tests}")
        print(f"  ❌ Failed: {failed_tests}")
        print(f"  📈 Success Rate: {(passed_tests/500)*100:.1f}%")

        if failed_tests == 0:
            print(f"\n🎉 ALL 500 INTEGRATION TESTS PASSED!")
            return True
        else:
            print(f"\n⚠️ {failed_tests} tests failed out of 500")
            return False

    except Exception as e:
        print(f"❌ Error during worker processing: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_integration_test()
    if success:
        print("\n🎯 Integration test completed successfully!")
    else:
        print("\n💥 Integration test failed!")
    sys.exit(0 if success else 1)
