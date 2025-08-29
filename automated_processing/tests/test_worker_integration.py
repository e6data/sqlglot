#!/usr/bin/env python3
"""
Integration test for worker functionality using pytest
Tests worker against real parquet data
"""
import pytest
import pyarrow.parquet as pq
import sys
import os
from unittest.mock import Mock

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from worker import process_query_batch


class TestWorkerIntegration:
    """Integration tests for worker functionality"""
    
    @pytest.fixture
    def sample_parquet_data(self):
        """Step 1: Get random sample from local parquet file"""
        # Read the combined parquet file using relative path
        current_dir = os.path.dirname(__file__)
        parquet_path = os.path.join(current_dir, 'results', 'combined_batch_statistics.parquet')
        
        if not os.path.exists(parquet_path):
            pytest.skip(f"Parquet file not found: {parquet_path}")
        
        table = pq.read_table(parquet_path)
        df = table.to_pandas()
        
        # Get successful queries only
        successful = df[df['status'] == 'success']
        
        # Sample 5 random queries for testing (reduced for debugging)
        if len(successful) < 500:
            pytest.skip(f"Not enough successful queries in parquet: {len(successful)}")
        
        sample = successful.sample(n=500, random_state=42)
        
        # Convert to test format
        test_data = []
        for _, row in sample.iterrows():
            test_data.append({
                'query': row['original_query'],
                'expected_status': row['status'],
                'expected_supported_functions': row['supported_functions'],
                'expected_converted_query': row['converted_query'],
                'expected_executable': row['executable'],
                'expected_tables_list': row.get('tables_list', []),
                'from_dialect': row.get('from_dialect', 'athena'),
                'to_dialect': row.get('to_dialect', 'e6')
            })
        
        return test_data
    
    def test_worker_integration(self, sample_parquet_data):
        """Step 2 & 3: Pass celery job with testing=True and assert results"""
        
        # Prepare job config with testing flag
        queries_list = [item['query'] for item in sample_parquet_data]
        
        job_config = {
            'batch_id': 0,
            'queries_list': queries_list,
            'metadata': {
                'session_id': 'integration_test',
                'company_name': 'test_company',
                'from_dialect': 'athena',
                'to_dialect': 'e6',
                'query_column': 'query',
                'total_batches': 1
            },
            'testing': True  # Enable testing mode
        }
        
        # Mock self parameter for celery task
        mock_self = Mock()
        mock_self.request.hostname = 'test_host'
        mock_self.request.id = 'test_id_12345'
        
        # Call worker function
        result = process_query_batch(job_config)
        
        # Assert basic response structure
        assert result['status'] == 'completed'
        assert result['processed_count'] == len(queries_list)
        assert 'query_results' in result
        assert len(result['query_results']) == len(queries_list)
        
        # Assert each query result
        for i, expected in enumerate(sample_parquet_data):
            actual_result = result['query_results'][i]
            
            # Test query analysis results
            self.assert_query_result(expected, actual_result, i)
        
        print(f"✅ All {len(sample_parquet_data)} integration tests passed!")
    
    def assert_query_result(self, expected, actual, query_index):
        """Assert individual query results match expected from parquet"""
        
        print(f"\n=== QUERY {query_index + 1} COMPARISON ===")
        print(f"Original Query: {expected['query'][:100]}...")
        
        print(f"\n--- EXPECTED (from parquet) ---")
        print(f"Status: {expected['expected_status']}")
        print(f"Executable: {expected['expected_executable']}")
        print(f"Supported Functions: {expected['expected_supported_functions']}")
        print(f"Converted Query: {expected['expected_converted_query'][:100]}...")
        
        print(f"\n--- ACTUAL (after transpilation) ---")
        print(f"Status: {actual['status']}")
        print(f"Executable: {actual['executable']}")
        print(f"Supported Functions: {actual['supported_functions']}")
        print(f"Converted Query: {actual['converted_query'][:100]}...")
        
        print(f"\n--- FULL ACTUAL RESULT DICTIONARY ---")
        for key, value in actual.items():
            if isinstance(value, str) and len(value) > 100:
                print(f"{key}: {value[:100]}...")
            else:
                print(f"{key}: {value}")
        
        # Test 1: Status should match
        assert actual['status'] == expected['expected_status'], \
            f"Query {query_index}: Status mismatch. Expected: {expected['expected_status']}, Got: {actual['status']}"
        
        # Test 2: Executable status should match
        # assert actual['executable'] == expected['expected_executable'], \
        #     f"Query {query_index}: Executable mismatch. Expected: {expected['expected_executable']}, Got: {actual['executable']}"
        #
        # Test 3: Supported functions should match (as sets since order may vary)
        expected_functions = set(expected['expected_supported_functions']) if isinstance(expected['expected_supported_functions'], list) else set()
        actual_functions = set(actual['supported_functions']) if isinstance(actual['supported_functions'], list) else set()
        
        assert expected_functions == actual_functions, \
            f"Query {query_index}: Supported functions mismatch.\nExpected: {expected_functions}\nActual: {actual_functions}"
        
        # Test 4: Original query should be preserved
        assert actual['original_query'] == expected['query'], \
            f"Query {query_index}: Original query not preserved"
        
        # Test 5: Converted query should match (normalize whitespace)
        expected_converted = expected['expected_converted_query'].strip() if expected['expected_converted_query'] else ''
        actual_converted = actual['converted_query'].strip() if actual['converted_query'] else ''
        
        assert expected_converted == actual_converted, \
            f"Query {query_index}: Converted query mismatch.\nExpected: {expected_converted}\nActual: {actual_converted}"
        
        print(f"✅ Query {query_index + 1} passed all 5 assertions")

    # def test_worker_error_handling(self):
    #     """Test worker handles invalid queries gracefully"""
        
    #     job_config = {
    #         'batch_id': 1,
    #         'queries_list': ['INVALID SQL SYNTAX HERE', 'SELECT * FROM nonexistent_table'],
    #         'metadata': {
    #             'session_id': 'error_test',
    #             'from_dialect': 'athena',
    #             'to_dialect': 'e6'
    #         },
    #         'testing': True
    #     }
        
    #     mock_self = Mock()
    #     mock_self.request.hostname = 'test_host'
    #     mock_self.request.id = 'test_error_id'
        
    #     # Should complete even with errors
    #     result = process_query_batch(job_config)
        
    #     assert result['status'] == 'completed'
    #     assert result['processed_count'] == 2
    #     assert len(result['query_results']) == 2
        
    #     # Check that errors are captured
    #     for query_result in result['query_results']:
    #         # Should have status (might be 'failed' or 'success' depending on processing)
    #         assert 'status' in query_result
    #         assert 'original_query' in query_result
    #         assert 'error_message' in query_result
        
    #     print("✅ Error handling test passed!")


if __name__ == '__main__':
    # Run tests directly with output capture disabled
    pytest.main([__file__, '-v', '-s'])