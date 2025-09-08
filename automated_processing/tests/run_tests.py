#!/usr/bin/env python3
"""
Simple test runner for worker integration tests
"""

import subprocess
import sys
import os


def run_integration_tests():
    """Run the integration tests and return True/False"""

    print("🚀 Running Worker Integration Tests...")
    print("-" * 50)

    # Change to tests directory
    test_dir = os.path.dirname(__file__)
    os.chdir(test_dir)

    try:
        # Run pytest with verbose output
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "test_worker_integration.py", "-v", "--tb=short"],
            capture_output=True,
            text=True,
        )

        print("STDOUT:", result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)

        if result.returncode == 0:
            print("\n🎉 ALL INTEGRATION TESTS PASSED!")
            return True
        else:
            print(f"\n❌ INTEGRATION TESTS FAILED (exit code: {result.returncode})")
            return False

    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return False


if __name__ == "__main__":
    success = run_integration_tests()
    sys.exit(0 if success else 1)
