#!/usr/bin/env python3
"""Test script to verify deployment configuration"""

import multiprocessing
import os
import sys


def test_worker_calculation():
    """Test the worker calculation logic"""
    print("=" * 50)
    print("TESTING WORKER CALCULATION LOGIC")
    print("=" * 50)

    # Get actual CPU count
    cpu_cores = multiprocessing.cpu_count()
    print(f"System CPU cores: {cpu_cores}")

    # Test the formula
    optimal_workers = min(max((2 * cpu_cores) + 1, 2), 20)
    print(f"Calculated workers: {optimal_workers}")

    # Test edge cases
    print("\nEdge case testing:")
    test_cases = [
        (1, 3),  # Min cores
        (2, 5),  # Low cores
        (4, 9),  # Medium cores
        (8, 17),  # High cores
        (10, 20),  # Max before cap
        (16, 20),  # Capped at 20
        (32, 20),  # Way over cap
    ]

    all_passed = True
    for cores, expected in test_cases:
        result = min(max((2 * cores) + 1, 2), 20)
        status = "✓" if result == expected else "✗"
        if result != expected:
            all_passed = False
        print(f"  {status} {cores} cores → {result} workers (expected: {expected})")

    # Test environment override
    print("\nEnvironment variable override test:")
    os.environ["UVICORN_WORKERS"] = "15"
    workers = int(os.getenv("UVICORN_WORKERS", optimal_workers))
    print(f"  UVICORN_WORKERS=15 → {workers} workers")

    # Clean up
    del os.environ["UVICORN_WORKERS"]

    return all_passed


def test_imports():
    """Test all required imports"""
    print("\n" + "=" * 50)
    print("TESTING REQUIRED IMPORTS")
    print("=" * 50)

    required_modules = [
        "fastapi",
        "uvicorn",
        "sqlglot",
        "multiprocessing",
        "os",
        "json",
        "logging",
        "datetime",
    ]

    all_imported = True
    for module in required_modules:
        try:
            __import__(module)
            print(f"  ✓ {module}")
        except ImportError as e:
            print(f"  ✗ {module}: {e}")
            all_imported = False

    return all_imported


def test_config_consistency():
    """Check configuration consistency"""
    print("\n" + "=" * 50)
    print("CONFIGURATION CONSISTENCY CHECK")
    print("=" * 50)

    issues = []

    # Check converter_api.py
    with open("converter_api.py", "r") as f:
        content = f.read()
        if 'host="0.0.0.0"' in content:
            print("  ✓ converter_api.py uses host='0.0.0.0'")
        else:
            print("  ✗ converter_api.py not using host='0.0.0.0'")
            issues.append("Host configuration")

        if "multiprocessing.cpu_count()" in content:
            print("  ✓ Dynamic worker calculation present")
        else:
            print("  ✗ Dynamic worker calculation missing")
            issues.append("Worker calculation")

    # Check Dockerfile
    with open("Dockerfile", "r") as f:
        content = f.read()
        if 'CMD ["python", "converter_api.py"]' in content:
            print("  ✓ Dockerfile CMD configured correctly")
        else:
            print("  ✗ Dockerfile CMD misconfigured")
            issues.append("Dockerfile CMD")

        if "EXPOSE 8100" in content:
            print("  ✓ Port 8100 exposed")
        else:
            print("  ✗ Port 8100 not exposed")
            issues.append("Port exposure")

    return len(issues) == 0


def main():
    """Run all tests"""
    print("\n🚀 DEPLOYMENT CONFIGURATION VERIFICATION\n")

    results = {
        "Worker Calculation": test_worker_calculation(),
        "Import Check": test_imports(),
        "Config Consistency": test_config_consistency(),
    }

    print("\n" + "=" * 50)
    print("FINAL RESULTS")
    print("=" * 50)

    all_passed = True
    for test_name, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"  {test_name}: {status}")
        if not passed:
            all_passed = False

    if all_passed:
        print("\n✅ All checks passed! Deployment configuration is correct.")
        return 0
    else:
        print("\n❌ Some checks failed. Please review the configuration.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
