#!/usr/bin/env python3
import os
import multiprocessing
import uvicorn

def get_optimal_workers():
    """Calculate optimal workers based on available resources."""
    
    # 1. Check Kubernetes resource limits (if available)
    cpu_limit = None
    if os.path.exists("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"):
        with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us") as f:
            quota = int(f.read())
        with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us") as f:
            period = int(f.read())
        if quota > 0 and period > 0:
            cpu_limit = quota / period
    
    # 2. Check environment variable
    if os.getenv("CPU_LIMIT"):
        cpu_limit = float(os.getenv("CPU_LIMIT"))
    
    # 3. Fall back to actual CPU count
    if cpu_limit is None:
        cpu_limit = multiprocessing.cpu_count()
    
    # Calculate workers: (2 × cores) + 1, with limits
    optimal = int((2 * cpu_limit) + 1)
    
    # Apply min/max limits
    min_workers = int(os.getenv("MIN_WORKERS", "2"))
    max_workers = int(os.getenv("MAX_WORKERS", "20"))
    
    workers = max(min_workers, min(optimal, max_workers))
    
    print(f"CPU Limit: {cpu_limit:.2f}, Optimal Workers: {optimal}, Using: {workers}")
    return workers

if __name__ == "__main__":
    workers = get_optimal_workers()
    
    # Allow explicit override
    workers = int(os.getenv("UVICORN_WORKERS", workers))
    
    uvicorn.run(
        "converter_api:app",
        host="0.0.0.0",
        port=8100,
        workers=workers,
        proxy_headers=True
    )