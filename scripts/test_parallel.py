"""Test parallel query execution."""

import logging
import os
import time
from pathlib import Path

# Load environment variables from .env.local
env_file = Path(__file__).parent / ".env.local"
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ[key] = value

from server.db import execute_query, execute_queries_parallel
from server.queries import (
    BILLING_SUMMARY,
    BILLING_BY_PRODUCT,
    BILLING_BY_WORKSPACE,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_default_params():
    """Get default date range params."""
    from datetime import date, timedelta
    end_date = date.today()
    start_date = end_date - timedelta(days=30)
    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }

def test_sequential():
    """Test sequential query execution."""
    print("\n" + "="*60)
    print("SEQUENTIAL EXECUTION TEST")
    print("="*60)

    params = get_default_params()
    start = time.time()

    summary = execute_query(BILLING_SUMMARY, params)
    products = execute_query(BILLING_BY_PRODUCT, params)
    workspaces = execute_query(BILLING_BY_WORKSPACE, params)

    elapsed = time.time() - start
    print(f"\n✓ Sequential execution: {elapsed:.2f}s")
    print(f"  - Summary: {len(summary)} rows")
    print(f"  - Products: {len(products)} rows")
    print(f"  - Workspaces: {len(workspaces)} rows")

    return elapsed

def test_parallel():
    """Test parallel query execution."""
    print("\n" + "="*60)
    print("PARALLEL EXECUTION TEST")
    print("="*60)

    params = get_default_params()
    start = time.time()

    queries = [
        ("summary", lambda: execute_query(BILLING_SUMMARY, params)),
        ("products", lambda: execute_query(BILLING_BY_PRODUCT, params)),
        ("workspaces", lambda: execute_query(BILLING_BY_WORKSPACE, params)),
    ]

    results = execute_queries_parallel(queries)

    elapsed = time.time() - start
    print(f"\n✓ Parallel execution: {elapsed:.2f}s")
    print(f"  - Summary: {len(results['summary'] or [])} rows")
    print(f"  - Products: {len(results['products'] or [])} rows")
    print(f"  - Workspaces: {len(results['workspaces'] or [])} rows")

    return elapsed

if __name__ == "__main__":
    # Run both tests
    seq_time = test_sequential()
    par_time = test_parallel()

    # Show speedup
    print("\n" + "="*60)
    print("RESULTS")
    print("="*60)
    print(f"Sequential: {seq_time:.2f}s")
    print(f"Parallel:   {par_time:.2f}s")
    if par_time > 0:
        speedup = seq_time / par_time
        print(f"Speedup:    {speedup:.1f}x faster")
    print("="*60)
