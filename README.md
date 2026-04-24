# CockroachDB Read-After-Write Consistency Test Suite

Proves that CockroachDB's read-after-write consistency guarantees hold across independent connection pools and gateway nodes — even under aggressive timing and contention.

## Problem Statement

A setup with multiple Node.js instances (each with its own `pg.Pool`) behind a load balancer reported that 1 out of 3 concurrent reads immediately after an INSERT occasionally missed the committed row. This test suite validates that the issue is **not** in CockroachDB, but in the application/infrastructure layer.

## What It Tests

| Test | Description |
|------|-------------|
| **Basic Read-After-Write** | INSERT on a random pool, immediate reads from 3 different pools |
| **Secondary Index Reads** | Reads via secondary index instead of primary key |
| **Session State Poisoning** | Deliberately poisons a connection with `follower_reads=on` to prove pool session leakage causes stale reads |
| **High Contention** | 50 concurrent INSERT+read cycles across all pools simultaneously |
| **REGIONAL BY TABLE** | Same test on a table with regional locality matching production config |
| **Rapid-Fire Writes** | 20 concurrent INSERTs from random pools, then verifies all rows are visible |

## Requirements

```bash
npm install pg
```

Node.js v18+ recommended.

## Usage

```bash
node crdb_consistency_test_v2.js <connection_string> [--region=<region>]
```

### Examples

```bash
# Basic run
node crdb_consistency_test_v2.js "postgresql://user:pass@host:26257/db?sslmode=verify-full&sslrootcert=/path/to/ca.crt"

# With REGIONAL BY TABLE test (multi-region cluster)
node crdb_consistency_test_v2.js "postgresql://user:pass@host:26257/db?sslmode=verify-full&sslrootcert=/path/to/ca.crt" --region=aws-us-east-1
```

## Output

The script creates temporary test tables, runs all tests, prints results, and cleans up. Example output:

```
==========================================================
  FINAL SUMMARY
==========================================================

  [PASS] Basic read-after-write: 100 passed, 0 failed
  [PASS] Secondary index reads: 100 passed, 0 failed
  [FAIL] Session poisoning detection: 0 passed, 50 failed (50 stale reads from poisoned conn)
  [PASS] High contention: 200 passed, 0 failed
  [PASS] REGIONAL BY TABLE: 100 passed, 0 failed (region=aws-ca-central-1)
  [PASS] Rapid-fire writes then read-all: 10 passed, 0 failed

  VERDICT: CockroachDB is NOT the problem.
```

The **Session Poisoning** test is expected to fail — it intentionally demonstrates that a connection with `follower_reads=on` leaked in a pool causes exactly the symptom described.

## Key Findings

1. **CockroachDB guarantees hold** — all clean reads across independent pools and nodes see committed data immediately
2. **Session state leakage is dangerous** — a single poisoned connection with `follower_reads=on` causes 100% stale reads on that connection
3. **`pool.on('connect')` is not a sufficient fix** — it only fires for new connections, not when a connection is reused from the pool

## Recommended Fix

```javascript
// Reset connection state on every checkout
async function safeQuery(pool, text, params) {
  const client = await pool.connect();
  try {
    await client.query("ROLLBACK");
    return await client.query(text, params);
  } finally {
    client.release();
  }
}
```

Or better yet, use `INSERT ... RETURNING *` to avoid the write-then-read pattern entirely.
