# SPEC-isolated-fixture-lifecycle

## User task

Run an application fixture lifecycle against an existing schema without reading or changing physical application rows. Reuse the connection for a later test with a fresh ZTD session.

## Expectation

Starting with a physical `users` table containing `(99, 'Physical seed')`, a newly wrapped session reads no fixture rows. In that session, inserting `(1, 'Alice')`, updating the name to `Updated`, and deleting row 1 each affects one row. Reads observe each change. A native read still returns exactly the original physical seed. A fresh wrapper starts empty and also leaves the seed unchanged.

Values, PHP types, affected row counts and physical effects are asserted. The older basic CRUD suite supplements this workflow with prepared reads and enable/disable checks.

## Basis

The [public PDO adapter contract](https://github.com/k-kinzal/ztd-query-php/blob/3a6c7e361a1613a7d75288a4628058e2b3af0e67/packages/ztd-query-pdo-adapter/README.md) describes session data independent of physical rows and `fromPdo()` wrapping. Native reads establish the physical state; they are not expected to see isolated writes.

## Scope

Definition reviewed for SQLite PDO with default ZTD options and exception error mode. Other adapters, transactions, multiple simultaneous connections and pooled connections need separate runs/scenarios. Runtime coverage is recorded by each execution, not by this definition.

## Links

- [Scenario](../../scenarios/isolation/SCN-sqlite-fixture-lifecycle/scenario.json)
- Historical topics: `SPEC-1.4`, `SPEC-2.2`, `SPEC-4.1`, `SPEC-4.2`, `SPEC-4.3` (search with `lab.py catalog --legacy`).
