# Pending comment for upstream #471 (execute_query evidence)

Attempted on 2026-09-26 20:18 UTC with `gh issue comment` and the REST `POST /repos/.../issues/471/comments`; both failed with HTTP 403 `Resource not accessible by integration` (the active GitHub CLI login is an installation token, `quuu824016[bot]`, without issue-write permission). Post the text below once a token with issue write access is available.

---

Additional evidence from a follow-up run (upstream `main` 249944eaed3ea200ed235fb6473296c7e301608a; split packages unchanged: ztd-query-mysqli-adapter 3fe31d57e65d337d53cee04a625ec8e855798181, ztd-query-mysql 969eb5d9f194f3bca1f661a99594b54ead492423, ztd-query-core f5ab3f0efdb355358b60feb61b7edfb9194fa21f; PHP 8.5.8 and 8.1.34, mysqlnd, MySQL 8.0.46):

- `ZtdMysqli::execute_query("INSERT INTO t (name) VALUES (?)", ['f'])` also returns a `mysqli_result` object, where native `mysqli::execute_query()` (PHP 8.2+) returns `true`.
- `ZtdMysqli::real_query("INSERT ...")` returns `true` as native does, so the divergence is limited to `query()` and `execute_query()`.

Retained run: https://github.com/k-kinzal/ztd-query-php-scenario (`cycles/2026/09/CYC-20260926T200050738102Z-mysqli-native-parity/`, checks `execute_query_insert` and `real_query_insert`).
