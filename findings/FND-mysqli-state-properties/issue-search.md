# Issue search: ZtdMysqli: mysqli connection and statement properties throw `Error`, and `errno`/`error` stay empty after a failed `query()`

Searched open and closed issues and pull requests at https://github.com/k-kinzal/ztd-query-php/issues on 2026-09-26 between 20:03 and 20:17 UTC with `gh api search/issues?q=repo:k-kinzal/ztd-query-php+<terms>` (all states).

| Terms | Hits | Relevant |
| --- | --- | --- |
| `insert_id`, `LAST_INSERT_ID`, `LAST_INSERT_ID()`, `generated key` | #219 (closed PR: identities allocated in shadow state), #367/#241/#240 (fixtures/tests) | #219 establishes that identities are generated; it does not mention `insert_id` or `LAST_INSERT_ID()` being readable |
| `AUTO_INCREMENT` | #219 | as above |
| `multi_query`, `store_result`, `use_result`, `real_query`, `stmt_init`, `bind_result`, `num_rows`, `field_count`, `fetch_fields`, `mysqli_init`, `real_connect`, `Property access`, `already closed`, `ZtdMysqliException`, `mysqli_sql_exception`, `errno`, `1062`, `1146`, `1364`, `duplicate entry` | none | – |
| `affected_rows` | #449 (closed docs PR documenting `lastAffectedRows()`) | documents only `affected_rows` as unavailable |
| `fromMysqli`, `execute_query` | #288, #239 (closed PRs: tooling, REPLACE) | no |
| `sqlstate`, `1064`, `syntax error` | #436 and parser/faker PRs | no |
| `transaction`, `rollback`, `autocommit`, `begin_transaction` | #225 (closed PR: transaction APIs synchronized with the session) | transactions passed in this scenario; not a report of the problems below |
| `constraint`, `primary key`, `not null` | #224 (closed PR: candidate-key conflict detection), #219 | describe that conflicts are detected, not how the error surfaces |
| `code 0` | #466, #318, #394 (unrelated) | no |
| `mysqli` (53 hits) | #471, #472 (this repository's earlier reports), #469, #473 (open), closed refactor/test PRs | #471/#472 cover `query()` return type and column typing only |

PR #213 "fix: type simulation error boundaries" (closed 2026-08) states that simulation failures are translated "to the configured database exception at the session boundary"; it does not report the current behavior as a problem.

Conclusion for `FND-mysqli-state-properties`: no open or closed issue reports this behavior; a new issue is required.
