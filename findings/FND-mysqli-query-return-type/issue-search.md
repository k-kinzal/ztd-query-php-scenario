# Issue search: ZtdMysqli::query() return value for statements without a result set

Searched open and closed issues and pull requests at https://github.com/k-kinzal/ztd-query-php/issues on 2026-09-26 between 14:29 and 14:32 UTC with `gh issue list --state all --search` and `gh api search/issues?q=repo:k-kinzal/ztd-query-php+<terms>`.

| Terms | Hits | Relevant |
| --- | --- | --- |
| `mysqli_result` | #288 (closed PR, tooling refactor) | no |
| `query returns true` | none | – |
| `affected_rows` | #449 (closed docs PR that documents `lastAffectedRows()`) | no; it does not mention the `query()` return value |
| `ZtdMysqli query return` | none | – |
| `mysqli query return` | #317, #237, #327 (closed PRs: tooling, CTAS schemas, parser) | no |
| `mysqli bool` | none | – |
| `mysqli in:title` | #317, #275, #288 (closed PRs) | no |

PR #226 "Fix simulated execution result metadata" (merged 2026-08-23) mentions exposing buffered mutation result sets through the PDO statement API and affected-row counts; it does not state that `ZtdMysqli::query()` should return a result object for DML/DDL, and no issue reports the user-visible difference. Conclusion: no existing report; a new issue is required.
