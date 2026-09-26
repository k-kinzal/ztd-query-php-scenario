# Issue search: ZtdMysqli::query() result column types differ from native mysqli

Searched open and closed issues and pull requests at https://github.com/k-kinzal/ztd-query-php/issues on 2026-09-26 at 14:32 UTC with `gh api search/issues?q=repo:k-kinzal/ztd-query-php+<terms>`.

| Terms | Hits | Relevant |
| --- | --- | --- |
| `INT_AND_FLOAT_NATIVE` | none | – |
| `fetch_assoc` | none | – |
| `native types` | #378, #314, #317, #268, #237, #307, #221, #217 (closed PRs about internal modelling, tooling, PDO metadata, CTAS, prepared execution, ENUM) | no user report of mysqli `query()` result types |
| `returns string` | #423, #437, #206, #195, #354 (closed PRs on SQL tooling) | no |
| `mysqli int string` | none | – |
| `mysqli type` | 29 results, all PRs (#469 open design issue, #321 Renovate, various closed fixes) | none describes mysqli text-protocol string results |

PR #268 "refactor(pdo): resolve result metadata in dialects" moved mysqli result-column type resolution into database packages; it does not document an intended difference from native `mysqli::query()` string results. Conclusion: no existing report; a new issue is required.
