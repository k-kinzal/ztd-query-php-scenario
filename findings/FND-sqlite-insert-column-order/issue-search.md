# Upstream disposition

Checked 2026-09-26 at approximately 09:21 UTC using:

```bash
gh issue list --repo k-kinzal/ztd-query-php --state all --search '"column" "order"' --limit 40 --json number,title,state,url
gh issue view 465 --repo k-kinzal/ztd-query-php --json url,title,state,body
```

The search returned 22 open/closed matches, including [#465](https://github.com/k-kinzal/ztd-query-php/issues/465), OPEN. Its complete body was read. It describes this exact clean SQLite positional INSERT, the same PHP/SQLite and package references, and the same wrong row/empty key lookup. Other matches included closed #54 (ALTER ADD COLUMN) and #92 (MySQL ENUM ordering); those describe different workflows.

Disposition: existing report, same observable failure and versions. Link #465; no duplicate issue or redundant comment. This cycle preserves the formerly remote-only minimal reproduction locally and confirms physical isolation. No changed affected version or newly discovered failure needs an upstream update.
