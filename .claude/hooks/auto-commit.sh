#!/bin/bash
INPUT=$(cat)

# Prevent infinite loop: if already continuing from a Stop hook, let Claude stop
if [ "$(echo "$INPUT" | jq -r '.stop_hook_active')" = "true" ]; then
  exit 0
fi

# No uncommitted changes → nothing to do
if git diff --quiet && git diff --cached --quiet && [ -z "$(git ls-files --others --exclude-standard)" ]; then
  exit 0
fi

# Uncommitted changes exist → tell Claude to commit them
jq -n '{
  "decision": "block",
  "reason": "There are uncommitted changes. Please stage the relevant files and commit them with an appropriate Conventional Commits message (e.g. feat:, fix:, refactor:, docs:, test:, chore:)."
}'
