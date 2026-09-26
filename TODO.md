# Work queue

The authoritative queue is [work/items/](work/items). Each item has a stable ID, one question, priority, next action and evidence-based completion criteria.

```bash
python3 scripts/lab.py status
```

Start with [WORKFLOW.md](WORKFLOW.md). Historical TODO content was preserved byte-for-byte in [spec/legacy/TODO.md](spec/legacy/TODO.md); its open questions have been migrated to the queue. Do not maintain a second checklist here.
