#!/usr/bin/env python3
"""Add verification matrices to spec items that don't have them yet."""

import re
import sys

MYSQL_MATRIX = """#### Verification Matrix — MySQL ({adapter})

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |
"""

PG_MATRIX = """#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |
"""

SQLITE_MATRIX = """#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |
"""


def get_mysql_adapter(platforms: str) -> "str | None":
    has_mysqli = "MySQLi" in platforms
    has_pdo = "MySQL-PDO" in platforms
    if has_mysqli and has_pdo:
        return "MySQLi, PDO"
    elif has_mysqli:
        return "MySQLi"
    elif has_pdo:
        return "PDO"
    return None


def build_matrices(platforms: str) -> str:
    parts = []
    adapter = get_mysql_adapter(platforms)
    if adapter:
        parts.append(MYSQL_MATRIX.format(adapter=adapter))
    if "PostgreSQL-PDO" in platforms:
        parts.append(PG_MATRIX)
    if "SQLite-PDO" in platforms:
        parts.append(SQLITE_MATRIX)
    return "\n".join(parts)


def process_file(path: str) -> None:
    with open(path, "r") as f:
        content = f.read()

    # Skip if already has matrices
    if "Verification Matrix" in content:
        print(f"Skipping {path}: already has matrices")
        return

    lines = content.split("\n")
    result = []
    i = 0
    current_platforms = None
    items_processed = 0

    while i < len(lines):
        line = lines[i]

        # Detect new spec item
        if line.startswith("## SPEC-"):
            # Before starting a new item, insert matrices for the previous item
            if current_platforms is not None:
                matrices = build_matrices(current_platforms)
                if matrices:
                    result.append(matrices.rstrip())
                    result.append("")
                items_processed += 1
            current_platforms = None

        # Capture platforms line
        if line.startswith("**Platforms:**"):
            current_platforms = line

        # Remove Tested versions lines
        if line.startswith("**Tested versions:**"):
            i += 1
            continue

        result.append(line)
        i += 1

    # Handle last item
    if current_platforms is not None:
        matrices = build_matrices(current_platforms)
        if matrices:
            result.append(matrices.rstrip())
            result.append("")
        items_processed += 1

    with open(path, "w") as f:
        f.write("\n".join(result))

    print(f"Processed {path}: {items_processed} items updated")


if __name__ == "__main__":
    for path in sys.argv[1:]:
        process_file(path)
