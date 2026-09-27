# Docker version matrix

Run from the repository root with Python 3 and Docker Compose v2+.
The host needs no PHP, Composer or database server. Dependencies are installed
from the committed `composer.lock`; the runner never updates them.

## Select versions

```sh
# Default: PHP 8.5, SQLite from Debian Bookworm
python3 scripts/docker-matrix.py

# PHP 8.1–8.5, each with its actual SQLite runtime recorded
python3 scripts/docker-matrix.py --php all

# MySQL lower bound and representative release lines
python3 scripts/docker-matrix.py --php 8.1,8.5 --database mysql --mysql 8.0.11,8.4,9.1

# PostgreSQL 16 and 17
python3 scripts/docker-matrix.py --database postgres --postgres all

# Switch the library loaded by pdo_sqlite and sqlite3
python3 scripts/docker-matrix.py --php 8.1,8.5 --sqlite 3.40.1,3.46.1

# Inspect the complete 65-cell plan without pulling/building images
python3 scripts/docker-matrix.py --all --list
# Execute it (initial builds/pulls take time and disk space)
python3 scripts/docker-matrix.py --all
```

The maintained selections live in [docker/matrix.json](../docker/matrix.json):

| Component | Selections |
| --- | --- |
| PHP | 8.1, 8.2, 8.3, 8.4, 8.5, Debian Bookworm CLI |
| MySQL | 8.0.11, 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 |
| PostgreSQL | 16, 17 |
| SQLite | `system`, 3.40.1, 3.46.1 |

These are representative versions within the repository's recorded support
range. SQLite 3.x and every MySQL/PHP patch release are **not** exhaustively
covered. `system` records the actual SQLite provided by the PHP image's OS.
MySQL 8.0.11 uses `linux/amd64` because its old image predates ARM support;
Apple Silicon needs Docker's x86 emulation. Other cells use the Docker server's
architecture. An unavailable image, failed build, or failed startup is an
explicit environment failure with a nonzero runner exit code.

MySQL/PostgreSQL use upstream container images; separate database Dockerfiles
would add no version control. SQLite runs inside PHP: the Dockerfile builds
selected SQLite source archives with a checked SHA-256, installs the shared
library, and checks **both PHP extensions' actual loaded version**. The probe
also retains SQLite compile options, since a version number alone does not
identify build features.

## Execute scenarios

By default, a cell checks the installed lock references, all five driver
extensions, requested PHP/database versions, native PDO prepared INSERT,
SELECT, UPDATE and DELETE, and a MySQLi prepared INSERT/SELECT on MySQL.
This proves the environment is usable. ZTD support requires running the
relevant user expectations.

Append a command after `--` to run it in every selected cell **after** the
native checks. `/evidence` is the per-cell output directory on the host.
The command's actual exit status is retained; failure remains nonzero and
subsequent cells still run.

```sh
# Reviewed SQLite fixture expectation, with retained JUnit
python3 scripts/docker-matrix.py --php all -- \
  php vendor/bin/phpunit tests/Pdo/SqliteFixtureLifecycleTest.php \
  --log-junit /evidence/junit.xml --colors=never --fail-on-skipped --fail-on-empty-test-suite

# Existing reviewed MySQL scenario (contains native controls)
python3 scripts/docker-matrix.py --database mysql --mysql 8.0,8.4,9.1 -- \
  php scenarios/types/SCN-mysql-product-types/scenario.php

# Exploratory legacy PostgreSQL suite: review assertions before drawing conclusions
python3 scripts/docker-matrix.py --database postgres --postgres all -- \
  php vendor/bin/phpunit --filter PostgresBasicCrudTest \
  --log-junit /evidence/junit.xml --colors=never --fail-on-skipped --fail-on-empty-test-suite
```

`MYSQL_*`, `POSTGRES_*`, and the existing curated MySQL scenarios' `ZTD_*`
connection variables point at the cell's database. There are no published
host ports or Docker socket mounts. Each cell uses a unique Compose project,
network and fresh database volumes. Logs are collected before `down --volumes`;
cleanup also runs after a failed build, failed command or Ctrl-C. The runner
retains PHP images for reuse. Cleanup failure is reported and changes the exit
status; use the recorded project/Compose command to finish cleanup manually.

## Evidence and replay

Output defaults to `build/docker-matrix/<UTC>/`. To retain a run for a cycle:

```sh
python3 scripts/docker-matrix.py --output cycles/YYYY/MM/CYC-.../environment
```

Each invocation stores a source archive and SHA-256 file manifest, Git revision,
Docker/Compose versions, selected cells and completion state. Builds use a
frozen copy of that archive, so editing the worktree while a long matrix runs
does not change later cells. Each cell stores:

- Resolved Compose configuration, exact commands and exit codes.
- Build/startup/service/cleanup logs, actual PHP/driver/server versions.
- Built PHP image ID and database image ID/digest/architecture.
- Composer lock hash and every installed ZTD package reference.
- Optional command output, version log and explicitly requested JUnit files.

`summary.json` is infrastructure evidence, not a `lab.py` behavioral baseline.
Follow [WORKFLOW.md](../WORKFLOW.md) when classifying scenario results, retaining
findings and reporting upstream. Keep the source archive and essential output
in Git for claims; `build/` alone is supplementary. The existing baseline is
not refreshed by this runner.

To replay, extract `source.tar.gz` into a clean directory and install/build
from that lock. Use the recorded image IDs if still local, or the recorded
registry digests. Tags such as `php:8.5-cli-bookworm`, `mysql:8.4`, `postgres:17`
and `composer:2` can move; the logs record their resolved build inputs, but a
future rebuild can pick up different Debian packages. Preserve/tag the built
PHP image (or `docker image save` it) when byte-identical runtime replay matters.
The saved Compose file uses the original temporary source/output paths: update
those paths when replaying on another machine.

## Direct Docker usage and extensions

The original `docker-compose.yml` services and the two legacy shell matrix
runners remain available. The new runner combines the PHP and database axes,
adds exact SQLite selection and preserves run-specific artifacts.

```sh
# Standalone image, same lock and all database drivers
# PHP_IMAGE also accepts a full tag or digest for a specific patch/runtime.
docker build --build-arg PHP_VERSION=8.2 -t ztd-local:8.2 .
docker run --rm ztd-local:8.2 tests/Pdo/SqliteFixtureLifecycleTest.php

# Inspect Compose without creating anything
docker compose -f docker/compose.matrix.yml --profile mysql config
```

Add version selections to `docker/matrix.json`. New SQLite entries need the
official source URL and its SHA-256; verify the source release before recording
the checksum. These two checksums were calculated from the linked upstream
archives on 2026-09-27. A new selection is pending coverage until actually run.

Sources: [official PHP image](https://github.com/docker-library/docs/blob/master/php/README.md),
[Compose startup order](https://docs.docker.com/compose/how-tos/startup-order/),
[SQLite source downloads](https://www.sqlite.org/download.html).
