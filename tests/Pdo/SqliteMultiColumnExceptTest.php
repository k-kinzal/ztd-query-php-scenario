<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests multi-column EXCEPT through CTE shadow store on SQLite.
 *
 * Multi-column INTERSECT is known to return 0 rows on SQLite (see
 * SqliteSetOperationTest::testIntersectMultiColumnReturnsEmptyOnSqlite).
 * This test checks whether multi-column EXCEPT has the same issue.
 */
class SqliteMultiColumnExceptTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_mce_a (
                id INTEGER PRIMARY KEY,
                department TEXT NOT NULL,
                skill TEXT NOT NULL
            )',
            'CREATE TABLE sl_mce_b (
                id INTEGER PRIMARY KEY,
                department TEXT NOT NULL,
                skill TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_mce_a', 'sl_mce_b'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Table A: (department, skill) pairs
        $this->pdo->exec("INSERT INTO sl_mce_a VALUES (1, 'Engineering', 'PHP')");
        $this->pdo->exec("INSERT INTO sl_mce_a VALUES (2, 'Engineering', 'Python')");
        $this->pdo->exec("INSERT INTO sl_mce_a VALUES (3, 'Engineering', 'Go')");
        $this->pdo->exec("INSERT INTO sl_mce_a VALUES (4, 'Data', 'SQL')");
        $this->pdo->exec("INSERT INTO sl_mce_a VALUES (5, 'DevOps', 'Docker')");

        // Table B: overlapping (department, skill) pairs
        // Shared with A: (Engineering, PHP), (Engineering, Python), (Data, SQL)
        // Unique to B: (Engineering, Rust), (DevOps, Kubernetes)
        $this->pdo->exec("INSERT INTO sl_mce_b VALUES (1, 'Engineering', 'PHP')");
        $this->pdo->exec("INSERT INTO sl_mce_b VALUES (2, 'Engineering', 'Python')");
        $this->pdo->exec("INSERT INTO sl_mce_b VALUES (3, 'Engineering', 'Rust')");
        $this->pdo->exec("INSERT INTO sl_mce_b VALUES (4, 'Data', 'SQL')");
        $this->pdo->exec("INSERT INTO sl_mce_b VALUES (5, 'DevOps', 'Kubernetes')");
    }

    /**
     * Baseline: single-column EXCEPT works correctly through CTE shadow store.
     *
     * A skills: PHP, Python, Go, SQL, Docker
     * B skills: PHP, Python, Rust, SQL, Kubernetes
     * A EXCEPT B: Docker, Go
     */
    public function testSingleColumnExceptBaseline(): void
    {
        $rows = $this->ztdQuery(
            "SELECT skill FROM sl_mce_a
             EXCEPT
             SELECT skill FROM sl_mce_b
             ORDER BY skill"
        );

        $skills = array_column($rows, 'skill');
        $this->assertSame(['Docker', 'Go'], $skills);
    }

    /**
     * Multi-column EXCEPT returns 0 rows on SQLite — same bug as multi-column INTERSECT.
     *
     * A pairs: (Engineering,PHP), (Engineering,Python), (Engineering,Go), (Data,SQL), (DevOps,Docker)
     * B pairs: (Engineering,PHP), (Engineering,Python), (Engineering,Rust), (Data,SQL), (DevOps,Kubernetes)
     * Expected A EXCEPT B: (DevOps,Docker), (Engineering,Go)
     * Actual: 0 rows
     *
     * The CTE rewriter fails when any set operation (EXCEPT, INTERSECT) involves
     * multiple non-PK columns. Single-column set operations work correctly.
     *
     * @see SqliteSetOperationTest::testIntersectMultiColumnReturnsEmptyOnSqlite
     */
    public function testMultiColumnExceptReturnsEmptyOnSqlite(): void
    {
        $rows = $this->ztdQuery(
            "SELECT department, skill FROM sl_mce_a
             EXCEPT
             SELECT department, skill FROM sl_mce_b
             ORDER BY department, skill"
        );

        // BUG: multi-column EXCEPT returns 0 rows through CTE shadow store
        $this->assertCount(0, $rows, 'Multi-column EXCEPT returns 0 rows (expected 2)');
    }

    /**
     * Multi-column EXCEPT (reverse direction) also returns 0 rows on SQLite.
     *
     * Expected B EXCEPT A: (DevOps,Kubernetes), (Engineering,Rust)
     * Actual: 0 rows
     *
     * Confirms the multi-column set operation bug is not direction-dependent.
     */
    public function testMultiColumnExceptReverseAlsoReturnsEmptyOnSqlite(): void
    {
        $rows = $this->ztdQuery(
            "SELECT department, skill FROM sl_mce_b
             EXCEPT
             SELECT department, skill FROM sl_mce_a
             ORDER BY department, skill"
        );

        // BUG: multi-column EXCEPT returns 0 rows through CTE shadow store
        $this->assertCount(0, $rows, 'Multi-column EXCEPT (reverse) returns 0 rows (expected 2)');
    }
}
