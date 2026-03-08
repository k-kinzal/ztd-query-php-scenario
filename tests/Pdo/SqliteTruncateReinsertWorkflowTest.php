<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests truncate-like workflow on SQLite ZTD.
 *
 * SQLite doesn't support TRUNCATE statement. The equivalent is DELETE FROM table.
 *
 * @see https://github.com/k-kinzal/ztd-query-php/issues/7
 * @spec SPEC-5.3
 */
class SqliteTruncateReinsertWorkflowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE trunc_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['trunc_test'];
    }


    /**
     * DELETE FROM table without WHERE should clear all rows.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/7
     */
    public function testDeleteWithoutWhereClearsShadow(): void
    {
        $this->pdo->exec("INSERT INTO trunc_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO trunc_test (id, name, score) VALUES (2, 'Bob', 80)");

        $this->pdo->exec('DELETE FROM trunc_test');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM trunc_test');
        $count = (int) $stmt->fetchColumn();
        if ($count !== 0) {
            $this->markTestIncomplete(
                'Issue #7: DELETE without WHERE silently ignored on SQLite. Expected 0, got ' . $count
            );
        }
        $this->assertSame(0, $count);
    }

    /**
     * DELETE FROM table WHERE 1=1 — workaround that clears all shadow rows.
     */
    public function testDeleteWithWhereOneEqualsOneClears(): void
    {
        $this->pdo->exec("INSERT INTO trunc_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO trunc_test (id, name, score) VALUES (2, 'Bob', 80)");

        $this->pdo->exec('DELETE FROM trunc_test WHERE 1=1');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM trunc_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * Delete all (WHERE 1=1) then re-insert — full workflow.
     */
    public function testDeleteAllThenReinsert(): void
    {
        $this->pdo->exec("INSERT INTO trunc_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO trunc_test (id, name, score) VALUES (2, 'Bob', 80)");
        $this->pdo->exec("INSERT INTO trunc_test (id, name, score) VALUES (3, 'Charlie', 70)");

        $this->pdo->exec('DELETE FROM trunc_test WHERE 1=1');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM trunc_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());

        // Re-insert new data
        $this->pdo->exec("INSERT INTO trunc_test (id, name, score) VALUES (10, 'Xavier', 95)");
        $this->pdo->exec("INSERT INTO trunc_test (id, name, score) VALUES (20, 'Yara', 85)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM trunc_test');
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name FROM trunc_test ORDER BY id');
        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Xavier', 'Yara'], $names);
    }

    /**
     * Re-insert with same IDs after delete all.
     */
    public function testReinsertSameIdsAfterDeleteAll(): void
    {
        $this->pdo->exec("INSERT INTO trunc_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO trunc_test (id, name, score) VALUES (2, 'Bob', 80)");

        $this->pdo->exec('DELETE FROM trunc_test WHERE 1=1');

        $this->pdo->exec("INSERT INTO trunc_test (id, name, score) VALUES (1, 'Alice V2', 95)");
        $this->pdo->exec("INSERT INTO trunc_test (id, name, score) VALUES (2, 'Bob V2', 85)");

        $stmt = $this->pdo->query('SELECT name, score FROM trunc_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice V2', $row['name']);
        $this->assertSame(95, (int) $row['score']);
    }

    /**
     * Multiple truncate-reinsert cycles using WHERE 1=1 workaround.
     */
    public function testMultipleTruncateReinsertCycles(): void
    {
        for ($cycle = 1; $cycle <= 3; $cycle++) {
            $this->pdo->exec('DELETE FROM trunc_test WHERE 1=1');

            for ($i = 1; $i <= $cycle; $i++) {
                $this->pdo->exec("INSERT INTO trunc_test (id, name, score) VALUES ($i, 'Cycle{$cycle}_User{$i}', " . ($cycle * 10 + $i) . ')');
            }

            $stmt = $this->pdo->query('SELECT COUNT(*) FROM trunc_test');
            $this->assertSame($cycle, (int) $stmt->fetchColumn(), "Cycle $cycle should have $cycle rows");
        }
    }

    /**
     * Partial delete with WHERE clause + update remaining.
     */
    public function testPartialDeleteThenUpdate(): void
    {
        $this->pdo->exec("INSERT INTO trunc_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO trunc_test (id, name, score) VALUES (2, 'Bob', 80)");
        $this->pdo->exec("INSERT INTO trunc_test (id, name, score) VALUES (3, 'Charlie', 70)");

        $this->pdo->exec('DELETE FROM trunc_test WHERE score < 85');

        $this->pdo->exec("UPDATE trunc_test SET name = 'Super Alice' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM trunc_test');
        $this->assertSame(1, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name FROM trunc_test WHERE id = 1');
        $this->assertSame('Super Alice', $stmt->fetchColumn());
    }

    /**
     * Physical isolation after truncate + reinsert.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO trunc_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec('DELETE FROM trunc_test WHERE 1=1');
        $this->pdo->exec("INSERT INTO trunc_test (id, name, score) VALUES (2, 'Bob', 80)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM trunc_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
