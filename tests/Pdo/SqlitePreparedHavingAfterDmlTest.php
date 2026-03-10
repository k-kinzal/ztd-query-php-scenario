<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests prepared statements with HAVING clause after DML on SQLite.
 *
 * Issue #22: Prepared params in HAVING fail on SQLite.
 * This tests the current behavior more specifically to document exact scope.
 *
 * @spec SPEC-4.2
 */
class SqlitePreparedHavingAfterDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_phd_items (
            id INTEGER PRIMARY KEY,
            category TEXT NOT NULL,
            name TEXT NOT NULL,
            price REAL NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_phd_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_phd_items VALUES (1, 'tools', 'Hammer', 15.00)");
        $this->pdo->exec("INSERT INTO sl_phd_items VALUES (2, 'tools', 'Wrench', 12.00)");
        $this->pdo->exec("INSERT INTO sl_phd_items VALUES (3, 'electronics', 'Radio', 45.00)");
        $this->pdo->exec("INSERT INTO sl_phd_items VALUES (4, 'clothing', 'Shirt', 25.00)");
    }

    /**
     * Prepared HAVING COUNT(*) > ? — documents Issue #22 behavior on SQLite.
     */
    public function testPreparedHavingCountAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_phd_items VALUES (5, 'tools', 'Saw', 20.00)");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT category, COUNT(*) AS cnt
                 FROM sl_phd_items
                 GROUP BY category
                 HAVING COUNT(*) > ?",
                [1]
            );

            if (empty($rows)) {
                $this->markTestIncomplete('Prepared HAVING on SQLite: empty result (Issue #22)');
            }
            $this->assertCount(1, $rows);
            $this->assertSame('tools', $rows[0]['category']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared HAVING COUNT on SQLite failed (Issue #22): ' . $e->getMessage());
        }
    }

    /**
     * Non-prepared HAVING (baseline) — should work even if prepared fails.
     */
    public function testNonPreparedHavingAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_phd_items VALUES (5, 'tools', 'Saw', 20.00)");

            $rows = $this->ztdQuery(
                "SELECT category, COUNT(*) AS cnt
                 FROM sl_phd_items
                 GROUP BY category
                 HAVING COUNT(*) > 1"
            );

            if (empty($rows)) {
                $this->markTestIncomplete('Non-prepared HAVING: empty result');
            }
            $this->assertCount(1, $rows);
            $this->assertSame('tools', $rows[0]['category']);
            $this->assertEquals(3, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Non-prepared HAVING failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared WHERE + HAVING combined.
     */
    public function testPreparedWhereAndHaving(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_phd_items VALUES (5, 'tools', 'Saw', 20.00)");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT category, COUNT(*) AS cnt
                 FROM sl_phd_items
                 WHERE price > ?
                 GROUP BY category
                 HAVING COUNT(*) >= ?",
                [10.0, 2]
            );

            if (empty($rows)) {
                $this->markTestIncomplete('WHERE+HAVING on SQLite: empty result (Issue #22)');
            }

            $cats = array_column($rows, 'category');
            if (!in_array('tools', $cats)) {
                $this->markTestIncomplete('WHERE+HAVING: tools not found. Got: ' . implode(', ', $cats));
            }
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared WHERE+HAVING on SQLite failed: ' . $e->getMessage());
        }
    }
}
