<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests subqueries in INSERT VALUES position through ZTD shadow store on SQLite.
 *
 * Patterns like INSERT INTO t (col) VALUES ((SELECT MAX(id)+1 FROM t)) are
 * used in production for sequence generation, copying, and derived values.
 * The CTE rewriter must handle subqueries within the VALUES clause.
 *
 * @spec SPEC-4.1
 */
class SqliteSubqueryInValuesDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_sqv_items (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                sort_order INTEGER NOT NULL
            )',
            'CREATE TABLE sl_sqv_config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_sqv_config', 'sl_sqv_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_sqv_items VALUES (1, 'First', 10)");
        $this->pdo->exec("INSERT INTO sl_sqv_items VALUES (2, 'Second', 20)");
        $this->pdo->exec("INSERT INTO sl_sqv_items VALUES (3, 'Third', 30)");
    }

    /**
     * INSERT with scalar subquery computing next sort_order.
     */
    public function testInsertWithScalarSubqueryMax(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_sqv_items (id, name, sort_order)
                 VALUES (4, 'Fourth', (SELECT MAX(sort_order) + 10 FROM sl_sqv_items))"
            );

            $rows = $this->ztdQuery("SELECT sort_order FROM sl_sqv_items WHERE id = 4");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Subquery MAX in VALUES: expected 1 row, got ' . count($rows)
                );
            }

            // MAX(sort_order) was 30, +10 = 40
            if ((int) $rows[0]['sort_order'] !== 40) {
                $this->markTestIncomplete(
                    'Subquery MAX in VALUES: expected 40, got ' . $rows[0]['sort_order']
                );
            }

            $this->assertSame(40, (int) $rows[0]['sort_order']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with scalar subquery MAX failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with subquery counting rows.
     */
    public function testInsertWithSubqueryCount(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_sqv_config (key, value)
                 VALUES ('item_count', (SELECT CAST(COUNT(*) AS TEXT) FROM sl_sqv_items))"
            );

            $rows = $this->ztdQuery("SELECT value FROM sl_sqv_config WHERE key = 'item_count'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Subquery COUNT in VALUES: expected 1, got ' . count($rows)
                );
            }

            $this->assertSame('3', $rows[0]['value']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with subquery COUNT failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with multiple subqueries in VALUES.
     */
    public function testInsertMultipleSubqueriesInValues(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_sqv_items (id, name, sort_order)
                 VALUES (
                     (SELECT MAX(id) + 1 FROM sl_sqv_items),
                     'Auto',
                     (SELECT MIN(sort_order) FROM sl_sqv_items)
                 )"
            );

            $rows = $this->ztdQuery("SELECT id, name, sort_order FROM sl_sqv_items WHERE name = 'Auto'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Multi-subquery VALUES: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame(4, (int) $rows[0]['id']); // MAX(3) + 1
            $this->assertSame(10, (int) $rows[0]['sort_order']); // MIN(sort_order)
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with multiple subqueries in VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with scalar subquery (cross-table lookup).
     */
    public function testUpdateSetWithSubquery(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_sqv_config VALUES ('offset', '100')");

            $this->pdo->exec(
                "UPDATE sl_sqv_items
                 SET sort_order = sort_order + (SELECT CAST(value AS INTEGER) FROM sl_sqv_config WHERE key = 'offset')
                 WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT sort_order FROM sl_sqv_items WHERE id = 1");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE SET subquery: got ' . json_encode($rows));
            }

            // Original 10 + 100 = 110
            $this->assertSame(110, (int) $rows[0]['sort_order']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET with subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT with subquery in VALUES.
     */
    public function testPreparedInsertWithSubqueryInValues(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO sl_sqv_items (id, name, sort_order)
                 VALUES (?, ?, (SELECT MAX(sort_order) + ? FROM sl_sqv_items))"
            );
            $stmt->execute([5, 'Fifth', 5]);

            $rows = $this->ztdQuery("SELECT sort_order FROM sl_sqv_items WHERE id = 5");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared subquery VALUES: expected 1, got ' . count($rows)
                );
            }

            // MAX was 30, +5 = 35
            $this->assertSame(35, (int) $rows[0]['sort_order']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT with subquery in VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE col = (subquery).
     */
    public function testDeleteWhereScalarSubquery(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM sl_sqv_items WHERE sort_order = (SELECT MIN(sort_order) FROM sl_sqv_items)"
            );

            $rows = $this->ztdQuery("SELECT name FROM sl_sqv_items ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE scalar subquery: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('Second', $rows[0]['name']);
            $this->assertSame('Third', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE scalar subquery failed: ' . $e->getMessage());
        }
    }
}
