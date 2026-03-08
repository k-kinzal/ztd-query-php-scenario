<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests data reconciliation patterns through ZTD shadow store (SQLite PDO).
 * @spec SPEC-10.2.39
 */
class SqliteDataReconciliationTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_rec_source (
                id INTEGER PRIMARY KEY, sku TEXT, name TEXT,
                price REAL, qty INTEGER
            )',
            'CREATE TABLE sl_rec_target (
                id INTEGER PRIMARY KEY, sku TEXT, name TEXT,
                price REAL, qty INTEGER
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_rec_target', 'sl_rec_source'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_rec_source VALUES (1, 'SKU-001', 'Widget A', 10.00, 100)");
        $this->pdo->exec("INSERT INTO sl_rec_source VALUES (2, 'SKU-002', 'Widget B', 20.00, 200)");
        $this->pdo->exec("INSERT INTO sl_rec_source VALUES (3, 'SKU-003', 'Widget C', 30.00, 300)");
        $this->pdo->exec("INSERT INTO sl_rec_source VALUES (4, 'SKU-004', 'Widget D', 40.00, 400)");

        $this->pdo->exec("INSERT INTO sl_rec_target VALUES (1, 'SKU-001', 'Widget A', 10.00, 100)");
        $this->pdo->exec("INSERT INTO sl_rec_target VALUES (2, 'SKU-002', 'Widget B', 25.00, 200)");
        $this->pdo->exec("INSERT INTO sl_rec_target VALUES (3, 'SKU-003', 'Widget C', 30.00, 250)");
        $this->pdo->exec("INSERT INTO sl_rec_target VALUES (5, 'SKU-005', 'Widget E', 50.00, 500)");
    }

    public function testFindMissingInTarget(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.id, s.sku FROM sl_rec_source s
             LEFT JOIN sl_rec_target t ON s.id = t.id
             WHERE t.id IS NULL ORDER BY s.id"
        );
        $this->assertCount(1, $rows);
        $this->assertEquals(4, (int) $rows[0]['id']);
    }

    public function testFindExtraInTarget(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.id, t.sku FROM sl_rec_target t
             LEFT JOIN sl_rec_source s ON t.id = s.id
             WHERE s.id IS NULL ORDER BY t.id"
        );
        $this->assertCount(1, $rows);
        $this->assertEquals(5, (int) $rows[0]['id']);
    }

    public function testFindMismatchedRows(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.id, s.price AS sp, t.price AS tp, s.qty AS sq, t.qty AS tq
             FROM sl_rec_source s JOIN sl_rec_target t ON s.id = t.id
             WHERE s.price != t.price OR s.qty != t.qty ORDER BY s.id"
        );
        $this->assertCount(2, $rows);
    }

    public function testExactMatches(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.id FROM sl_rec_source s
             JOIN sl_rec_target t ON s.id = t.id
             WHERE s.price = t.price AND s.qty = t.qty AND s.name = t.name"
        );
        $this->assertCount(1, $rows);
    }

    public function testReconciliationSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                 (SELECT COUNT(*) FROM sl_rec_source s JOIN sl_rec_target t ON s.id = t.id
                  WHERE s.price = t.price AND s.qty = t.qty AND s.name = t.name) AS matched,
                 (SELECT COUNT(*) FROM sl_rec_source s JOIN sl_rec_target t ON s.id = t.id
                  WHERE s.price != t.price OR s.qty != t.qty OR s.name != t.name) AS mismatched,
                 (SELECT COUNT(*) FROM sl_rec_source s LEFT JOIN sl_rec_target t ON s.id = t.id
                  WHERE t.id IS NULL) AS missing_in_target,
                 (SELECT COUNT(*) FROM sl_rec_target t LEFT JOIN sl_rec_source s ON t.id = s.id
                  WHERE s.id IS NULL) AS extra_in_target"
        );
        $this->assertEquals(1, (int) $rows[0]['matched']);
        $this->assertEquals(2, (int) $rows[0]['mismatched']);
        $this->assertEquals(1, (int) $rows[0]['missing_in_target']);
        $this->assertEquals(1, (int) $rows[0]['extra_in_target']);
    }

    public function testColumnLevelDifferences(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.id,
                    CASE WHEN s.price != t.price THEN 'price' ELSE '' END AS price_diff,
                    CASE WHEN s.qty != t.qty THEN 'qty' ELSE '' END AS qty_diff
             FROM sl_rec_source s JOIN sl_rec_target t ON s.id = t.id
             WHERE s.price != t.price OR s.qty != t.qty ORDER BY s.id"
        );
        $this->assertCount(2, $rows);
    }

    public function testReconciliationAfterFix(): void
    {
        $this->pdo->exec("INSERT INTO sl_rec_target VALUES (4, 'SKU-004', 'Widget D', 40.00, 400)");
        $this->pdo->exec("UPDATE sl_rec_target SET price = 20.00 WHERE id = 2");

        $rows = $this->ztdQuery(
            "SELECT s.id FROM sl_rec_source s LEFT JOIN sl_rec_target t ON s.id = t.id WHERE t.id IS NULL"
        );
        $this->assertCount(0, $rows);
    }

    public function testPreparedReconciliation(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT s.price AS sp, t.price AS tp FROM sl_rec_source s
             JOIN sl_rec_target t ON s.sku = t.sku WHERE s.sku = ?',
            ['SKU-002']
        );
        $this->assertCount(1, $rows);
        $this->assertEquals(20.00, (float) $rows[0]['sp']);
        $this->assertEquals(25.00, (float) $rows[0]['tp']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_rec_source');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
