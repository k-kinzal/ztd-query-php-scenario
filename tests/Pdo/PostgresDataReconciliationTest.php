<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests data reconciliation patterns through ZTD shadow store (PostgreSQL PDO).
 * @spec SPEC-10.2.39
 */
class PostgresDataReconciliationTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_rec_source (
                id INT PRIMARY KEY, sku VARCHAR(50), name VARCHAR(255),
                price DECIMAL(10,2), qty INT
            )',
            'CREATE TABLE pg_rec_target (
                id INT PRIMARY KEY, sku VARCHAR(50), name VARCHAR(255),
                price DECIMAL(10,2), qty INT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_rec_target', 'pg_rec_source'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_rec_source VALUES (1, 'SKU-001', 'Widget A', 10.00, 100)");
        $this->pdo->exec("INSERT INTO pg_rec_source VALUES (2, 'SKU-002', 'Widget B', 20.00, 200)");
        $this->pdo->exec("INSERT INTO pg_rec_source VALUES (3, 'SKU-003', 'Widget C', 30.00, 300)");
        $this->pdo->exec("INSERT INTO pg_rec_source VALUES (4, 'SKU-004', 'Widget D', 40.00, 400)");

        $this->pdo->exec("INSERT INTO pg_rec_target VALUES (1, 'SKU-001', 'Widget A', 10.00, 100)");
        $this->pdo->exec("INSERT INTO pg_rec_target VALUES (2, 'SKU-002', 'Widget B', 25.00, 200)");
        $this->pdo->exec("INSERT INTO pg_rec_target VALUES (3, 'SKU-003', 'Widget C', 30.00, 250)");
        $this->pdo->exec("INSERT INTO pg_rec_target VALUES (5, 'SKU-005', 'Widget E', 50.00, 500)");
    }

    public function testFindMissingInTarget(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.id, s.sku FROM pg_rec_source s
             LEFT JOIN pg_rec_target t ON s.id = t.id
             WHERE t.id IS NULL ORDER BY s.id"
        );
        $this->assertCount(1, $rows);
        $this->assertEquals(4, (int) $rows[0]['id']);
    }

    public function testFindExtraInTarget(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.id, t.sku FROM pg_rec_target t
             LEFT JOIN pg_rec_source s ON t.id = s.id
             WHERE s.id IS NULL ORDER BY t.id"
        );
        $this->assertCount(1, $rows);
        $this->assertEquals(5, (int) $rows[0]['id']);
    }

    public function testFindMismatchedRows(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.id, s.price AS sp, t.price AS tp, s.qty AS sq, t.qty AS tq
             FROM pg_rec_source s JOIN pg_rec_target t ON s.id = t.id
             WHERE s.price != t.price OR s.qty != t.qty ORDER BY s.id"
        );
        $this->assertCount(2, $rows);
    }

    /**
     * PostgreSQL FULL OUTER JOIN for complete reconciliation.
     */
    public function testFullOuterJoinReconciliation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COALESCE(s.id, t.id) AS item_id,
                    CASE
                        WHEN s.id IS NULL THEN 'extra_in_target'
                        WHEN t.id IS NULL THEN 'missing_in_target'
                        WHEN s.price = t.price AND s.qty = t.qty AND s.name = t.name THEN 'match'
                        ELSE 'mismatch'
                    END AS status
             FROM pg_rec_source s
             FULL OUTER JOIN pg_rec_target t ON s.id = t.id
             ORDER BY item_id"
        );

        $this->assertCount(5, $rows);
        $statuses = array_column($rows, 'status', 'item_id');
        $this->assertSame('match', $statuses[1]);
        $this->assertSame('mismatch', $statuses[2]);
        $this->assertSame('mismatch', $statuses[3]);
        $this->assertSame('missing_in_target', $statuses[4]);
        $this->assertSame('extra_in_target', $statuses[5]);
    }

    public function testReconciliationSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                 (SELECT COUNT(*) FROM pg_rec_source s JOIN pg_rec_target t ON s.id = t.id
                  WHERE s.price = t.price AND s.qty = t.qty AND s.name = t.name) AS matched,
                 (SELECT COUNT(*) FROM pg_rec_source s JOIN pg_rec_target t ON s.id = t.id
                  WHERE s.price != t.price OR s.qty != t.qty OR s.name != t.name) AS mismatched,
                 (SELECT COUNT(*) FROM pg_rec_source s LEFT JOIN pg_rec_target t ON s.id = t.id
                  WHERE t.id IS NULL) AS missing_in_target,
                 (SELECT COUNT(*) FROM pg_rec_target t LEFT JOIN pg_rec_source s ON t.id = s.id
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
             FROM pg_rec_source s JOIN pg_rec_target t ON s.id = t.id
             WHERE s.price != t.price OR s.qty != t.qty ORDER BY s.id"
        );
        $this->assertCount(2, $rows);
    }

    public function testReconciliationAfterFix(): void
    {
        $this->pdo->exec("INSERT INTO pg_rec_target VALUES (4, 'SKU-004', 'Widget D', 40.00, 400)");
        $this->pdo->exec("UPDATE pg_rec_target SET price = 20.00 WHERE id = 2");

        $rows = $this->ztdQuery(
            "SELECT s.id FROM pg_rec_source s LEFT JOIN pg_rec_target t ON s.id = t.id WHERE t.id IS NULL"
        );
        $this->assertCount(0, $rows);
    }

    public function testPreparedReconciliation(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT s.price AS sp, t.price AS tp FROM pg_rec_source s
             JOIN pg_rec_target t ON s.sku = t.sku WHERE s.sku = ?',
            ['SKU-002']
        );
        $this->assertCount(1, $rows);
        $this->assertEquals(20.00, (float) $rows[0]['sp']);
        $this->assertEquals(25.00, (float) $rows[0]['tp']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_rec_source');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
