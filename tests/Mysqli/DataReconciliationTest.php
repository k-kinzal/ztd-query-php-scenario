<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests data reconciliation patterns through ZTD shadow store.
 * Simulates comparing two tables to find missing, extra, and mismatched rows.
 * @spec SPEC-10.2.39
 */
class DataReconciliationTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_rec_source (
                id INT PRIMARY KEY,
                sku VARCHAR(50),
                name VARCHAR(255),
                price DECIMAL(10,2),
                qty INT
            )',
            'CREATE TABLE mi_rec_target (
                id INT PRIMARY KEY,
                sku VARCHAR(50),
                name VARCHAR(255),
                price DECIMAL(10,2),
                qty INT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_rec_target', 'mi_rec_source'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Source table (e.g., staging data)
        $this->mysqli->query("INSERT INTO mi_rec_source VALUES (1, 'SKU-001', 'Widget A', 10.00, 100)");
        $this->mysqli->query("INSERT INTO mi_rec_source VALUES (2, 'SKU-002', 'Widget B', 20.00, 200)");
        $this->mysqli->query("INSERT INTO mi_rec_source VALUES (3, 'SKU-003', 'Widget C', 30.00, 300)");
        $this->mysqli->query("INSERT INTO mi_rec_source VALUES (4, 'SKU-004', 'Widget D', 40.00, 400)");

        // Target table (e.g., production data) - some matches, some differences
        $this->mysqli->query("INSERT INTO mi_rec_target VALUES (1, 'SKU-001', 'Widget A', 10.00, 100)"); // exact match
        $this->mysqli->query("INSERT INTO mi_rec_target VALUES (2, 'SKU-002', 'Widget B', 25.00, 200)"); // price mismatch
        $this->mysqli->query("INSERT INTO mi_rec_target VALUES (3, 'SKU-003', 'Widget C', 30.00, 250)"); // qty mismatch
        $this->mysqli->query("INSERT INTO mi_rec_target VALUES (5, 'SKU-005', 'Widget E', 50.00, 500)"); // extra in target
    }

    /**
     * Find rows in source but not in target (LEFT JOIN anti-join).
     */
    public function testFindMissingInTarget(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.id, s.sku, s.name
             FROM mi_rec_source s
             LEFT JOIN mi_rec_target t ON s.id = t.id
             WHERE t.id IS NULL
             ORDER BY s.id"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(4, (int) $rows[0]['id']);
        $this->assertSame('SKU-004', $rows[0]['sku']);
    }

    /**
     * Find rows in target but not in source (extra in target).
     */
    public function testFindExtraInTarget(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.id, t.sku, t.name
             FROM mi_rec_target t
             LEFT JOIN mi_rec_source s ON t.id = s.id
             WHERE s.id IS NULL
             ORDER BY t.id"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(5, (int) $rows[0]['id']);
        $this->assertSame('SKU-005', $rows[0]['sku']);
    }

    /**
     * Find rows with matching IDs but different values.
     */
    public function testFindMismatchedRows(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.id, s.sku,
                    s.price AS source_price, t.price AS target_price,
                    s.qty AS source_qty, t.qty AS target_qty
             FROM mi_rec_source s
             JOIN mi_rec_target t ON s.id = t.id
             WHERE s.price != t.price OR s.qty != t.qty
             ORDER BY s.id"
        );

        $this->assertCount(2, $rows);
        $this->assertEquals(2, (int) $rows[0]['id']); // price mismatch
        $this->assertEquals(20.00, (float) $rows[0]['source_price']);
        $this->assertEquals(25.00, (float) $rows[0]['target_price']);
        $this->assertEquals(3, (int) $rows[1]['id']); // qty mismatch
        $this->assertEquals(300, (int) $rows[1]['source_qty']);
        $this->assertEquals(250, (int) $rows[1]['target_qty']);
    }

    /**
     * Find exact matches between source and target.
     */
    public function testFindExactMatches(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.id, s.sku
             FROM mi_rec_source s
             JOIN mi_rec_target t ON s.id = t.id
             WHERE s.price = t.price AND s.qty = t.qty AND s.name = t.name
             ORDER BY s.id"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
    }

    /**
     * Reconciliation summary: counts of matched, mismatched, missing, extra.
     */
    public function testReconciliationSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                 (SELECT COUNT(*) FROM mi_rec_source s JOIN mi_rec_target t ON s.id = t.id
                  WHERE s.price = t.price AND s.qty = t.qty AND s.name = t.name) AS matched,
                 (SELECT COUNT(*) FROM mi_rec_source s JOIN mi_rec_target t ON s.id = t.id
                  WHERE s.price != t.price OR s.qty != t.qty OR s.name != t.name) AS mismatched,
                 (SELECT COUNT(*) FROM mi_rec_source s LEFT JOIN mi_rec_target t ON s.id = t.id
                  WHERE t.id IS NULL) AS missing_in_target,
                 (SELECT COUNT(*) FROM mi_rec_target t LEFT JOIN mi_rec_source s ON t.id = s.id
                  WHERE s.id IS NULL) AS extra_in_target"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['matched']);
        $this->assertEquals(2, (int) $rows[0]['mismatched']);
        $this->assertEquals(1, (int) $rows[0]['missing_in_target']);
        $this->assertEquals(1, (int) $rows[0]['extra_in_target']);
    }

    /**
     * Find value differences with column-level detail.
     */
    public function testColumnLevelDifferences(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.id, s.sku,
                    CASE WHEN s.price != t.price THEN 'price' ELSE '' END AS price_diff,
                    CASE WHEN s.qty != t.qty THEN 'qty' ELSE '' END AS qty_diff
             FROM mi_rec_source s
             JOIN mi_rec_target t ON s.id = t.id
             WHERE s.price != t.price OR s.qty != t.qty
             ORDER BY s.id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('price', $rows[0]['price_diff']);
        $this->assertSame('', $rows[0]['qty_diff']);
        $this->assertSame('', $rows[1]['price_diff']);
        $this->assertSame('qty', $rows[1]['qty_diff']);
    }

    /**
     * Reconciliation after INSERT into target (fix missing row).
     */
    public function testReconciliationAfterInsert(): void
    {
        // Add the missing row to target
        $this->mysqli->query("INSERT INTO mi_rec_target VALUES (4, 'SKU-004', 'Widget D', 40.00, 400)");

        $rows = $this->ztdQuery(
            "SELECT s.id FROM mi_rec_source s
             LEFT JOIN mi_rec_target t ON s.id = t.id
             WHERE t.id IS NULL"
        );

        $this->assertCount(0, $rows); // No more missing rows
    }

    /**
     * Reconciliation after UPDATE to fix mismatch.
     */
    public function testReconciliationAfterUpdate(): void
    {
        // Fix the price mismatch for SKU-002
        $this->mysqli->query("UPDATE mi_rec_target SET price = 20.00 WHERE id = 2");

        $rows = $this->ztdQuery(
            "SELECT s.id FROM mi_rec_source s
             JOIN mi_rec_target t ON s.id = t.id
             WHERE s.price != t.price
             ORDER BY s.id"
        );

        $this->assertCount(0, $rows); // Price mismatch fixed
    }

    /**
     * Prepared reconciliation by SKU.
     */
    public function testPreparedReconciliation(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT s.price AS source_price, t.price AS target_price
             FROM mi_rec_source s
             JOIN mi_rec_target t ON s.sku = t.sku
             WHERE s.sku = ?',
            ['SKU-002']
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(20.00, (float) $rows[0]['source_price']);
        $this->assertEquals(25.00, (float) $rows[0]['target_price']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_rec_source');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
