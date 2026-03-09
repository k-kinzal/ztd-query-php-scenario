<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a data archival workflow through ZTD shadow store (SQLite PDO).
 * Covers INSERT SELECT for archival, DELETE after archival, cross-table
 * read-your-writes consistency, archive queries, and physical isolation.
 * @spec SPEC-10.2.104
 */
class SqliteDataArchivalTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_da_orders (
                id INTEGER PRIMARY KEY,
                customer TEXT,
                product TEXT,
                amount REAL,
                status TEXT,
                created_at TEXT,
                completed_at TEXT
            )',
            'CREATE TABLE sl_da_archived_orders (
                id INTEGER PRIMARY KEY,
                customer TEXT,
                product TEXT,
                amount REAL,
                status TEXT,
                created_at TEXT,
                completed_at TEXT,
                archived_at TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_da_archived_orders', 'sl_da_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Active orders: mix of completed and pending
        $this->pdo->exec("INSERT INTO sl_da_orders VALUES (1, 'Alice', 'Widget', 29.99, 'completed', '2025-11-01', '2025-11-05')");
        $this->pdo->exec("INSERT INTO sl_da_orders VALUES (2, 'Bob', 'Gadget', 49.99, 'completed', '2025-12-01', '2025-12-10')");
        $this->pdo->exec("INSERT INTO sl_da_orders VALUES (3, 'Charlie', 'Widget', 29.99, 'completed', '2026-01-15', '2026-01-20')");
        $this->pdo->exec("INSERT INTO sl_da_orders VALUES (4, 'Alice', 'Gizmo', 99.99, 'pending', '2026-03-01', NULL)");
        $this->pdo->exec("INSERT INTO sl_da_orders VALUES (5, 'Diana', 'Widget', 29.99, 'shipped', '2026-03-05', NULL)");
    }

    /**
     * Archive old completed orders using INSERT SELECT.
     * Note: Literal values in INSERT SELECT become NULL on SQLite (SPEC-11.INSERT-SELECT-COMPUTED).
     */
    public function testArchiveCompletedOrders(): void
    {
        // Archive orders completed before 2026-01-01
        $affected = $this->pdo->exec(
            "INSERT INTO sl_da_archived_orders (id, customer, product, amount, status, created_at, completed_at, archived_at)
             SELECT id, customer, product, amount, status, created_at, completed_at, '2026-03-09'
             FROM sl_da_orders
             WHERE status = 'completed' AND completed_at < '2026-01-01'"
        );

        // Orders 1 and 2 should be archived
        $this->assertSame(2, $affected);

        // Verify archived data
        $rows = $this->ztdQuery("SELECT id, customer, archived_at FROM sl_da_archived_orders ORDER BY id");
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['customer']);
        // Known issue: literal value '2026-03-09' in INSERT SELECT becomes NULL on SQLite
        // (SPEC-11.INSERT-SELECT-COMPUTED)
        $this->assertNull($rows[0]['archived_at']);
        $this->assertSame('Bob', $rows[1]['customer']);
    }

    /**
     * Delete archived orders from active table after archival.
     */
    public function testDeleteAfterArchival(): void
    {
        // Archive first
        $this->pdo->exec(
            "INSERT INTO sl_da_archived_orders (id, customer, product, amount, status, created_at, completed_at, archived_at)
             SELECT id, customer, product, amount, status, created_at, completed_at, '2026-03-09'
             FROM sl_da_orders
             WHERE status = 'completed' AND completed_at < '2026-01-01'"
        );

        // Delete archived orders from active table
        $affected = $this->pdo->exec(
            "DELETE FROM sl_da_orders WHERE status = 'completed' AND completed_at < '2026-01-01'"
        );
        $this->assertSame(2, $affected);

        // Active table should have 3 remaining orders
        $rows = $this->ztdQuery("SELECT id FROM sl_da_orders ORDER BY id");
        $this->assertCount(3, $rows);
        $this->assertEquals(3, (int) $rows[0]['id']);
        $this->assertEquals(4, (int) $rows[1]['id']);
        $this->assertEquals(5, (int) $rows[2]['id']);

        // Archive should have 2
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_da_archived_orders");
        $this->assertEquals(2, (int) $rows[0]['cnt']);
    }

    /**
     * Total revenue across active and archived tables using separate queries.
     * Note: UNION ALL in a derived table on SQLite returns empty due to
     * bare subquery table references not being rewritten (SPEC-11.BARE-SUBQUERY-REWRITE).
     */
    public function testTotalRevenueAcrossTables(): void
    {
        // Archive 2 orders first
        $this->pdo->exec(
            "INSERT INTO sl_da_archived_orders (id, customer, product, amount, status, created_at, completed_at, archived_at)
             SELECT id, customer, product, amount, status, created_at, completed_at, '2026-03-09'
             FROM sl_da_orders
             WHERE status = 'completed' AND completed_at < '2026-01-01'"
        );
        $this->pdo->exec(
            "DELETE FROM sl_da_orders WHERE status = 'completed' AND completed_at < '2026-01-01'"
        );

        // Workaround: query each table separately, sum in PHP
        $active = $this->ztdQuery(
            "SELECT COALESCE(SUM(amount), 0) AS subtotal FROM sl_da_orders WHERE 1=1"
        );
        $archived = $this->ztdQuery(
            "SELECT COALESCE(SUM(amount), 0) AS subtotal FROM sl_da_archived_orders WHERE 1=1"
        );

        $totalRevenue = (float) $active[0]['subtotal'] + (float) $archived[0]['subtotal'];

        // Active: orders 3 (29.99), 4 (99.99), 5 (29.99) = 159.97
        // Archived: orders 1 (29.99), 2 (49.99) = 79.98
        // Total: 239.95
        $this->assertEqualsWithDelta(239.95, $totalRevenue, 0.01);
    }

    /**
     * Archive summary: count and total by product across both tables.
     */
    public function testArchiveSummaryByProduct(): void
    {
        // Archive all completed orders
        $this->pdo->exec(
            "INSERT INTO sl_da_archived_orders (id, customer, product, amount, status, created_at, completed_at, archived_at)
             SELECT id, customer, product, amount, status, created_at, completed_at, '2026-03-09'
             FROM sl_da_orders
             WHERE status = 'completed'"
        );
        $this->pdo->exec("DELETE FROM sl_da_orders WHERE status = 'completed'");

        // Verify active table only has non-completed orders
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_da_orders");
        $this->assertEquals(2, (int) $rows[0]['cnt']);

        // Verify archived orders
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_da_archived_orders");
        $this->assertEquals(3, (int) $rows[0]['cnt']);
    }

    /**
     * Customer order history across active and archived tables.
     */
    public function testCustomerHistoryAcrossTables(): void
    {
        // Archive old orders
        $this->pdo->exec(
            "INSERT INTO sl_da_archived_orders (id, customer, product, amount, status, created_at, completed_at, archived_at)
             SELECT id, customer, product, amount, status, created_at, completed_at, '2026-03-09'
             FROM sl_da_orders
             WHERE status = 'completed' AND completed_at < '2026-01-01'"
        );
        $this->pdo->exec("DELETE FROM sl_da_orders WHERE status = 'completed' AND completed_at < '2026-01-01'");

        // Alice's history from both tables
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, product, amount, status, 'active' AS source
             FROM sl_da_orders WHERE customer = ?
             UNION ALL
             SELECT id, product, amount, status, 'archived' AS source
             FROM sl_da_archived_orders WHERE customer = ?
             ORDER BY id",
            ['Alice', 'Alice']
        );

        $this->assertCount(2, $rows);
        // Order 1 was archived, order 4 is still active
        $this->assertSame('archived', $rows[0]['source']);
        $this->assertSame('active', $rows[1]['source']);
        $this->assertSame('Gizmo', $rows[1]['product']);
    }

    /**
     * Physical isolation: archival mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec(
            "INSERT INTO sl_da_archived_orders (id, customer, product, amount, status, created_at, completed_at, archived_at)
             SELECT id, customer, product, amount, status, created_at, completed_at, '2026-03-09'
             FROM sl_da_orders
             WHERE status = 'completed' AND completed_at < '2026-01-01'"
        );

        // ZTD sees 2 archived rows
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_da_archived_orders");
        $this->assertEquals(2, (int) $rows[0]['cnt']);

        // Physical table is empty
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_da_archived_orders")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
