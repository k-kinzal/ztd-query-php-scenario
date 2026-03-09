<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests an audit trail scenario through ZTD shadow store (SQLite PDO).
 * Covers audit logging with change tracking, point-in-time state
 * reconstruction, and revert capabilities.
 * @spec SPEC-10.2.67
 */
class SqliteAuditTrailTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_at_products (
                id INTEGER PRIMARY KEY,
                name TEXT,
                price REAL,
                status TEXT
            )',
            'CREATE TABLE sl_at_audit_log (
                id INTEGER PRIMARY KEY,
                table_name TEXT,
                record_id INTEGER,
                action TEXT,
                old_value TEXT,
                new_value TEXT,
                changed_by TEXT,
                changed_at TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_at_audit_log', 'sl_at_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 products
        $this->pdo->exec("INSERT INTO sl_at_products VALUES (1, 'Widget', 10.00, 'active')");
        $this->pdo->exec("INSERT INTO sl_at_products VALUES (2, 'Gadget', 25.00, 'active')");
        $this->pdo->exec("INSERT INTO sl_at_products VALUES (3, 'Doohickey', 5.00, 'discontinued')");

        // 5 audit_log entries
        $this->pdo->exec("INSERT INTO sl_at_audit_log VALUES (1, 'products', 1, 'INSERT', NULL, 'Widget|10.00|active', 'admin', '2026-03-01 10:00:00')");
        $this->pdo->exec("INSERT INTO sl_at_audit_log VALUES (2, 'products', 1, 'UPDATE', '10.00', '12.00', 'manager', '2026-03-02 14:00:00')");
        $this->pdo->exec("INSERT INTO sl_at_audit_log VALUES (3, 'products', 2, 'INSERT', NULL, 'Gadget|25.00|active', 'admin', '2026-03-01 10:05:00')");
        $this->pdo->exec("INSERT INTO sl_at_audit_log VALUES (4, 'products', 3, 'INSERT', NULL, 'Doohickey|5.00|active', 'admin', '2026-03-01 10:10:00')");
        $this->pdo->exec("INSERT INTO sl_at_audit_log VALUES (5, 'products', 3, 'UPDATE', 'active', 'discontinued', 'admin', '2026-03-03 09:00:00')");
    }

    /**
     * INSERT a product and its audit_log record, verify both are visible.
     */
    public function testLogInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_at_products VALUES (4, 'Thingamajig', 15.00, 'active')");
        $this->pdo->exec("INSERT INTO sl_at_audit_log VALUES (6, 'products', 4, 'INSERT', NULL, 'Thingamajig|15.00|active', 'admin', '2026-03-09 12:00:00')");

        $product = $this->ztdQuery("SELECT id, name, price, status FROM sl_at_products WHERE id = 4");
        $this->assertCount(1, $product);
        $this->assertSame('Thingamajig', $product[0]['name']);
        $this->assertSame('active', $product[0]['status']);

        $log = $this->ztdQuery("SELECT action, new_value, changed_by FROM sl_at_audit_log WHERE id = 6");
        $this->assertCount(1, $log);
        $this->assertSame('INSERT', $log[0]['action']);
        $this->assertSame('Thingamajig|15.00|active', $log[0]['new_value']);
        $this->assertSame('admin', $log[0]['changed_by']);
    }

    /**
     * UPDATE a product price and INSERT an audit_log record with old/new values.
     */
    public function testLogUpdate(): void
    {
        $this->pdo->exec("UPDATE sl_at_products SET price = 12.00 WHERE id = 1");
        $this->pdo->exec("INSERT INTO sl_at_audit_log VALUES (6, 'products', 1, 'UPDATE', '10.00', '12.00', 'manager', '2026-03-09 13:00:00')");

        $product = $this->ztdQuery("SELECT price FROM sl_at_products WHERE id = 1");
        $this->assertEquals(12.00, (float) $product[0]['price']);

        $log = $this->ztdQuery("SELECT old_value, new_value, changed_by FROM sl_at_audit_log WHERE id = 6");
        $this->assertSame('10.00', $log[0]['old_value']);
        $this->assertSame('12.00', $log[0]['new_value']);
        $this->assertSame('manager', $log[0]['changed_by']);
    }

    /**
     * SELECT audit_log for a specific record ordered by changed_at to see full history.
     */
    public function testChangeHistoryForRecord(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, action, old_value, new_value, changed_by, changed_at
             FROM sl_at_audit_log
             WHERE record_id = ?
             ORDER BY changed_at",
            [1]
        );

        $this->assertCount(2, $rows);
        $this->assertSame('INSERT', $rows[0]['action']);
        $this->assertNull($rows[0]['old_value']);
        $this->assertSame('admin', $rows[0]['changed_by']);
        $this->assertSame('UPDATE', $rows[1]['action']);
        $this->assertSame('10.00', $rows[1]['old_value']);
        $this->assertSame('12.00', $rows[1]['new_value']);
        $this->assertSame('manager', $rows[1]['changed_by']);
    }

    /**
     * Use correlated subquery with MAX(id) to get latest audit entry per record.
     */
    public function testLatestStateFromAuditLog(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.record_id, a.action, a.new_value, a.changed_by
             FROM sl_at_audit_log a
             WHERE a.id = (
                 SELECT MAX(a2.id) FROM sl_at_audit_log a2
                 WHERE a2.record_id = a.record_id
             )
             ORDER BY a.record_id"
        );

        $this->assertCount(3, $rows);

        // Product 1: latest is UPDATE (id=2)
        $this->assertEquals(1, (int) $rows[0]['record_id']);
        $this->assertSame('UPDATE', $rows[0]['action']);
        $this->assertSame('12.00', $rows[0]['new_value']);

        // Product 2: latest is INSERT (id=3)
        $this->assertEquals(2, (int) $rows[1]['record_id']);
        $this->assertSame('INSERT', $rows[1]['action']);

        // Product 3: latest is UPDATE (id=5)
        $this->assertEquals(3, (int) $rows[2]['record_id']);
        $this->assertSame('UPDATE', $rows[2]['action']);
        $this->assertSame('discontinued', $rows[2]['new_value']);
    }

    /**
     * GROUP BY action with COUNT(*) for INSERT/UPDATE breakdown.
     */
    public function testCountChangesByAction(): void
    {
        $rows = $this->ztdQuery(
            "SELECT action, COUNT(*) AS cnt
             FROM sl_at_audit_log
             GROUP BY action
             ORDER BY action"
        );

        $this->assertCount(2, $rows);

        $this->assertSame('INSERT', $rows[0]['action']);
        $this->assertEquals(3, (int) $rows[0]['cnt']);

        $this->assertSame('UPDATE', $rows[1]['action']);
        $this->assertEquals(2, (int) $rows[1]['cnt']);
    }

    /**
     * Read old_value from audit_log and UPDATE product with it to revert a change.
     */
    public function testRevertToOldValue(): void
    {
        // Apply the update to match the audit trail
        $this->pdo->exec("UPDATE sl_at_products SET price = 12.00 WHERE id = 1");

        $product = $this->ztdQuery("SELECT price FROM sl_at_products WHERE id = 1");
        $this->assertEquals(12.00, (float) $product[0]['price']);

        // Read the old_value from the audit log
        $log = $this->ztdPrepareAndExecute(
            "SELECT old_value FROM sl_at_audit_log WHERE record_id = ? AND action = 'UPDATE' ORDER BY changed_at DESC",
            [1]
        );
        $this->assertCount(1, $log);
        $oldPrice = $log[0]['old_value'];
        $this->assertSame('10.00', $oldPrice);

        // Revert the product price to the old value
        $this->pdo->exec("UPDATE sl_at_products SET price = {$oldPrice} WHERE id = 1");

        // Log the revert in audit
        $this->pdo->exec("INSERT INTO sl_at_audit_log VALUES (6, 'products', 1, 'UPDATE', '12.00', '10.00', 'admin', '2026-03-09 14:00:00')");

        $product = $this->ztdQuery("SELECT price FROM sl_at_products WHERE id = 1");
        $this->assertEquals(10.00, (float) $product[0]['price']);

        // Verify audit log now shows 3 entries for product 1
        $history = $this->ztdPrepareAndExecute(
            "SELECT id FROM sl_at_audit_log WHERE record_id = ? ORDER BY changed_at",
            [1]
        );
        $this->assertCount(3, $history);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_at_products VALUES (4, 'NewProduct', 99.99, 'active')");
        $this->pdo->exec("INSERT INTO sl_at_audit_log VALUES (6, 'products', 4, 'INSERT', NULL, 'NewProduct|99.99|active', 'admin', '2026-03-09 15:00:00')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_at_products");
        $this->assertSame(4, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_at_audit_log");
        $this->assertSame(6, (int) $rows[0]['cnt']);

        // Physical tables untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_at_products")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);

        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_at_audit_log")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
