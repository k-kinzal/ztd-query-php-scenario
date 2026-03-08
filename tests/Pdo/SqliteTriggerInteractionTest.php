<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests database trigger interaction with ZTD on SQLite PDO.
 *
 * Cross-platform parity with MySQL/PostgreSQL trigger tests.
 * @spec SPEC-8.3
 */
class SqliteTriggerInteractionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_trig_orders (id INT PRIMARY KEY, product VARCHAR(50), quantity INT, total DECIMAL(10,2))',
            'CREATE TABLE sl_trig_audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, table_name VARCHAR(50), action VARCHAR(10), record_id INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_trig_audit_log', 'sl_trig_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Create triggers on the physical tables
        $this->pdo->disableZtd();
        $this->pdo->exec("
            CREATE TRIGGER sl_trig_after_insert AFTER INSERT ON sl_trig_orders
            BEGIN
                INSERT INTO sl_trig_audit_log (table_name, action, record_id) VALUES ('orders', 'INSERT', NEW.id);
            END
        ");
        $this->pdo->exec("
            CREATE TRIGGER sl_trig_after_update AFTER UPDATE ON sl_trig_orders
            BEGIN
                INSERT INTO sl_trig_audit_log (table_name, action, record_id) VALUES ('orders', 'UPDATE', NEW.id);
            END
        ");
        $this->pdo->exec("
            CREATE TRIGGER sl_trig_after_delete AFTER DELETE ON sl_trig_orders
            BEGIN
                INSERT INTO sl_trig_audit_log (table_name, action, record_id) VALUES ('orders', 'DELETE', OLD.id);
            END
        ");
        $this->pdo->enableZtd();
    }

    /**
     * INSERT through ZTD does NOT fire AFTER INSERT trigger.
     */
    public function testInsertDoesNotFireTrigger(): void
    {
        $this->pdo->exec("INSERT INTO sl_trig_orders VALUES (1, 'Widget', 5, 49.95)");

        $stmt = $this->pdo->query('SELECT product FROM sl_trig_orders WHERE id = 1');
        $this->assertSame('Widget', $stmt->fetchColumn());

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_trig_audit_log');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * UPDATE through ZTD does NOT fire AFTER UPDATE trigger.
     */
    public function testUpdateDoesNotFireTrigger(): void
    {
        $this->pdo->exec("INSERT INTO sl_trig_orders VALUES (1, 'Widget', 5, 49.95)");
        $this->pdo->exec("UPDATE sl_trig_orders SET quantity = 10 WHERE id = 1");

        $stmt = $this->pdo->query('SELECT quantity FROM sl_trig_orders WHERE id = 1');
        $this->assertSame(10, (int) $stmt->fetchColumn());

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_trig_audit_log');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE through ZTD does NOT fire AFTER DELETE trigger.
     */
    public function testDeleteDoesNotFireTrigger(): void
    {
        $this->pdo->exec("INSERT INTO sl_trig_orders VALUES (1, 'Widget', 5, 49.95)");
        $this->pdo->exec("DELETE FROM sl_trig_orders WHERE 1=1");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_trig_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_trig_audit_log');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * Shadow CRUD works correctly with triggers present.
     */
    public function testShadowCrudWithTriggersPresent(): void
    {
        $this->pdo->exec("INSERT INTO sl_trig_orders VALUES (1, 'Widget', 5, 49.95)");
        $this->pdo->exec("INSERT INTO sl_trig_orders VALUES (2, 'Gadget', 3, 89.97)");
        $this->pdo->exec("UPDATE sl_trig_orders SET total = 59.94 WHERE id = 1");
        $this->pdo->exec('DELETE FROM sl_trig_orders WHERE id = 2');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_trig_orders');
        $this->assertSame(1, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT product, total FROM sl_trig_orders WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Widget', $row['product']);
        $this->assertEquals(59.94, (float) $row['total']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_trig_orders VALUES (1, 'Widget', 5, 49.95)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_trig_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
