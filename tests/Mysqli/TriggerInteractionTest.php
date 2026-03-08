<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests database trigger interaction with ZTD on MySQLi.
 *
 * Triggers are a common real-world pattern (audit trails, auto-timestamps,
 * counter updates). Since ZTD operations go to the shadow store rather than
 * the physical database, triggers on physical tables should NOT fire for
 * shadow operations.
 * @spec SPEC-8.3
 */
class TriggerInteractionTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_trig_orders (id INT PRIMARY KEY, product VARCHAR(50), quantity INT, total DECIMAL(10,2))',
            'CREATE TABLE mi_trig_audit_log (id INT AUTO_INCREMENT PRIMARY KEY, table_name VARCHAR(50), action VARCHAR(10), record_id INT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)',
            'CREATE TABLE mi_trig_counters (name VARCHAR(50) PRIMARY KEY, value INT DEFAULT 0)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_trig_audit_log', 'mi_trig_orders', 'mi_trig_counters'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Create triggers using a raw connection
        $raw = new \mysqli(
            \Tests\Support\MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            \Tests\Support\MySQLContainer::getPort(),
        );
        $raw->query('DROP TRIGGER IF EXISTS mi_trig_after_insert');
        $raw->query('DROP TRIGGER IF EXISTS mi_trig_after_update');
        $raw->query('DROP TRIGGER IF EXISTS mi_trig_after_delete');

        $raw->query("
            CREATE TRIGGER mi_trig_after_insert AFTER INSERT ON mi_trig_orders
            FOR EACH ROW
            INSERT INTO mi_trig_audit_log (table_name, action, record_id) VALUES ('orders', 'INSERT', NEW.id)
        ");
        $raw->query("
            CREATE TRIGGER mi_trig_after_update AFTER UPDATE ON mi_trig_orders
            FOR EACH ROW
            INSERT INTO mi_trig_audit_log (table_name, action, record_id) VALUES ('orders', 'UPDATE', NEW.id)
        ");
        $raw->query("
            CREATE TRIGGER mi_trig_after_delete AFTER DELETE ON mi_trig_orders
            FOR EACH ROW
            INSERT INTO mi_trig_audit_log (table_name, action, record_id) VALUES ('orders', 'DELETE', OLD.id)
        ");

        // Initialize counter
        $raw->query("INSERT INTO mi_trig_counters VALUES ('order_count', 0)");
        $raw->close();
    }

    /**
     * INSERT through ZTD does NOT fire AFTER INSERT trigger.
     */
    public function testInsertDoesNotFireTrigger(): void
    {
        $this->mysqli->query("INSERT INTO mi_trig_orders VALUES (1, 'Widget', 5, 49.95)");

        // Shadow data is visible
        $result = $this->mysqli->query('SELECT product FROM mi_trig_orders WHERE id = 1');
        $this->assertSame('Widget', $result->fetch_assoc()['product']);

        // Audit log should be empty — trigger did not fire
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_trig_audit_log');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * UPDATE through ZTD does NOT fire AFTER UPDATE trigger.
     */
    public function testUpdateDoesNotFireTrigger(): void
    {
        $this->mysqli->query("INSERT INTO mi_trig_orders VALUES (1, 'Widget', 5, 49.95)");
        $this->mysqli->query("UPDATE mi_trig_orders SET quantity = 10, total = 99.90 WHERE id = 1");

        // Shadow data is updated
        $result = $this->mysqli->query('SELECT quantity FROM mi_trig_orders WHERE id = 1');
        $this->assertSame(10, (int) $result->fetch_assoc()['quantity']);

        // No audit log entry
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_trig_audit_log');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * DELETE through ZTD does NOT fire AFTER DELETE trigger.
     */
    public function testDeleteDoesNotFireTrigger(): void
    {
        $this->mysqli->query("INSERT INTO mi_trig_orders VALUES (1, 'Widget', 5, 49.95)");
        $this->mysqli->query('DELETE FROM mi_trig_orders WHERE id = 1');

        // Shadow data is deleted
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_trig_orders');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);

        // No audit log entry
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_trig_audit_log');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Shadow operations work correctly even with triggers present on the table.
     */
    public function testShadowCrudWithTriggersPresent(): void
    {
        $this->mysqli->query("INSERT INTO mi_trig_orders VALUES (1, 'Widget', 5, 49.95)");
        $this->mysqli->query("INSERT INTO mi_trig_orders VALUES (2, 'Gadget', 3, 89.97)");
        $this->mysqli->query("UPDATE mi_trig_orders SET total = 59.94 WHERE id = 1");
        $this->mysqli->query('DELETE FROM mi_trig_orders WHERE id = 2');

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_trig_orders');
        $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT product, total FROM mi_trig_orders WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Widget', $row['product']);
        $this->assertEquals(59.94, (float) $row['total']);
    }

    /**
     * Physical isolation — no data in physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_trig_orders VALUES (1, 'Widget', 5, 49.95)");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_trig_orders');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new \mysqli(
                \Tests\Support\MySQLContainer::getHost(),
                'root',
                'root',
                'test',
                \Tests\Support\MySQLContainer::getPort(),
            );
            $raw->query('DROP TRIGGER IF EXISTS mi_trig_after_insert');
            $raw->query('DROP TRIGGER IF EXISTS mi_trig_after_update');
            $raw->query('DROP TRIGGER IF EXISTS mi_trig_after_delete');
            $raw->query('DROP TABLE IF EXISTS mi_trig_audit_log');
            $raw->query('DROP TABLE IF EXISTS mi_trig_orders');
            $raw->query('DROP TABLE IF EXISTS mi_trig_counters');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
