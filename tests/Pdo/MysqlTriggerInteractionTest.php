<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests database trigger interaction with ZTD on MySQL PDO.
 *
 * Cross-platform parity with TriggerInteractionTest (MySQLi).
 * @spec SPEC-8.3
 */
class MysqlTriggerInteractionTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_trig_orders (id INT PRIMARY KEY, product VARCHAR(50), quantity INT, total DECIMAL(10,2))',
            'CREATE TABLE mp_trig_audit_log (id INT AUTO_INCREMENT PRIMARY KEY, table_name VARCHAR(50), action VARCHAR(10), record_id INT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_trig_audit_log', 'mp_trig_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $raw = new PDO(
            \Tests\Support\MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TRIGGER IF EXISTS mp_trig_after_insert');
        $raw->exec('DROP TRIGGER IF EXISTS mp_trig_after_update');
        $raw->exec('DROP TRIGGER IF EXISTS mp_trig_after_delete');

        $raw->exec("
            CREATE TRIGGER mp_trig_after_insert AFTER INSERT ON mp_trig_orders
            FOR EACH ROW
            INSERT INTO mp_trig_audit_log (table_name, action, record_id) VALUES ('orders', 'INSERT', NEW.id)
        ");
        $raw->exec("
            CREATE TRIGGER mp_trig_after_update AFTER UPDATE ON mp_trig_orders
            FOR EACH ROW
            INSERT INTO mp_trig_audit_log (table_name, action, record_id) VALUES ('orders', 'UPDATE', NEW.id)
        ");
        $raw->exec("
            CREATE TRIGGER mp_trig_after_delete AFTER DELETE ON mp_trig_orders
            FOR EACH ROW
            INSERT INTO mp_trig_audit_log (table_name, action, record_id) VALUES ('orders', 'DELETE', OLD.id)
        ");
    }

    /**
     * INSERT through ZTD does NOT fire AFTER INSERT trigger.
     */
    public function testInsertDoesNotFireTrigger(): void
    {
        $this->pdo->exec("INSERT INTO mp_trig_orders VALUES (1, 'Widget', 5, 49.95)");

        $stmt = $this->pdo->query('SELECT product FROM mp_trig_orders WHERE id = 1');
        $this->assertSame('Widget', $stmt->fetchColumn());

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mp_trig_audit_log');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * UPDATE through ZTD does NOT fire AFTER UPDATE trigger.
     */
    public function testUpdateDoesNotFireTrigger(): void
    {
        $this->pdo->exec("INSERT INTO mp_trig_orders VALUES (1, 'Widget', 5, 49.95)");
        $this->pdo->exec("UPDATE mp_trig_orders SET quantity = 10 WHERE id = 1");

        $stmt = $this->pdo->query('SELECT quantity FROM mp_trig_orders WHERE id = 1');
        $this->assertSame(10, (int) $stmt->fetchColumn());

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mp_trig_audit_log');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE through ZTD does NOT fire AFTER DELETE trigger.
     */
    public function testDeleteDoesNotFireTrigger(): void
    {
        $this->pdo->exec("INSERT INTO mp_trig_orders VALUES (1, 'Widget', 5, 49.95)");
        $this->pdo->exec('DELETE FROM mp_trig_orders WHERE id = 1');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mp_trig_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mp_trig_audit_log');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * Shadow CRUD works correctly with triggers present.
     */
    public function testShadowCrudWithTriggersPresent(): void
    {
        $this->pdo->exec("INSERT INTO mp_trig_orders VALUES (1, 'Widget', 5, 49.95)");
        $this->pdo->exec("INSERT INTO mp_trig_orders VALUES (2, 'Gadget', 3, 89.97)");
        $this->pdo->exec("UPDATE mp_trig_orders SET total = 59.94 WHERE id = 1");
        $this->pdo->exec('DELETE FROM mp_trig_orders WHERE id = 2');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mp_trig_orders');
        $this->assertSame(1, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT product, total FROM mp_trig_orders WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Widget', $row['product']);
        $this->assertEquals(59.94, (float) $row['total']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_trig_orders VALUES (1, 'Widget', 5, 49.95)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mp_trig_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                \Tests\Support\MySQLContainer::getDsn(),
                'root',
                'root',
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            );
            $raw->exec('DROP TRIGGER IF EXISTS mp_trig_after_insert');
            $raw->exec('DROP TRIGGER IF EXISTS mp_trig_after_update');
            $raw->exec('DROP TRIGGER IF EXISTS mp_trig_after_delete');
            $raw->exec('DROP TABLE IF EXISTS mp_trig_audit_log');
            $raw->exec('DROP TABLE IF EXISTS mp_trig_orders');
        } catch (\Exception $e) {
        }
    }
}
