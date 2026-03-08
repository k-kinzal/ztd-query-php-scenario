<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests database trigger interaction with ZTD on PostgreSQL PDO.
 *
 * PostgreSQL triggers use CREATE FUNCTION + CREATE TRIGGER syntax.
 * Cross-platform parity with MySQL trigger tests.
 * @spec SPEC-8.3
 */
class PostgresTriggerInteractionTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TRIGGER IF EXISTS pg_trig_after_insert ON pg_trig_orders');
        $raw->exec('DROP TRIGGER IF EXISTS pg_trig_after_update ON pg_trig_orders');
        $raw->exec('DROP TRIGGER IF EXISTS pg_trig_after_delete ON pg_trig_orders');
        $raw->exec('DROP FUNCTION IF EXISTS pg_trig_audit_func()');
        $raw->exec('DROP TABLE IF EXISTS pg_trig_audit_log');
        $raw->exec('DROP TABLE IF EXISTS pg_trig_orders');

        $raw->exec('CREATE TABLE pg_trig_orders (id INT PRIMARY KEY, product VARCHAR(50), quantity INT, total DECIMAL(10,2))');
        $raw->exec('CREATE TABLE pg_trig_audit_log (id SERIAL PRIMARY KEY, table_name VARCHAR(50), action VARCHAR(10), record_id INT)');

        $raw->exec("
            CREATE FUNCTION pg_trig_audit_func() RETURNS TRIGGER AS \$\$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    INSERT INTO pg_trig_audit_log (table_name, action, record_id) VALUES ('orders', TG_OP, OLD.id);
                    RETURN OLD;
                ELSE
                    INSERT INTO pg_trig_audit_log (table_name, action, record_id) VALUES ('orders', TG_OP, NEW.id);
                    RETURN NEW;
                END IF;
            END;
            \$\$ LANGUAGE plpgsql
        ");

        $raw->exec('CREATE TRIGGER pg_trig_after_insert AFTER INSERT ON pg_trig_orders FOR EACH ROW EXECUTE FUNCTION pg_trig_audit_func()');
        $raw->exec('CREATE TRIGGER pg_trig_after_update AFTER UPDATE ON pg_trig_orders FOR EACH ROW EXECUTE FUNCTION pg_trig_audit_func()');
        $raw->exec('CREATE TRIGGER pg_trig_after_delete AFTER DELETE ON pg_trig_orders FOR EACH ROW EXECUTE FUNCTION pg_trig_audit_func()');

        $this->pdo = ZtdPdo::fromPdo(new PDO(
            'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        ));
    }

    /**
     * INSERT through ZTD does NOT fire AFTER INSERT trigger.
     */
    public function testInsertDoesNotFireTrigger(): void
    {
        $this->pdo->exec("INSERT INTO pg_trig_orders VALUES (1, 'Widget', 5, 49.95)");

        $stmt = $this->pdo->query('SELECT product FROM pg_trig_orders WHERE id = 1');
        $this->assertSame('Widget', $stmt->fetchColumn());

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_trig_audit_log');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * UPDATE through ZTD does NOT fire AFTER UPDATE trigger.
     */
    public function testUpdateDoesNotFireTrigger(): void
    {
        $this->pdo->exec("INSERT INTO pg_trig_orders VALUES (1, 'Widget', 5, 49.95)");
        $this->pdo->exec("UPDATE pg_trig_orders SET quantity = 10 WHERE id = 1");

        $stmt = $this->pdo->query('SELECT quantity FROM pg_trig_orders WHERE id = 1');
        $this->assertSame(10, (int) $stmt->fetchColumn());

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_trig_audit_log');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE through ZTD does NOT fire AFTER DELETE trigger.
     */
    public function testDeleteDoesNotFireTrigger(): void
    {
        $this->pdo->exec("INSERT INTO pg_trig_orders VALUES (1, 'Widget', 5, 49.95)");
        $this->pdo->exec('DELETE FROM pg_trig_orders WHERE id = 1');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_trig_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_trig_audit_log');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * Shadow CRUD works correctly with triggers present.
     */
    public function testShadowCrudWithTriggersPresent(): void
    {
        $this->pdo->exec("INSERT INTO pg_trig_orders VALUES (1, 'Widget', 5, 49.95)");
        $this->pdo->exec("INSERT INTO pg_trig_orders VALUES (2, 'Gadget', 3, 89.97)");
        $this->pdo->exec("UPDATE pg_trig_orders SET total = 59.94 WHERE id = 1");
        $this->pdo->exec('DELETE FROM pg_trig_orders WHERE id = 2');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_trig_orders');
        $this->assertSame(1, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT product, total FROM pg_trig_orders WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Widget', $row['product']);
        $this->assertEquals(59.94, (float) $row['total']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_trig_orders VALUES (1, 'Widget', 5, 49.95)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_trig_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
                'test',
                'test',
            );
            $raw->exec('DROP TRIGGER IF EXISTS pg_trig_after_insert ON pg_trig_orders');
            $raw->exec('DROP TRIGGER IF EXISTS pg_trig_after_update ON pg_trig_orders');
            $raw->exec('DROP TRIGGER IF EXISTS pg_trig_after_delete ON pg_trig_orders');
            $raw->exec('DROP FUNCTION IF EXISTS pg_trig_audit_func()');
            $raw->exec('DROP TABLE IF EXISTS pg_trig_audit_log');
            $raw->exec('DROP TABLE IF EXISTS pg_trig_orders');
        } catch (\Exception $e) {
        }
    }
}
