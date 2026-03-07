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
 * Tests PostgreSQL-specific multi-table operations.
 * PostgreSQL uses UPDATE ... FROM and DELETE ... USING syntax
 * instead of MySQL's JOIN-based syntax.
 */
class PostgresMultiTableOperationsTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_orders');
        $raw->exec('DROP TABLE IF EXISTS pg_users');
        $raw->exec('CREATE TABLE pg_users (id INT PRIMARY KEY, name VARCHAR(255), active INT DEFAULT 1)');
        $raw->exec('CREATE TABLE pg_orders (id INT PRIMARY KEY, user_id INT, amount DECIMAL(10,2))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pg_users (id, name, active) VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO pg_users (id, name, active) VALUES (2, 'Bob', 1)");
        $this->pdo->exec("INSERT INTO pg_orders (id, user_id, amount) VALUES (1, 1, 100.00)");
        $this->pdo->exec("INSERT INTO pg_orders (id, user_id, amount) VALUES (2, 1, 200.00)");
        $this->pdo->exec("INSERT INTO pg_orders (id, user_id, amount) VALUES (3, 2, 50.00)");
    }

    public function testUpdateWithFrom(): void
    {
        // PostgreSQL multi-table UPDATE uses FROM clause
        $this->pdo->exec("UPDATE pg_users SET active = 0 FROM pg_orders WHERE pg_users.id = pg_orders.user_id AND pg_orders.amount > 150");

        $stmt = $this->pdo->query('SELECT active FROM pg_users WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['active']);

        $stmt = $this->pdo->query('SELECT active FROM pg_users WHERE id = 2');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $rows[0]['active']);
    }

    public function testDeleteWithUsing(): void
    {
        // PostgreSQL multi-table DELETE uses USING clause
        $this->pdo->exec("DELETE FROM pg_orders USING pg_users WHERE pg_orders.user_id = pg_users.id AND pg_users.name = 'Bob'");

        $stmt = $this->pdo->query('SELECT * FROM pg_orders');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }

    public function testUpdateWithFromIsolation(): void
    {
        $this->pdo->exec("UPDATE pg_users SET active = 0 FROM pg_orders WHERE pg_users.id = pg_orders.user_id AND pg_orders.amount > 150");

        // Physical table should be unchanged
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM pg_users');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_orders');
        $raw->exec('DROP TABLE IF EXISTS pg_users');
    }
}
