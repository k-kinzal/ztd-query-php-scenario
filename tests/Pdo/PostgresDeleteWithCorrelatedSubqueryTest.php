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
 * Tests DELETE with correlated subqueries on PostgreSQL.
 *
 * Cross-platform parity with SqliteDeleteWithCorrelatedSubqueryTest.
 */
class PostgresDeleteWithCorrelatedSubqueryTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(PostgreSQLContainer::getDsn(), 'test', 'test');
        $raw->exec('DROP TABLE IF EXISTS pg_del_orders');
        $raw->exec('DROP TABLE IF EXISTS pg_del_customers');
        $raw->exec('CREATE TABLE pg_del_customers (id INT PRIMARY KEY, name VARCHAR(50), active INT)');
        $raw->exec('CREATE TABLE pg_del_orders (id INT PRIMARY KEY, customer_id INT, amount NUMERIC(10,2))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(PostgreSQLContainer::getDsn(), 'test', 'test');

        $this->pdo->exec("INSERT INTO pg_del_customers VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO pg_del_customers VALUES (2, 'Bob', 0)");
        $this->pdo->exec("INSERT INTO pg_del_customers VALUES (3, 'Charlie', 1)");

        $this->pdo->exec("INSERT INTO pg_del_orders VALUES (1, 1, 100.00)");
        $this->pdo->exec("INSERT INTO pg_del_orders VALUES (2, 1, 200.00)");
        $this->pdo->exec("INSERT INTO pg_del_orders VALUES (3, 3, 150.00)");
    }

    /**
     * DELETE with EXISTS correlated subquery.
     */
    public function testDeleteWithExistsSubquery(): void
    {
        $this->pdo->exec('
            DELETE FROM pg_del_customers
            WHERE EXISTS (SELECT 1 FROM pg_del_orders o WHERE o.customer_id = pg_del_customers.id)
        ');

        $stmt = $this->pdo->query('SELECT name FROM pg_del_customers ORDER BY name');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]);
    }

    /**
     * DELETE with NOT EXISTS.
     */
    public function testDeleteWithNotExistsSubquery(): void
    {
        $this->pdo->exec('
            DELETE FROM pg_del_customers
            WHERE NOT EXISTS (SELECT 1 FROM pg_del_orders o WHERE o.customer_id = pg_del_customers.id)
        ');

        $stmt = $this->pdo->query('SELECT name FROM pg_del_customers ORDER BY name');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]);
        $this->assertSame('Charlie', $rows[1]);
    }

    /**
     * DELETE with IN subquery.
     */
    public function testDeleteWithInSubquery(): void
    {
        $this->pdo->exec('
            DELETE FROM pg_del_orders
            WHERE customer_id IN (SELECT id FROM pg_del_customers WHERE active = 0)
        ');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_del_orders');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('
            DELETE FROM pg_del_customers
            WHERE EXISTS (SELECT 1 FROM pg_del_orders o WHERE o.customer_id = pg_del_customers.id)
        ');

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_del_customers');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(PostgreSQLContainer::getDsn(), 'test', 'test');
            $raw->exec('DROP TABLE IF EXISTS pg_del_orders');
            $raw->exec('DROP TABLE IF EXISTS pg_del_customers');
        } catch (\Exception $e) {
        }
    }
}
