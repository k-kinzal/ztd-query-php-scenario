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
 * Tests correlated subqueries in SELECT after mutations on PostgreSQL.
 *
 * Correlated subqueries reference the outer query's row and must
 * correctly reflect shadow mutations (INSERT/UPDATE/DELETE).
 */
class PostgresCorrelatedSubqueryAfterMutationTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(PostgreSQLContainer::getDsn(), 'test', 'test');
        $raw->exec('DROP TABLE IF EXISTS pg_corr_orders');
        $raw->exec('DROP TABLE IF EXISTS pg_corr_customers');
        $raw->exec('CREATE TABLE pg_corr_customers (id INT PRIMARY KEY, name VARCHAR(50))');
        $raw->exec('CREATE TABLE pg_corr_orders (id INT PRIMARY KEY, customer_id INT, amount NUMERIC(10,2))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(PostgreSQLContainer::getDsn(), 'test', 'test');

        $this->pdo->exec("INSERT INTO pg_corr_customers VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pg_corr_customers VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO pg_corr_customers VALUES (3, 'Charlie')");

        $this->pdo->exec("INSERT INTO pg_corr_orders VALUES (1, 1, 100.00)");
        $this->pdo->exec("INSERT INTO pg_corr_orders VALUES (2, 1, 200.00)");
        $this->pdo->exec("INSERT INTO pg_corr_orders VALUES (3, 2, 150.00)");
    }

    /**
     * Scalar correlated subquery in SELECT list.
     */
    public function testScalarCorrelatedSubquery(): void
    {
        $stmt = $this->pdo->query('
            SELECT c.name,
                   (SELECT SUM(o.amount) FROM pg_corr_orders o WHERE o.customer_id = c.id) AS total
            FROM pg_corr_customers c
            ORDER BY c.id
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total'], 0.01);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEqualsWithDelta(150.0, (float) $rows[1]['total'], 0.01);
        $this->assertNull($rows[2]['total']); // Charlie has no orders
    }

    /**
     * Correlated subquery reflects INSERT mutation.
     */
    public function testCorrelatedSubqueryReflectsInsert(): void
    {
        $this->pdo->exec("INSERT INTO pg_corr_orders VALUES (4, 3, 500.00)");

        $stmt = $this->pdo->query('
            SELECT (SELECT SUM(o.amount) FROM pg_corr_orders o WHERE o.customer_id = c.id) AS total
            FROM pg_corr_customers c
            WHERE c.id = 3
        ');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(500.0, (float) $row['total'], 0.01);
    }

    /**
     * EXISTS correlated subquery after mutation.
     */
    public function testExistsAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO pg_corr_orders VALUES (4, 3, 50.00)");

        $stmt = $this->pdo->query('
            SELECT c.name
            FROM pg_corr_customers c
            WHERE EXISTS (SELECT 1 FROM pg_corr_orders o WHERE o.customer_id = c.id)
            ORDER BY c.name
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(3, $rows);
    }

    /**
     * NOT EXISTS after DELETE.
     */
    public function testNotExistsAfterDelete(): void
    {
        $this->pdo->exec('DELETE FROM pg_corr_orders WHERE customer_id = 2');

        $stmt = $this->pdo->query('
            SELECT c.name
            FROM pg_corr_customers c
            WHERE NOT EXISTS (SELECT 1 FROM pg_corr_orders o WHERE o.customer_id = c.id)
            ORDER BY c.name
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertContains('Bob', $rows);
        $this->assertContains('Charlie', $rows);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_corr_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(PostgreSQLContainer::getDsn(), 'test', 'test');
            $raw->exec('DROP TABLE IF EXISTS pg_corr_orders');
            $raw->exec('DROP TABLE IF EXISTS pg_corr_customers');
        } catch (\Exception $e) {
        }
    }
}
