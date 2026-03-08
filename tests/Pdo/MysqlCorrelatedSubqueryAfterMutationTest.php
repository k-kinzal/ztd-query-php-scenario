<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests correlated subqueries after mutations on MySQL PDO.
 *
 * Cross-platform parity with SqliteCorrelatedSubqueryAfterMutationTest
 * and PostgresCorrelatedSubqueryAfterMutationTest.
 */
class MysqlCorrelatedSubqueryAfterMutationTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
        $raw->exec('DROP TABLE IF EXISTS pdo_corr_orders');
        $raw->exec('DROP TABLE IF EXISTS pdo_corr_customers');
        $raw->exec('CREATE TABLE pdo_corr_customers (id INT PRIMARY KEY, name VARCHAR(50))');
        $raw->exec('CREATE TABLE pdo_corr_orders (id INT PRIMARY KEY, customer_id INT, amount DECIMAL(10,2))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(MySQLContainer::getDsn(), 'root', 'root');

        $this->pdo->exec("INSERT INTO pdo_corr_customers VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pdo_corr_customers VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO pdo_corr_customers VALUES (3, 'Charlie')");

        $this->pdo->exec("INSERT INTO pdo_corr_orders VALUES (1, 1, 100.00)");
        $this->pdo->exec("INSERT INTO pdo_corr_orders VALUES (2, 1, 200.00)");
        $this->pdo->exec("INSERT INTO pdo_corr_orders VALUES (3, 2, 150.00)");
    }

    /**
     * Scalar correlated subquery in SELECT.
     */
    public function testScalarCorrelatedSubquery(): void
    {
        $stmt = $this->pdo->query('
            SELECT c.name,
                   (SELECT SUM(o.amount) FROM pdo_corr_orders o WHERE o.customer_id = c.id) AS total
            FROM pdo_corr_customers c
            ORDER BY c.id
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total'], 0.01);
        $this->assertEqualsWithDelta(150.0, (float) $rows[1]['total'], 0.01);
        $this->assertNull($rows[2]['total']);
    }

    /**
     * Correlated subquery reflects INSERT.
     */
    public function testCorrelatedSubqueryReflectsInsert(): void
    {
        $this->pdo->exec("INSERT INTO pdo_corr_orders VALUES (4, 3, 500.00)");

        $stmt = $this->pdo->query('
            SELECT (SELECT SUM(o.amount) FROM pdo_corr_orders o WHERE o.customer_id = c.id) AS total
            FROM pdo_corr_customers c
            WHERE c.id = 3
        ');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(500.0, (float) $row['total'], 0.01);
    }

    /**
     * EXISTS correlated subquery after INSERT.
     */
    public function testExistsAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO pdo_corr_orders VALUES (4, 3, 50.00)");

        $stmt = $this->pdo->query('
            SELECT c.name
            FROM pdo_corr_customers c
            WHERE EXISTS (SELECT 1 FROM pdo_corr_orders o WHERE o.customer_id = c.id)
            ORDER BY c.name
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(3, $rows);
    }

    /**
     * Correlated subquery with COUNT.
     */
    public function testCorrelatedSubqueryWithCount(): void
    {
        $stmt = $this->pdo->query('
            SELECT c.name,
                   (SELECT COUNT(*) FROM pdo_corr_orders o WHERE o.customer_id = c.id) AS order_count
            FROM pdo_corr_customers c
            ORDER BY c.id
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $rows[0]['order_count']);
        $this->assertSame(1, (int) $rows[1]['order_count']);
        $this->assertSame(0, (int) $rows[2]['order_count']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_corr_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP TABLE IF EXISTS pdo_corr_orders');
            $raw->exec('DROP TABLE IF EXISTS pdo_corr_customers');
        } catch (\Exception $e) {
        }
    }
}
