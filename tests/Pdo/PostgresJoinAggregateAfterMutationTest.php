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
 * Tests JOIN + aggregate queries after shadow mutations on PostgreSQL PDO.
 *
 * Cross-platform parity with SqliteJoinAggregateAfterMutationTest.
 */
class PostgresJoinAggregateAfterMutationTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
            'test',
            'test',
        );
        $raw->exec('DROP TABLE IF EXISTS pdo_pjag_orders');
        $raw->exec('DROP TABLE IF EXISTS pdo_pjag_customers');
        $raw->exec('CREATE TABLE pdo_pjag_customers (id INT PRIMARY KEY, name VARCHAR(50))');
        $raw->exec('CREATE TABLE pdo_pjag_orders (id INT PRIMARY KEY, customer_id INT, amount DECIMAL(10,2))');
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO pdo_pjag_customers VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pdo_pjag_customers VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO pdo_pjag_customers VALUES (3, 'Charlie')");

        $this->pdo->exec('INSERT INTO pdo_pjag_orders VALUES (1, 1, 100.00)');
        $this->pdo->exec('INSERT INTO pdo_pjag_orders VALUES (2, 1, 200.00)');
        $this->pdo->exec('INSERT INTO pdo_pjag_orders VALUES (3, 2, 50.00)');
    }

    /**
     * LEFT JOIN with COUNT after INSERT.
     */
    public function testLeftJoinCountAfterInsert(): void
    {
        $this->pdo->exec('INSERT INTO pdo_pjag_orders VALUES (4, 3, 75.00)');

        $stmt = $this->pdo->query('
            SELECT c.name, COUNT(o.id) AS order_count
            FROM pdo_pjag_customers c
            LEFT JOIN pdo_pjag_orders o ON c.id = o.customer_id
            GROUP BY c.id, c.name
            ORDER BY c.id
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $rows[0]['order_count']); // Alice
        $this->assertSame(1, (int) $rows[1]['order_count']); // Bob
        $this->assertSame(1, (int) $rows[2]['order_count']); // Charlie (new)
    }

    /**
     * SUM aggregate after UPDATE.
     */
    public function testSumAfterUpdate(): void
    {
        $this->pdo->exec('UPDATE pdo_pjag_orders SET amount = 500.00 WHERE id = 1');

        $stmt = $this->pdo->query('
            SELECT c.name, SUM(o.amount) AS total
            FROM pdo_pjag_customers c
            JOIN pdo_pjag_orders o ON c.id = o.customer_id
            GROUP BY c.id, c.name
            ORDER BY c.id
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(700.00, (float) $rows[0]['total'], 0.01);
    }

    /**
     * LEFT JOIN after DELETE shows zero count.
     */
    public function testLeftJoinAfterDeleteAllOrders(): void
    {
        $this->pdo->exec('DELETE FROM pdo_pjag_orders WHERE customer_id = 1');

        $stmt = $this->pdo->query('
            SELECT c.name, COUNT(o.id) AS order_count
            FROM pdo_pjag_customers c
            LEFT JOIN pdo_pjag_orders o ON c.id = o.customer_id
            GROUP BY c.id, c.name
            ORDER BY c.id
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['order_count']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_pjag_customers');
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
            $raw->exec('DROP TABLE IF EXISTS pdo_pjag_orders');
            $raw->exec('DROP TABLE IF EXISTS pdo_pjag_customers');
        } catch (\Exception $e) {
        }
    }
}
