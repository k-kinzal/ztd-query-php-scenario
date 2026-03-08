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
 * Tests prepared DELETE with subqueries on PostgreSQL PDO.
 *
 * Cross-platform parity with SqlitePreparedDeleteWithSubqueryTest.
 * @spec pending
 */
class PostgresPreparedDeleteWithSubqueryTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pdo_ppdel_orders');
        $raw->exec('DROP TABLE IF EXISTS pdo_ppdel_customers');
        $raw->exec('CREATE TABLE pdo_ppdel_customers (id INT PRIMARY KEY, name VARCHAR(50), tier VARCHAR(20))');
        $raw->exec('CREATE TABLE pdo_ppdel_orders (id INT PRIMARY KEY, customer_id INT, amount DECIMAL(10,2))');
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

        $this->pdo->exec("INSERT INTO pdo_ppdel_customers VALUES (1, 'Alice', 'gold')");
        $this->pdo->exec("INSERT INTO pdo_ppdel_customers VALUES (2, 'Bob', 'silver')");
        $this->pdo->exec("INSERT INTO pdo_ppdel_customers VALUES (3, 'Charlie', 'bronze')");

        $this->pdo->exec('INSERT INTO pdo_ppdel_orders VALUES (1, 1, 100.00)');
        $this->pdo->exec('INSERT INTO pdo_ppdel_orders VALUES (2, 1, 200.00)');
        $this->pdo->exec('INSERT INTO pdo_ppdel_orders VALUES (3, 2, 50.00)');
    }

    /**
     * Prepared DELETE with IN subquery.
     */
    public function testPreparedDeleteWithInSubquery(): void
    {
        $stmt = $this->pdo->prepare('
            DELETE FROM pdo_ppdel_orders
            WHERE customer_id IN (SELECT id FROM pdo_ppdel_customers WHERE tier = ?)
        ');
        $stmt->execute(['gold']);

        $qstmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_ppdel_orders');
        $this->assertSame(1, (int) $qstmt->fetchColumn());
    }

    /**
     * Prepared DELETE with simple WHERE.
     */
    public function testPreparedDeleteWithSimpleWhere(): void
    {
        $stmt = $this->pdo->prepare('DELETE FROM pdo_ppdel_customers WHERE tier = ?');
        $stmt->execute(['bronze']);

        $qstmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_ppdel_customers');
        $this->assertSame(2, (int) $qstmt->fetchColumn());
    }

    /**
     * Prepared DELETE then correlated SELECT.
     */
    public function testPreparedDeleteThenCorrelatedSelect(): void
    {
        $stmt = $this->pdo->prepare('DELETE FROM pdo_ppdel_orders WHERE customer_id = ?');
        $stmt->execute([1]);

        $qstmt = $this->pdo->query('
            SELECT c.name,
                   (SELECT COUNT(*) FROM pdo_ppdel_orders o WHERE o.customer_id = c.id) AS cnt
            FROM pdo_ppdel_customers c
            ORDER BY c.id
        ');
        $rows = $qstmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']); // Alice — deleted
        $this->assertSame(1, (int) $rows[1]['cnt']); // Bob
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $stmt = $this->pdo->prepare('DELETE FROM pdo_ppdel_customers WHERE tier = ?');
        $stmt->execute(['gold']);

        $this->pdo->disableZtd();
        $qstmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_ppdel_customers');
        $this->assertSame(0, (int) $qstmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
                'test',
                'test',
            );
            $raw->exec('DROP TABLE IF EXISTS pdo_ppdel_orders');
            $raw->exec('DROP TABLE IF EXISTS pdo_ppdel_customers');
        } catch (\Exception $e) {
        }
    }
}
