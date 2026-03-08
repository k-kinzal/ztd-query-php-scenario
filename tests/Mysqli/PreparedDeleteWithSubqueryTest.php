<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests prepared DELETE with subqueries via MySQLi.
 *
 * Cross-platform parity with SqlitePreparedDeleteWithSubqueryTest (PDO).
 */
class PreparedDeleteWithSubqueryTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_pdel_orders');
        $raw->query('DROP TABLE IF EXISTS mi_pdel_customers');
        $raw->query('CREATE TABLE mi_pdel_customers (id INT PRIMARY KEY, name VARCHAR(50), tier VARCHAR(20))');
        $raw->query('CREATE TABLE mi_pdel_orders (id INT PRIMARY KEY, customer_id INT, amount DECIMAL(10,2))');
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

        $this->mysqli->query("INSERT INTO mi_pdel_customers VALUES (1, 'Alice', 'gold')");
        $this->mysqli->query("INSERT INTO mi_pdel_customers VALUES (2, 'Bob', 'silver')");
        $this->mysqli->query("INSERT INTO mi_pdel_customers VALUES (3, 'Charlie', 'bronze')");

        $this->mysqli->query('INSERT INTO mi_pdel_orders VALUES (1, 1, 100.00)');
        $this->mysqli->query('INSERT INTO mi_pdel_orders VALUES (2, 1, 200.00)');
        $this->mysqli->query('INSERT INTO mi_pdel_orders VALUES (3, 2, 50.00)');
    }

    /**
     * Prepared DELETE with IN subquery.
     */
    public function testPreparedDeleteWithInSubquery(): void
    {
        $stmt = $this->mysqli->prepare('
            DELETE FROM mi_pdel_orders
            WHERE customer_id IN (SELECT id FROM mi_pdel_customers WHERE tier = ?)
        ');
        $tier = 'gold';
        $stmt->bind_param('s', $tier);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_pdel_orders');
        $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Prepared DELETE with simple WHERE.
     */
    public function testPreparedDeleteWithSimpleWhere(): void
    {
        $stmt = $this->mysqli->prepare('DELETE FROM mi_pdel_customers WHERE tier = ?');
        $tier = 'bronze';
        $stmt->bind_param('s', $tier);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_pdel_customers');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Prepared DELETE then correlated SELECT.
     */
    public function testPreparedDeleteThenCorrelatedSelect(): void
    {
        $stmt = $this->mysqli->prepare('DELETE FROM mi_pdel_orders WHERE customer_id = ?');
        $custId = 1;
        $stmt->bind_param('i', $custId);
        $stmt->execute();

        $result = $this->mysqli->query('
            SELECT c.name,
                   (SELECT COUNT(*) FROM mi_pdel_orders o WHERE o.customer_id = c.id) AS cnt
            FROM mi_pdel_customers c
            ORDER BY c.id
        ');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']); // Alice — deleted
        $this->assertSame(1, (int) $rows[1]['cnt']); // Bob
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $stmt = $this->mysqli->prepare('DELETE FROM mi_pdel_customers WHERE tier = ?');
        $tier = 'gold';
        $stmt->bind_param('s', $tier);
        $stmt->execute();

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_pdel_customers');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    protected function tearDown(): void
    {
        if (isset($this->mysqli)) {
            $this->mysqli->close();
        }
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new \mysqli(
                MySQLContainer::getHost(),
                'root',
                'root',
                'test',
                MySQLContainer::getPort(),
            );
            $raw->query('DROP TABLE IF EXISTS mi_pdel_orders');
            $raw->query('DROP TABLE IF EXISTS mi_pdel_customers');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
