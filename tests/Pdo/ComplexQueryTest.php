<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

class ComplexQueryTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS order_items');
        $raw->exec('DROP TABLE IF EXISTS orders');
        $raw->exec('DROP TABLE IF EXISTS users');
        $raw->exec('CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), department VARCHAR(255))');
        $raw->exec('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount DECIMAL(10,2), status VARCHAR(50))');
        $raw->exec('CREATE TABLE order_items (id INT PRIMARY KEY, order_id INT, product VARCHAR(255), qty INT)');

        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public function testJoinWithShadowData(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO orders (id, user_id, amount, status) VALUES (10, 1, 99.99, 'completed')");

        $stmt = $this->pdo->query(
            'SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.id = 1'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('99.99', $rows[0]['amount']);
    }

    public function testLeftJoinWithShadowData(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Sales')");
        $this->pdo->exec("INSERT INTO orders (id, user_id, amount, status) VALUES (10, 1, 50.00, 'completed')");

        $stmt = $this->pdo->query(
            'SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('50.00', $rows[0]['amount']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertNull($rows[1]['amount']);
    }

    public function testCountAggregation(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Engineering')");
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (3, 'Charlie', 'Sales')");

        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM users');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame(3, (int) $rows[0]['cnt']);
    }

    public function testGroupByWithCount(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Engineering')");
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (3, 'Charlie', 'Sales')");

        $stmt = $this->pdo->query(
            'SELECT department, COUNT(*) AS cnt FROM users GROUP BY department ORDER BY department'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['department']);
        $this->assertSame(2, (int) $rows[0]['cnt']);
        $this->assertSame('Sales', $rows[1]['department']);
        $this->assertSame(1, (int) $rows[1]['cnt']);
    }

    public function testSumAggregation(): void
    {
        $this->pdo->exec("INSERT INTO orders (id, user_id, amount, status) VALUES (1, 1, 100.00, 'completed')");
        $this->pdo->exec("INSERT INTO orders (id, user_id, amount, status) VALUES (2, 1, 200.50, 'completed')");
        $this->pdo->exec("INSERT INTO orders (id, user_id, amount, status) VALUES (3, 2, 50.00, 'pending')");

        $stmt = $this->pdo->query("SELECT SUM(amount) AS total FROM orders WHERE status = 'completed'");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame('300.50', $rows[0]['total']);
    }

    public function testOrderByAndLimit(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Sales')");
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (3, 'Charlie', 'Engineering')");

        $stmt = $this->pdo->query('SELECT name FROM users ORDER BY name ASC LIMIT 2');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testLimitWithOffset(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'A')");
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'B')");
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (3, 'Charlie', 'C')");

        $stmt = $this->pdo->query('SELECT name FROM users ORDER BY id LIMIT 2 OFFSET 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    public function testSubqueryInWhere(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Sales')");
        $this->pdo->exec("INSERT INTO orders (id, user_id, amount, status) VALUES (10, 1, 99.99, 'completed')");

        $stmt = $this->pdo->query(
            'SELECT name FROM users WHERE id IN (SELECT user_id FROM orders)'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testHavingClause(): void
    {
        $this->pdo->exec("INSERT INTO orders (id, user_id, amount, status) VALUES (1, 1, 100.00, 'completed')");
        $this->pdo->exec("INSERT INTO orders (id, user_id, amount, status) VALUES (2, 1, 200.00, 'completed')");
        $this->pdo->exec("INSERT INTO orders (id, user_id, amount, status) VALUES (3, 2, 50.00, 'completed')");

        $stmt = $this->pdo->query(
            'SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id HAVING total > 100 ORDER BY user_id'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['user_id']);
        $this->assertSame('300.00', $rows[0]['total']);
    }

    public function testDistinctSelect(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Engineering')");
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (3, 'Charlie', 'Sales')");

        $stmt = $this->pdo->query('SELECT DISTINCT department FROM users ORDER BY department');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['department']);
        $this->assertSame('Sales', $rows[1]['department']);
    }

    public function testJoinWithAggregationOnShadowData(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Sales')");
        $this->pdo->exec("INSERT INTO orders (id, user_id, amount, status) VALUES (1, 1, 100.00, 'completed')");
        $this->pdo->exec("INSERT INTO orders (id, user_id, amount, status) VALUES (2, 1, 200.00, 'completed')");
        $this->pdo->exec("INSERT INTO orders (id, user_id, amount, status) VALUES (3, 2, 50.00, 'pending')");

        $stmt = $this->pdo->query(
            'SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total '
            . 'FROM users u INNER JOIN orders o ON u.id = o.user_id '
            . 'GROUP BY u.id, u.name ORDER BY u.name'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(2, (int) $rows[0]['order_count']);
        $this->assertSame('300.00', $rows[0]['total']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame(1, (int) $rows[1]['order_count']);
        $this->assertSame('50.00', $rows[1]['total']);
    }

    public function testUpdateWithSubquery(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO orders (id, user_id, amount, status) VALUES (1, 1, 100.00, 'pending')");

        $this->pdo->exec(
            "UPDATE orders SET status = 'completed' WHERE user_id IN (SELECT id FROM users WHERE department = 'Engineering')"
        );

        $stmt = $this->pdo->query('SELECT status FROM orders WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame('completed', $rows[0]['status']);
    }

    public function testDeleteWithSubquery(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Sales')");
        $this->pdo->exec("INSERT INTO orders (id, user_id, amount, status) VALUES (1, 1, 100.00, 'completed')");
        $this->pdo->exec("INSERT INTO orders (id, user_id, amount, status) VALUES (2, 2, 50.00, 'completed')");

        $this->pdo->exec(
            "DELETE FROM orders WHERE user_id IN (SELECT id FROM users WHERE department = 'Sales')"
        );

        $stmt = $this->pdo->query('SELECT * FROM orders');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['id']);
    }

    public function testZtdSelectReadsOnlyFromShadowStore(): void
    {
        // Insert physical data via raw connection
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");

        // Physical data is visible with ZTD disabled (passthrough)
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM users');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC),
            'Physical data visible with ZTD disabled');
        $this->pdo->enableZtd();

        // With ZTD enabled, SELECT reads only from the shadow store.
        // Physical table data is NOT merged — the shadow store is an
        // independent layer that replaces the physical table entirely.
        $stmt = $this->pdo->query('SELECT * FROM users');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC),
            'ZTD SELECT reads only from shadow store; physical data is not visible');
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS order_items');
        $raw->exec('DROP TABLE IF EXISTS orders');
        $raw->exec('DROP TABLE IF EXISTS users');
    }
}
