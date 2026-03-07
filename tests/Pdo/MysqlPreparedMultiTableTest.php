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
 * Tests multi-table UPDATE/DELETE with prepared statement parameters on MySQL PDO.
 */
class MysqlPreparedMultiTableTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pmt_orders');
        $raw->exec('DROP TABLE IF EXISTS pmt_users');
        $raw->exec('CREATE TABLE pmt_users (id INT PRIMARY KEY, name VARCHAR(50), active TINYINT)');
        $raw->exec('CREATE TABLE pmt_orders (id INT PRIMARY KEY, user_id INT, amount DECIMAL(10,2), status VARCHAR(20))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pmt_users (id, name, active) VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO pmt_users (id, name, active) VALUES (2, 'Bob', 1)");
        $this->pdo->exec("INSERT INTO pmt_users (id, name, active) VALUES (3, 'Charlie', 0)");
        $this->pdo->exec("INSERT INTO pmt_orders (id, user_id, amount, status) VALUES (1, 1, 100.0, 'completed')");
        $this->pdo->exec("INSERT INTO pmt_orders (id, user_id, amount, status) VALUES (2, 1, 200.0, 'completed')");
        $this->pdo->exec("INSERT INTO pmt_orders (id, user_id, amount, status) VALUES (3, 2, 50.0, 'pending')");
        $this->pdo->exec("INSERT INTO pmt_orders (id, user_id, amount, status) VALUES (4, 3, 75.0, 'completed')");
    }

    public function testPreparedMultiTableUpdate(): void
    {
        // UPDATE users JOIN orders to deactivate users with high-value orders
        $stmt = $this->pdo->prepare(
            'UPDATE pmt_users u JOIN pmt_orders o ON u.id = o.user_id SET u.active = 0 WHERE o.amount > ?'
        );
        $stmt->execute([150.0]);
        $this->assertSame(1, $stmt->rowCount());

        // Alice should now be inactive (she has order of 200.0)
        $result = $this->pdo->query("SELECT name, active FROM pmt_users WHERE id = 1");
        $row = $result->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $row['active']);
    }

    public function testPreparedMultiTableDelete(): void
    {
        // DELETE orders for inactive users
        $stmt = $this->pdo->prepare(
            'DELETE o FROM pmt_orders o JOIN pmt_users u ON o.user_id = u.id WHERE u.active = ?'
        );
        $stmt->execute([0]);
        $this->assertSame(1, $stmt->rowCount());

        // Order id=4 (Charlie, inactive) should be deleted
        $result = $this->pdo->query('SELECT COUNT(*) AS cnt FROM pmt_orders');
        $this->assertSame(3, (int) $result->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    public function testPreparedJoinSelectWithParams(): void
    {
        $stmt = $this->pdo->prepare(
            'SELECT u.name, SUM(o.amount) AS total '
            . 'FROM pmt_users u '
            . 'JOIN pmt_orders o ON u.id = o.user_id '
            . 'WHERE o.status = ? '
            . 'GROUP BY u.name '
            . 'HAVING SUM(o.amount) > ? '
            . 'ORDER BY total DESC'
        );
        $stmt->execute(['completed', 100.0]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total'], 0.01);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pmt_orders');
        $raw->exec('DROP TABLE IF EXISTS pmt_users');
    }
}
