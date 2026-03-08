<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests MySQL multi-table DELETE via MySQLi.
 *
 * Cross-platform parity with MysqlMultiTableDeleteTest (PDO).
 */
class MultiTableDeleteTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_md_orders');
        $raw->query('DROP TABLE IF EXISTS mi_md_users');
        $raw->query('CREATE TABLE mi_md_users (id INT PRIMARY KEY, name VARCHAR(50), active TINYINT DEFAULT 1)');
        $raw->query('CREATE TABLE mi_md_orders (id INT PRIMARY KEY, user_id INT, amount INT)');
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

        $this->mysqli->query("INSERT INTO mi_md_users (id, name, active) VALUES (1, 'Alice', 1)");
        $this->mysqli->query("INSERT INTO mi_md_users (id, name, active) VALUES (2, 'Bob', 0)");
        $this->mysqli->query("INSERT INTO mi_md_users (id, name, active) VALUES (3, 'Charlie', 1)");
        $this->mysqli->query("INSERT INTO mi_md_orders (id, user_id, amount) VALUES (1, 1, 100)");
        $this->mysqli->query("INSERT INTO mi_md_orders (id, user_id, amount) VALUES (2, 2, 200)");
        $this->mysqli->query("INSERT INTO mi_md_orders (id, user_id, amount) VALUES (3, 2, 50)");
        $this->mysqli->query("INSERT INTO mi_md_orders (id, user_id, amount) VALUES (4, 3, 300)");
    }

    /**
     * Single-target DELETE with JOIN.
     */
    public function testSingleTargetDeleteWithJoin(): void
    {
        $this->mysqli->query(
            'DELETE o FROM mi_md_orders o JOIN mi_md_users u ON o.user_id = u.id WHERE u.active = 0'
        );

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_orders WHERE user_id = 2');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_orders');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Multi-target DELETE only affects the first table.
     */
    public function testMultiTargetDeleteOnlyAffectsFirstTable(): void
    {
        $this->mysqli->query(
            'DELETE u, o FROM mi_md_users u JOIN mi_md_orders o ON u.id = o.user_id WHERE u.active = 0'
        );

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_md_users WHERE name = 'Bob'");
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);

        // Second table (orders): Bob's orders are NOT deleted
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_orders WHERE user_id = 2');
        $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_users');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Multi-target DELETE with prepared statement.
     */
    public function testMultiTargetDeletePrepared(): void
    {
        $stmt = $this->mysqli->prepare(
            'DELETE o FROM mi_md_orders o JOIN mi_md_users u ON o.user_id = u.id WHERE u.name = ?'
        );
        $name = 'Bob';
        $stmt->bind_param('s', $name);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_orders WHERE user_id = 2');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query(
            'DELETE u, o FROM mi_md_users u JOIN mi_md_orders o ON u.id = o.user_id WHERE u.active = 0'
        );

        $this->mysqli->disableZtd();

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_users');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_orders');
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
            $raw->query('DROP TABLE IF EXISTS mi_md_orders');
            $raw->query('DROP TABLE IF EXISTS mi_md_users');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
