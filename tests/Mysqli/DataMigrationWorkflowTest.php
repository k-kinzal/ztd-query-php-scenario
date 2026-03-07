<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests a data migration workflow on MySQLi: INSERT...SELECT with transformations,
 * subqueries in SELECT/WHERE, and complex aggregation patterns.
 */
class DataMigrationWorkflowTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_dm_user_stats');
        $raw->query('DROP TABLE IF EXISTS mi_dm_orders');
        $raw->query('DROP TABLE IF EXISTS mi_dm_users');
        $raw->query('CREATE TABLE mi_dm_users (id INT PRIMARY KEY, name VARCHAR(50), email VARCHAR(100), tier VARCHAR(20), created_date DATE)');
        $raw->query('CREATE TABLE mi_dm_orders (id INT PRIMARY KEY, user_id INT, product VARCHAR(50), amount DECIMAL(10,2), status VARCHAR(20), created_date DATE)');
        $raw->query('CREATE TABLE mi_dm_user_stats (user_id INT PRIMARY KEY, total_orders INT, total_spent DECIMAL(10,2), last_order_date DATE)');
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
    }

    /**
     * INSERT...SELECT with LEFT JOIN + GROUP BY + aggregation throws a column-not-found
     * error on MySQL — the CTE rewriter loses the JOIN alias references.
     * (On SQLite, this silently produces NULL values instead.)
     */
    public function testInsertSelectWithJoinGroupByThrowsError(): void
    {
        $this->mysqli->query("INSERT INTO mi_dm_users VALUES (1, 'Alice', 'alice@co.com', 'gold', '2023-01-15')");
        $this->mysqli->query("INSERT INTO mi_dm_users VALUES (2, 'Bob', 'bob@co.com', 'silver', '2023-06-20')");

        $this->mysqli->query("INSERT INTO mi_dm_orders VALUES (1, 1, 'Widget', 100, 'completed', '2024-01-05')");
        $this->mysqli->query("INSERT INTO mi_dm_orders VALUES (2, 1, 'Gadget', 200, 'completed', '2024-02-10')");

        $this->expectException(\Throwable::class);
        $this->mysqli->query("
            INSERT INTO mi_dm_user_stats (user_id, total_orders, total_spent, last_order_date)
            SELECT u.id, COUNT(o.id), COALESCE(SUM(o.amount), 0), MAX(o.created_date)
            FROM mi_dm_users u
            LEFT JOIN mi_dm_orders o ON o.user_id = u.id
            GROUP BY u.id
        ");
    }

    public function testMigrationWorkflowWithManualInserts(): void
    {
        $this->mysqli->query("INSERT INTO mi_dm_users VALUES (1, 'Alice', 'alice@co.com', 'gold', '2023-01-15')");
        $this->mysqli->query("INSERT INTO mi_dm_users VALUES (2, 'Bob', 'bob@co.com', 'silver', '2023-06-20')");
        $this->mysqli->query("INSERT INTO mi_dm_orders VALUES (1, 1, 'Widget', 100, 'completed', '2024-01-05')");
        $this->mysqli->query("INSERT INTO mi_dm_orders VALUES (2, 1, 'Gadget', 200, 'completed', '2024-02-10')");
        $this->mysqli->query("INSERT INTO mi_dm_orders VALUES (3, 2, 'Widget', 75, 'completed', '2024-01-15')");

        $result = $this->mysqli->query("
            SELECT u.id, COUNT(o.id) AS total_orders, SUM(o.amount) AS total_spent, MAX(o.created_date) AS last_order
            FROM mi_dm_users u LEFT JOIN mi_dm_orders o ON o.user_id = u.id
            GROUP BY u.id ORDER BY u.id
        ");
        $stats = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $stats);

        foreach ($stats as $s) {
            $this->mysqli->query(sprintf(
                "INSERT INTO mi_dm_user_stats VALUES (%d, %d, %s, '%s')",
                (int) $s['id'], (int) $s['total_orders'], (float) $s['total_spent'], $s['last_order']
            ));
        }

        $result = $this->mysqli->query("SELECT * FROM mi_dm_user_stats ORDER BY user_id");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame(1, (int) $rows[0]['user_id']);
        $this->assertSame(2, (int) $rows[0]['total_orders']);
        $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total_spent'], 0.01);

        $this->mysqli->query("UPDATE mi_dm_users SET tier = 'platinum' WHERE id IN (SELECT user_id FROM mi_dm_user_stats WHERE total_spent >= 200)");

        $result = $this->mysqli->query("SELECT name, tier FROM mi_dm_users WHERE tier = 'platinum'");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testSubqueryInSelectCase(): void
    {
        $this->mysqli->query("INSERT INTO mi_dm_users VALUES (1, 'Alice', 'alice@co.com', 'gold', '2023-01-15')");
        $this->mysqli->query("INSERT INTO mi_dm_users VALUES (2, 'Bob', 'bob@co.com', 'silver', '2023-06-20')");

        $this->mysqli->query("INSERT INTO mi_dm_orders VALUES (1, 1, 'Widget', 100, 'completed', '2024-01-05')");
        $this->mysqli->query("INSERT INTO mi_dm_orders VALUES (2, 1, 'Gadget', 200, 'completed', '2024-02-10')");

        $result = $this->mysqli->query("
            SELECT u.name,
                   CASE
                       WHEN (SELECT COUNT(*) FROM mi_dm_orders o WHERE o.user_id = u.id) > 1 THEN 'active'
                       WHEN (SELECT COUNT(*) FROM mi_dm_orders o WHERE o.user_id = u.id) = 1 THEN 'one-time'
                       ELSE 'inactive'
                   END AS activity_level
            FROM mi_dm_users u
            ORDER BY u.name
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('active', $rows[0]['activity_level']);
        $this->assertSame('inactive', $rows[1]['activity_level']);
    }

    protected function tearDown(): void
    {
        $this->mysqli->close();
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_dm_user_stats');
        $raw->query('DROP TABLE IF EXISTS mi_dm_orders');
        $raw->query('DROP TABLE IF EXISTS mi_dm_users');
        $raw->close();
    }
}
