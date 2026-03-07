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
 * Tests a data migration workflow on PostgreSQL PDO: INSERT...SELECT with transformations,
 * subqueries in SELECT/WHERE, and complex aggregation patterns.
 */
class PostgresDataMigrationWorkflowTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_dm_user_stats');
        $raw->exec('DROP TABLE IF EXISTS pg_dm_orders');
        $raw->exec('DROP TABLE IF EXISTS pg_dm_users');
        $raw->exec('CREATE TABLE pg_dm_users (id INT PRIMARY KEY, name VARCHAR(50), email VARCHAR(100), tier VARCHAR(20), created_date DATE)');
        $raw->exec('CREATE TABLE pg_dm_orders (id INT PRIMARY KEY, user_id INT, product VARCHAR(50), amount NUMERIC(10,2), status VARCHAR(20), created_date DATE)');
        $raw->exec('CREATE TABLE pg_dm_user_stats (user_id INT PRIMARY KEY, total_orders INT, total_spent NUMERIC(10,2), last_order_date DATE)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    /**
     * INSERT...SELECT with LEFT JOIN + GROUP BY + aggregation inserts rows
     * but all column values are NULL on PostgreSQL too.
     */
    public function testInsertSelectWithJoinGroupByProducesNullValues(): void
    {
        $this->pdo->exec("INSERT INTO pg_dm_users VALUES (1, 'Alice', 'alice@co.com', 'gold', '2023-01-15')");
        $this->pdo->exec("INSERT INTO pg_dm_users VALUES (2, 'Bob', 'bob@co.com', 'silver', '2023-06-20')");

        $this->pdo->exec("INSERT INTO pg_dm_orders VALUES (1, 1, 'Widget', 100, 'completed', '2024-01-05')");
        $this->pdo->exec("INSERT INTO pg_dm_orders VALUES (2, 1, 'Gadget', 200, 'completed', '2024-02-10')");

        $affected = $this->pdo->exec("
            INSERT INTO pg_dm_user_stats (user_id, total_orders, total_spent, last_order_date)
            SELECT u.id, COUNT(o.id), COALESCE(SUM(o.amount), 0), MAX(o.created_date)
            FROM pg_dm_users u
            LEFT JOIN pg_dm_orders o ON o.user_id = u.id
            GROUP BY u.id
        ");
        $this->assertSame(2, $affected);

        $stmt = $this->pdo->query("SELECT * FROM pg_dm_user_stats ORDER BY user_id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertNull($rows[0]['user_id']);
        $this->assertNull($rows[0]['total_orders']);
    }

    public function testMigrationWorkflowWithManualInserts(): void
    {
        $this->pdo->exec("INSERT INTO pg_dm_users VALUES (1, 'Alice', 'alice@co.com', 'gold', '2023-01-15')");
        $this->pdo->exec("INSERT INTO pg_dm_users VALUES (2, 'Bob', 'bob@co.com', 'silver', '2023-06-20')");
        $this->pdo->exec("INSERT INTO pg_dm_orders VALUES (1, 1, 'Widget', 100, 'completed', '2024-01-05')");
        $this->pdo->exec("INSERT INTO pg_dm_orders VALUES (2, 1, 'Gadget', 200, 'completed', '2024-02-10')");
        $this->pdo->exec("INSERT INTO pg_dm_orders VALUES (3, 2, 'Widget', 75, 'completed', '2024-01-15')");

        $stmt = $this->pdo->query("
            SELECT u.id, COUNT(o.id) AS total_orders, SUM(o.amount) AS total_spent, MAX(o.created_date) AS last_order
            FROM pg_dm_users u LEFT JOIN pg_dm_orders o ON o.user_id = u.id
            GROUP BY u.id ORDER BY u.id
        ");
        $stats = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $stats);

        foreach ($stats as $s) {
            $this->pdo->exec(sprintf(
                "INSERT INTO pg_dm_user_stats VALUES (%d, %d, %s, '%s')",
                (int) $s['id'], (int) $s['total_orders'], (float) $s['total_spent'], $s['last_order']
            ));
        }

        $check = $this->pdo->query("SELECT * FROM pg_dm_user_stats ORDER BY user_id");
        $rows = $check->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame(1, (int) $rows[0]['user_id']);
        $this->assertSame(2, (int) $rows[0]['total_orders']);
        $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total_spent'], 0.01);

        $this->pdo->exec("UPDATE pg_dm_users SET tier = 'platinum' WHERE id IN (SELECT user_id FROM pg_dm_user_stats WHERE total_spent >= 200)");

        $stmt = $this->pdo->query("SELECT name, tier FROM pg_dm_users WHERE tier = 'platinum'");
        $platinum = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $platinum);
        $this->assertSame('Alice', $platinum[0]['name']);
    }

    public function testSubqueryInSelectCase(): void
    {
        $this->pdo->exec("INSERT INTO pg_dm_users VALUES (1, 'Alice', 'alice@co.com', 'gold', '2023-01-15')");
        $this->pdo->exec("INSERT INTO pg_dm_users VALUES (2, 'Bob', 'bob@co.com', 'silver', '2023-06-20')");

        $this->pdo->exec("INSERT INTO pg_dm_orders VALUES (1, 1, 'Widget', 100, 'completed', '2024-01-05')");
        $this->pdo->exec("INSERT INTO pg_dm_orders VALUES (2, 1, 'Gadget', 200, 'completed', '2024-02-10')");

        $stmt = $this->pdo->query("
            SELECT u.name,
                   CASE
                       WHEN (SELECT COUNT(*) FROM pg_dm_orders o WHERE o.user_id = u.id) > 1 THEN 'active'
                       WHEN (SELECT COUNT(*) FROM pg_dm_orders o WHERE o.user_id = u.id) = 1 THEN 'one-time'
                       ELSE 'inactive'
                   END AS activity_level
            FROM pg_dm_users u
            ORDER BY u.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('active', $rows[0]['activity_level']);
        $this->assertSame('inactive', $rows[1]['activity_level']);
    }

    public function testComplexWhereWithMultipleSubqueries(): void
    {
        $this->pdo->exec("INSERT INTO pg_dm_users VALUES (1, 'Alice', 'alice@co.com', 'gold', '2023-01-15')");
        $this->pdo->exec("INSERT INTO pg_dm_users VALUES (2, 'Bob', 'bob@co.com', 'silver', '2023-06-20')");
        $this->pdo->exec("INSERT INTO pg_dm_users VALUES (3, 'Charlie', 'charlie@co.com', 'bronze', '2024-01-01')");

        $this->pdo->exec("INSERT INTO pg_dm_orders VALUES (1, 1, 'Widget', 100, 'completed', '2024-01-05')");
        $this->pdo->exec("INSERT INTO pg_dm_orders VALUES (2, 2, 'Gadget', 200, 'completed', '2024-02-10')");
        $this->pdo->exec("INSERT INTO pg_dm_orders VALUES (3, 3, 'Widget', 50, 'completed', '2024-03-01')");

        $stmt = $this->pdo->query("
            SELECT u.name
            FROM pg_dm_users u
            WHERE u.id IN (
                SELECT o.user_id FROM pg_dm_orders o
                WHERE o.status = 'completed'
                GROUP BY o.user_id
                HAVING SUM(o.amount) > (SELECT AVG(amount) FROM pg_dm_orders WHERE status = 'completed')
            )
            AND u.created_date < '2024-01-01'
            ORDER BY u.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_dm_user_stats');
        $raw->exec('DROP TABLE IF EXISTS pg_dm_orders');
        $raw->exec('DROP TABLE IF EXISTS pg_dm_users');
    }
}
