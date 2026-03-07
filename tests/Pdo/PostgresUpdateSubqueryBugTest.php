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
 * Tests UPDATE with IN (subquery containing GROUP BY HAVING) on PostgreSQL.
 * This tests whether the CTE rewriter issue found on SQLite also affects PostgreSQL.
 *
 * Result: PostgreSQL is also affected, but with a different error.
 * SQLite produces "incomplete input", PostgreSQL produces "ambiguous column"
 * because the CTE rewriter generates a cross join between the tables.
 * MySQL handles this pattern correctly.
 */
class PostgresUpdateSubqueryBugTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pg_subq_orders');
        $raw->exec('DROP TABLE IF EXISTS pg_subq_users');
        $raw->exec('CREATE TABLE pg_subq_users (id INT PRIMARY KEY, name VARCHAR(255), tier VARCHAR(50))');
        $raw->exec('CREATE TABLE pg_subq_orders (id INT PRIMARY KEY, user_id INT, total DECIMAL(10,2), status VARCHAR(50))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pg_subq_users (id, name, tier) VALUES (1, 'Alice', 'standard')");
        $this->pdo->exec("INSERT INTO pg_subq_users (id, name, tier) VALUES (2, 'Bob', 'standard')");
        $this->pdo->exec("INSERT INTO pg_subq_orders (id, user_id, total, status) VALUES (1, 1, 500, 'completed')");
        $this->pdo->exec("INSERT INTO pg_subq_orders (id, user_id, total, status) VALUES (2, 1, 300, 'completed')");
        $this->pdo->exec("INSERT INTO pg_subq_orders (id, user_id, total, status) VALUES (3, 2, 100, 'completed')");
    }

    public function testUpdateWithInSubqueryGroupByHavingFails(): void
    {
        // PostgreSQL also fails: CTE rewriter generates a cross join making "id" ambiguous
        // Error: "column reference 'id' is ambiguous" (different from SQLite's "incomplete input")
        $this->expectException(\Throwable::class);
        $this->pdo->exec("UPDATE pg_subq_users SET tier = 'premium' WHERE id IN (SELECT user_id FROM pg_subq_orders WHERE status = 'completed' GROUP BY user_id HAVING SUM(total) > 400)");
    }

    public function testSelectWithGroupByHavingWorks(): void
    {
        $stmt = $this->pdo->query("SELECT user_id FROM pg_subq_orders WHERE status = 'completed' GROUP BY user_id HAVING SUM(total) > 400");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['user_id']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_subq_orders');
        $raw->exec('DROP TABLE IF EXISTS pg_subq_users');
    }
}
