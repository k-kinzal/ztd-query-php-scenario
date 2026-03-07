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
 * Tests user-written CTE queries and INSERT ... SELECT on PostgreSQL.
 */
class PostgresUserCteTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pg_cte_backup');
        $raw->exec('DROP TABLE IF EXISTS pg_cte_products');
        $raw->exec('CREATE TABLE pg_cte_products (id INT PRIMARY KEY, name VARCHAR(255), category VARCHAR(255), price DECIMAL(10,2))');
        $raw->exec('CREATE TABLE pg_cte_backup (id INT PRIMARY KEY, name VARCHAR(255), category VARCHAR(255), price DECIMAL(10,2))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pg_cte_products (id, name, category, price) VALUES (1, 'Widget A', 'gadgets', 10.00)");
        $this->pdo->exec("INSERT INTO pg_cte_products (id, name, category, price) VALUES (2, 'Widget B', 'gadgets', 20.00)");
        $this->pdo->exec("INSERT INTO pg_cte_products (id, name, category, price) VALUES (3, 'Gizmo X', 'tools', 30.00)");
    }

    public function testUserCteSelectReturnsEmpty(): void
    {
        // User-written CTEs on PostgreSQL: the ZTD CTE rewriter does not
        // rewrite table references inside user CTEs. The inner SELECT
        // reads from the physical table (which is empty), so the result is empty.
        // This works correctly on SQLite (returns shadow data).
        $stmt = $this->pdo->query("
            WITH expensive AS (
                SELECT * FROM pg_cte_products WHERE price > 15
            )
            SELECT name, price FROM expensive ORDER BY price
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // Returns 0 rows because the inner CTE reads from physical table (empty)
        // rather than shadow store. On SQLite this returns 2 rows.
        $this->assertCount(0, $rows);
    }

    public function testInsertSelectExplicitColumns(): void
    {
        $this->pdo->exec("INSERT INTO pg_cte_backup (id, name, category, price) SELECT id, name, category, price FROM pg_cte_products");

        $stmt = $this->pdo->query('SELECT * FROM pg_cte_backup ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Widget A', $rows[0]['name']);
    }

    public function testInsertSelectIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_cte_backup (id, name, category, price) SELECT id, name, category, price FROM pg_cte_products");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM pg_cte_backup');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_cte_backup');
        $raw->exec('DROP TABLE IF EXISTS pg_cte_products');
    }
}
