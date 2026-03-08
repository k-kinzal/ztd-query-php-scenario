<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;

/**
 * Tests UPDATE with subquery in SET clause on PostgreSQL.
 *
 * On PostgreSQL, ALL subqueries in UPDATE SET clauses fail under ZTD:
 * - Self-referencing subqueries fail with "duplicate alias" because the
 *   CTE references the same table twice.
 * - Cross-table subqueries fail with "grouping error" because the CTE
 *   rewriter generates an invalid SELECT with aggregate + non-aggregate columns.
 *
 * This is a platform-specific limitation not seen on MySQL or SQLite.
 */
class PostgresUpdateWithSubqueryInSetTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(PostgreSQLContainer::getDsn(), 'test', 'test');
        $raw->exec('DROP TABLE IF EXISTS pg_updsub_products');
        $raw->exec('DROP TABLE IF EXISTS pg_updsub_categories');
        $raw->exec('CREATE TABLE pg_updsub_categories (id INT PRIMARY KEY, name VARCHAR(50), avg_price NUMERIC(10,2))');
        $raw->exec('CREATE TABLE pg_updsub_products (id INT PRIMARY KEY, name VARCHAR(50), price NUMERIC(10,2), category_id INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(PostgreSQLContainer::getDsn(), 'test', 'test');

        $this->pdo->exec("INSERT INTO pg_updsub_categories VALUES (1, 'Electronics', 0)");
        $this->pdo->exec("INSERT INTO pg_updsub_products VALUES (1, 'Laptop', 1000.00, 1)");
        $this->pdo->exec("INSERT INTO pg_updsub_products VALUES (2, 'Phone', 500.00, 1)");
    }

    /**
     * Self-referencing subquery fails with duplicate alias.
     */
    public function testUpdateSetSelfReferencingSubqueryFails(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessage('specified more than once');

        $this->pdo->exec('
            UPDATE pg_updsub_products
            SET price = (SELECT MAX(price) FROM pg_updsub_products)
            WHERE id = 2
        ');
    }

    /**
     * Cross-table AVG subquery fails with grouping error.
     */
    public function testUpdateSetCrossTableSubqueryFails(): void
    {
        $this->expectException(ZtdPdoException::class);

        $this->pdo->exec('
            UPDATE pg_updsub_categories
            SET avg_price = (SELECT AVG(price) FROM pg_updsub_products)
            WHERE id = 1
        ');
    }

    /**
     * Simple UPDATE without subquery works.
     */
    public function testSimpleUpdateStillWorks(): void
    {
        $this->pdo->exec('UPDATE pg_updsub_categories SET avg_price = 999 WHERE id = 1');

        $stmt = $this->pdo->query('SELECT avg_price FROM pg_updsub_categories WHERE id = 1');
        $this->assertEqualsWithDelta(999.0, (float) $stmt->fetchColumn(), 0.01);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('UPDATE pg_updsub_categories SET avg_price = 999');

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_updsub_categories');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(PostgreSQLContainer::getDsn(), 'test', 'test');
            $raw->exec('DROP TABLE IF EXISTS pg_updsub_products');
            $raw->exec('DROP TABLE IF EXISTS pg_updsub_categories');
        } catch (\Exception $e) {
        }
    }
}
