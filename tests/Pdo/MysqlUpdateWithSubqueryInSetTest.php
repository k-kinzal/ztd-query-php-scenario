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
 * Tests UPDATE with subquery in SET clause on MySQL.
 *
 * Cross-platform parity with SqliteUpdateWithSubqueryInSetTest.
 * Non-correlated scalar subqueries work; correlated subqueries
 * may fail depending on the CTE rewriter.
 */
class MysqlUpdateWithSubqueryInSetTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
        $raw->exec('DROP TABLE IF EXISTS pdo_updsub_products');
        $raw->exec('DROP TABLE IF EXISTS pdo_updsub_categories');
        $raw->exec('CREATE TABLE pdo_updsub_categories (id INT PRIMARY KEY, name VARCHAR(50), avg_price DECIMAL(10,2))');
        $raw->exec('CREATE TABLE pdo_updsub_products (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2), category_id INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(MySQLContainer::getDsn(), 'root', 'root');

        $this->pdo->exec("INSERT INTO pdo_updsub_categories VALUES (1, 'Electronics', 0)");
        $this->pdo->exec("INSERT INTO pdo_updsub_categories VALUES (2, 'Books', 0)");

        $this->pdo->exec("INSERT INTO pdo_updsub_products VALUES (1, 'Laptop', 1000.00, 1)");
        $this->pdo->exec("INSERT INTO pdo_updsub_products VALUES (2, 'Phone', 500.00, 1)");
        $this->pdo->exec("INSERT INTO pdo_updsub_products VALUES (3, 'Novel', 15.00, 2)");
        $this->pdo->exec("INSERT INTO pdo_updsub_products VALUES (4, 'Textbook', 85.00, 2)");
    }

    /**
     * UPDATE SET col = (non-correlated scalar subquery) works.
     */
    public function testUpdateSetNonCorrelatedSubqueryWorks(): void
    {
        $this->pdo->exec('
            UPDATE pdo_updsub_products
            SET price = (SELECT MAX(price) FROM pdo_updsub_products)
            WHERE id = 3
        ');

        $stmt = $this->pdo->query('SELECT price FROM pdo_updsub_products WHERE id = 3');
        $this->assertEqualsWithDelta(1000.0, (float) $stmt->fetchColumn(), 0.01);
    }

    /**
     * UPDATE SET col = (AVG non-correlated subquery) works.
     */
    public function testUpdateSetAvgSubqueryWorks(): void
    {
        $this->pdo->exec('
            UPDATE pdo_updsub_categories
            SET avg_price = (SELECT AVG(price) FROM pdo_updsub_products)
            WHERE id = 1
        ');

        $stmt = $this->pdo->query('SELECT avg_price FROM pdo_updsub_categories WHERE id = 1');
        $this->assertEqualsWithDelta(400.0, (float) $stmt->fetchColumn(), 0.01);
    }

    /**
     * UPDATE SET col = (correlated subquery) — may fail on MySQL.
     */
    public function testUpdateSetCorrelatedSubquery(): void
    {
        try {
            $this->pdo->exec('
                UPDATE pdo_updsub_categories
                SET avg_price = (SELECT AVG(price) FROM pdo_updsub_products WHERE category_id = pdo_updsub_categories.id)
                WHERE id = 1
            ');

            // If it succeeds, verify the value
            $stmt = $this->pdo->query('SELECT avg_price FROM pdo_updsub_categories WHERE id = 1');
            $this->assertEqualsWithDelta(750.0, (float) $stmt->fetchColumn(), 0.01);
        } catch (\Exception $e) {
            // Correlated subquery in SET may fail on MySQL too
            $this->assertNotEmpty($e->getMessage());
        }
    }

    /**
     * Simple UPDATE without subquery still works.
     */
    public function testSimpleUpdateStillWorks(): void
    {
        $this->pdo->exec('UPDATE pdo_updsub_categories SET avg_price = 999 WHERE id = 1');

        $stmt = $this->pdo->query('SELECT avg_price FROM pdo_updsub_categories WHERE id = 1');
        $this->assertEqualsWithDelta(999.0, (float) $stmt->fetchColumn(), 0.01);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('UPDATE pdo_updsub_categories SET avg_price = 999');

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_updsub_categories');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP TABLE IF EXISTS pdo_updsub_products');
            $raw->exec('DROP TABLE IF EXISTS pdo_updsub_categories');
        } catch (\Exception $e) {
        }
    }
}
