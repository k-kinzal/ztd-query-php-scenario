<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT...SELECT with multi-table JOIN and aggregate on MySQL PDO.
 *
 * Finding: INSERT...SELECT with multi-table JOIN fails on MySQL with
 * "Unknown column 'alias.col' in 'field list'" — the InsertTransformer
 * cannot resolve column references from JOINed table aliases.
 * This is distinct from SPEC-11.MYSQL-INSERT-SELECT-STAR (which is about
 * SELECT * column count mismatch) and SPEC-11.INSERT-SELECT-COMPUTED
 * (which is about computed columns being NULL on SQLite/PostgreSQL).
 *
 * @spec SPEC-4.1a
 */
class MysqlInsertSelectJoinAggregateTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_isja_customers (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, region VARCHAR(20) NOT NULL)',
            'CREATE TABLE my_isja_orders (id INT PRIMARY KEY, customer_id INT NOT NULL, amount DECIMAL(10,2) NOT NULL, order_date DATE NOT NULL)',
            'CREATE TABLE my_isja_summary (id INT PRIMARY KEY, customer_id INT, customer_name VARCHAR(50), total_orders INT, total_amount DECIMAL(10,2), region VARCHAR(20))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_isja_summary', 'my_isja_orders', 'my_isja_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_isja_customers VALUES (1, 'Alice', 'East')");
        $this->pdo->exec("INSERT INTO my_isja_customers VALUES (2, 'Bob', 'West')");
        $this->pdo->exec("INSERT INTO my_isja_customers VALUES (3, 'Carol', 'East')");

        $this->pdo->exec("INSERT INTO my_isja_orders VALUES (1, 1, 100.00, '2025-01-10')");
        $this->pdo->exec("INSERT INTO my_isja_orders VALUES (2, 1, 200.00, '2025-01-15')");
        $this->pdo->exec("INSERT INTO my_isja_orders VALUES (3, 2, 150.00, '2025-01-12')");
        $this->pdo->exec("INSERT INTO my_isja_orders VALUES (4, 3, 300.00, '2025-01-20')");
        $this->pdo->exec("INSERT INTO my_isja_orders VALUES (5, 3, 50.00, '2025-01-22')");
    }

    /**
     * INSERT...SELECT with multi-table JOIN fails on MySQL.
     * The InsertTransformer throws "Unknown column 'o.id'" because it
     * cannot resolve column references from JOINed table aliases.
     */
    public function testInsertSelectWithJoinFailsOnMysql(): void
    {
        $this->expectException(\PDOException::class);
        $this->expectExceptionMessageMatches('/Unknown column/');

        $this->pdo->exec(
            "INSERT INTO my_isja_summary (id, customer_id, customer_name, total_orders, total_amount, region)
             SELECT c.id, c.id, c.name, COUNT(o.id), SUM(o.amount), c.region
             FROM my_isja_customers c
             JOIN my_isja_orders o ON o.customer_id = c.id
             GROUP BY c.id, c.name, c.region"
        );
    }

    /**
     * Workaround: INSERT...SELECT from a single table (no JOINs) works.
     */
    public function testInsertSelectSingleTableWorks(): void
    {
        $this->pdo->exec(
            "INSERT INTO my_isja_summary (id, customer_id, customer_name, total_orders, total_amount, region)
             SELECT id, id, name, 0, 0.00, region
             FROM my_isja_customers"
        );

        $rows = $this->ztdQuery("SELECT * FROM my_isja_summary ORDER BY id");
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['customer_name']);
        $this->assertSame('East', $rows[0]['region']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec(
            "INSERT INTO my_isja_summary (id, customer_id, customer_name, total_orders, total_amount, region)
             SELECT id, id, name, 0, 0.00, region
             FROM my_isja_customers"
        );

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM my_isja_summary")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
