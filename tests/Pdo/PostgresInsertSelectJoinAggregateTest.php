<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT...SELECT with multi-table JOIN and aggregate on PostgreSQL.
 *
 * Finding: INSERT...SELECT with JOIN and GROUP BY inserts rows but
 * non-PK columns become NULL. Extends SPEC-11.INSERT-SELECT-COMPUTED.
 *
 * @spec SPEC-4.1a
 */
class PostgresInsertSelectJoinAggregateTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_isja_customers (id SERIAL PRIMARY KEY, name TEXT NOT NULL, region TEXT NOT NULL)',
            'CREATE TABLE pg_isja_orders (id SERIAL PRIMARY KEY, customer_id INT NOT NULL, amount NUMERIC(10,2) NOT NULL, order_date DATE NOT NULL)',
            'CREATE TABLE pg_isja_summary (id INT PRIMARY KEY, customer_id INT, customer_name TEXT, total_orders INT, total_amount NUMERIC(10,2), region TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_isja_summary', 'pg_isja_orders', 'pg_isja_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_isja_customers VALUES (1, 'Alice', 'East')");
        $this->pdo->exec("INSERT INTO pg_isja_customers VALUES (2, 'Bob', 'West')");
        $this->pdo->exec("INSERT INTO pg_isja_customers VALUES (3, 'Carol', 'East')");

        $this->pdo->exec("INSERT INTO pg_isja_orders VALUES (1, 1, 100.00, '2025-01-10')");
        $this->pdo->exec("INSERT INTO pg_isja_orders VALUES (2, 1, 200.00, '2025-01-15')");
        $this->pdo->exec("INSERT INTO pg_isja_orders VALUES (3, 2, 150.00, '2025-01-12')");
        $this->pdo->exec("INSERT INTO pg_isja_orders VALUES (4, 3, 300.00, '2025-01-20')");
        $this->pdo->exec("INSERT INTO pg_isja_orders VALUES (5, 3, 50.00, '2025-01-22')");
    }

    /**
     * INSERT...SELECT with JOIN and GROUP BY — rows inserted but
     * non-PK columns become NULL on PostgreSQL (extends SPEC-11.INSERT-SELECT-COMPUTED).
     */
    public function testInsertSelectWithJoinNullColumns(): void
    {
        $this->pdo->exec(
            "INSERT INTO pg_isja_summary (id, customer_id, customer_name, total_orders, total_amount, region)
             SELECT c.id, c.id, c.name, COUNT(o.id)::int, SUM(o.amount), c.region
             FROM pg_isja_customers c
             JOIN pg_isja_orders o ON o.customer_id = c.id
             GROUP BY c.id, c.name, c.region"
        );

        $rows = $this->ztdQuery("SELECT * FROM pg_isja_summary ORDER BY id");

        $this->assertCount(3, $rows);
        $this->assertNull($rows[0]['customer_name'], 'Expected NULL — extends SPEC-11.INSERT-SELECT-COMPUTED to JOIN sources');
        $this->assertNull($rows[0]['total_orders'], 'COUNT aggregate is NULL');
        $this->assertNull($rows[0]['total_amount'], 'SUM aggregate is NULL');
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec(
            "INSERT INTO pg_isja_summary (id, customer_id, customer_name, total_orders, total_amount, region)
             SELECT c.id, c.id, c.name, COUNT(o.id)::int, SUM(o.amount), c.region
             FROM pg_isja_customers c
             JOIN pg_isja_orders o ON o.customer_id = c.id
             GROUP BY c.id, c.name, c.region"
        );

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) FROM pg_isja_summary")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['count']);
    }
}
