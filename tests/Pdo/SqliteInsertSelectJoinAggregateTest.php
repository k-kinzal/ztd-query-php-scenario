<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT...SELECT with multi-table JOIN and aggregate — a common
 * data migration/materialization pattern.
 *
 * The CTE rewriter must handle INSERT target + SELECT source tables
 * with JOINs and GROUP BY correctly. Related: SPEC-11.INSERT-SELECT-COMPUTED
 * (computed columns in INSERT...SELECT may become NULL on SQLite/PostgreSQL).
 *
 * Finding: INSERT...SELECT with JOIN and GROUP BY inserts rows but
 * non-PK columns from JOINed tables and aggregates become NULL on SQLite.
 * This extends SPEC-11.INSERT-SELECT-COMPUTED to JOINed multi-table sources.
 *
 * @spec SPEC-4.1a
 */
class SqliteInsertSelectJoinAggregateTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_isja_customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, region TEXT NOT NULL)',
            'CREATE TABLE sl_isja_orders (id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL, amount REAL NOT NULL, order_date TEXT NOT NULL)',
            'CREATE TABLE sl_isja_summary (id INTEGER PRIMARY KEY, customer_id INTEGER, customer_name TEXT, total_orders INTEGER, total_amount REAL, region TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_isja_summary', 'sl_isja_orders', 'sl_isja_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_isja_customers VALUES (1, 'Alice', 'East')");
        $this->pdo->exec("INSERT INTO sl_isja_customers VALUES (2, 'Bob', 'West')");
        $this->pdo->exec("INSERT INTO sl_isja_customers VALUES (3, 'Carol', 'East')");

        $this->pdo->exec("INSERT INTO sl_isja_orders VALUES (1, 1, 100.00, '2025-01-10')");
        $this->pdo->exec("INSERT INTO sl_isja_orders VALUES (2, 1, 200.00, '2025-01-15')");
        $this->pdo->exec("INSERT INTO sl_isja_orders VALUES (3, 2, 150.00, '2025-01-12')");
        $this->pdo->exec("INSERT INTO sl_isja_orders VALUES (4, 3, 300.00, '2025-01-20')");
        $this->pdo->exec("INSERT INTO sl_isja_orders VALUES (5, 3, 50.00, '2025-01-22')");
    }

    /**
     * INSERT...SELECT with JOIN and GROUP BY — rows are inserted but
     * aggregate columns (COUNT, SUM) and some source columns become NULL.
     * Extends SPEC-11.INSERT-SELECT-COMPUTED to multi-table JOIN sources.
     *
     * Observed: c.name → NULL, COUNT(o.id) → NULL, SUM(o.amount) → NULL,
     * but c.region preserves its value. The InsertTransformer appears to
     * partially resolve column values from the source query.
     */
    public function testInsertSelectWithJoinAndGroupByPartialNulls(): void
    {
        $this->pdo->exec(
            "INSERT INTO sl_isja_summary (id, customer_id, customer_name, total_orders, total_amount, region)
             SELECT c.id, c.id, c.name, COUNT(o.id), SUM(o.amount), c.region
             FROM sl_isja_customers c
             JOIN sl_isja_orders o ON o.customer_id = c.id
             GROUP BY c.id, c.name, c.region"
        );

        $rows = $this->ztdQuery("SELECT * FROM sl_isja_summary ORDER BY id");

        // 3 rows are inserted
        $this->assertCount(3, $rows);

        // Aggregate columns are NULL (extends SPEC-11.INSERT-SELECT-COMPUTED)
        $this->assertNull($rows[0]['customer_name'], 'c.name becomes NULL');
        $this->assertNull($rows[0]['total_orders'], 'COUNT aggregate is NULL');
        $this->assertNull($rows[0]['total_amount'], 'SUM aggregate is NULL');

        // Some source columns may preserve values
        $this->assertSame('East', $rows[0]['region']);
    }

    /**
     * Physical isolation: summary table is empty in physical DB.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec(
            "INSERT INTO sl_isja_summary (id, customer_id, customer_name, total_orders, total_amount, region)
             SELECT c.id, c.id, c.name, COUNT(o.id), SUM(o.amount), c.region
             FROM sl_isja_customers c
             JOIN sl_isja_orders o ON o.customer_id = c.id
             GROUP BY c.id, c.name, c.region"
        );

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_isja_summary")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
