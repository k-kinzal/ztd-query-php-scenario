<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL IF() function in queries on shadow data.
 *
 * The MySQL IF(condition, true_val, false_val) function is widely used
 * as a shorthand for CASE WHEN. Since IF is also a SQL keyword (used in
 * stored procedures and conditional DDL), the CTE rewriter's SQL parser
 * must correctly identify IF() as a function call, not a control flow
 * statement.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class MysqlIfFunctionTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_iff_products (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                stock INT NOT NULL,
                active TINYINT NOT NULL DEFAULT 1
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_iff_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_iff_products VALUES (1, 'Widget', 29.99, 100, 1)");
        $this->ztdExec("INSERT INTO my_iff_products VALUES (2, 'Gadget', 149.99, 0, 1)");
        $this->ztdExec("INSERT INTO my_iff_products VALUES (3, 'Bolt', 2.99, 500, 0)");
        $this->ztdExec("INSERT INTO my_iff_products VALUES (4, 'Screw', 0.99, 1000, 1)");
    }

    /**
     * IF() function in SELECT list.
     */
    public function testIfFunctionInSelect(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name,
                    IF(stock > 0, 'In Stock', 'Out of Stock') AS availability,
                    IF(active = 1, 'Yes', 'No') AS is_active
             FROM my_iff_products
             ORDER BY name"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Out of Stock', $rows[1]['availability']); // Gadget: stock=0
        $this->assertSame('No', $rows[0]['is_active']); // Bolt: active=0
    }

    /**
     * IF() function in WHERE clause.
     */
    public function testIfFunctionInWhere(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM my_iff_products
             WHERE IF(active = 1 AND stock > 0, 1, 0) = 1
             ORDER BY name"
        );

        // Only Widget (active, stock 100) and Screw (active, stock 1000) qualify
        $this->assertCount(2, $rows);
        $this->assertSame('Screw', $rows[0]['name']);
        $this->assertSame('Widget', $rows[1]['name']);
    }

    /**
     * IF() function in UPDATE SET clause.
     */
    public function testIfFunctionInUpdateSet(): void
    {
        $this->ztdExec(
            "UPDATE my_iff_products SET price = IF(stock > 100, price * 0.90, price * 1.10)
             WHERE active = 1"
        );

        $rows = $this->ztdQuery("SELECT name, price FROM my_iff_products ORDER BY name");

        // Gadget: active, stock 0 → price * 1.10 = 164.989
        $this->assertEqualsWithDelta(164.99, (float) $rows[1]['price'], 0.01);
        // Screw: active, stock 1000 → price * 0.90 = 0.891
        $this->assertEqualsWithDelta(0.89, (float) $rows[2]['price'], 0.01);
        // Bolt: inactive, unchanged
        $this->assertEqualsWithDelta(2.99, (float) $rows[0]['price'], 0.01);
    }

    /**
     * Nested IF() functions.
     */
    public function testNestedIfFunction(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name,
                    IF(active = 1,
                       IF(stock > 50, 'plenty', 'low'),
                       'inactive') AS status
             FROM my_iff_products
             ORDER BY name"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('inactive', $rows[0]['status']); // Bolt: inactive
        $this->assertSame('low', $rows[1]['status']);       // Gadget: active, stock 0
        $this->assertSame('plenty', $rows[2]['status']);    // Screw: active, stock 1000
        $this->assertSame('plenty', $rows[3]['status']);    // Widget: active, stock 100
    }

    /**
     * IF() with prepared params.
     */
    public function testIfFunctionWithPreparedParams(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name, IF(price > ?, 'expensive', 'cheap') AS category
             FROM my_iff_products
             WHERE active = ?
             ORDER BY name",
            [10.00, 1]
        );

        $this->assertCount(3, $rows);
        $this->assertSame('expensive', $rows[0]['category']); // Gadget: 149.99
        $this->assertSame('cheap', $rows[1]['category']);      // Screw: 0.99
        $this->assertSame('expensive', $rows[2]['category']);  // Widget: 29.99
    }

    /**
     * IF() combined with SUM for conditional aggregation.
     */
    public function testIfFunctionWithSumAggregation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                SUM(IF(active = 1, 1, 0)) AS active_count,
                SUM(IF(stock = 0, 1, 0)) AS out_of_stock_count,
                SUM(IF(active = 1 AND stock > 0, price * stock, 0)) AS inventory_value
             FROM my_iff_products"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(3, (int) $rows[0]['active_count']);
        $this->assertEquals(1, (int) $rows[0]['out_of_stock_count']);
        // Widget: 29.99*100=2999, Screw: 0.99*1000=990
        $this->assertEqualsWithDelta(3989.00, (float) $rows[0]['inventory_value'], 0.01);
    }

    /**
     * IFNULL() function (MySQL-specific).
     */
    public function testIfNullFunction(): void
    {
        // Insert a row with NULL-like behavior via direct exec
        $this->ztdExec("INSERT INTO my_iff_products VALUES (5, 'NullTest', 0.00, 0, 1)");

        $rows = $this->ztdQuery(
            "SELECT name, IFNULL(NULLIF(price, 0), 9.99) AS effective_price
             FROM my_iff_products
             WHERE id = 5"
        );

        $this->assertCount(1, $rows);
        // NULLIF(0, 0) = NULL → IFNULL(NULL, 9.99) = 9.99
        $this->assertEqualsWithDelta(9.99, (float) $rows[0]['effective_price'], 0.01);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM my_iff_products')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
