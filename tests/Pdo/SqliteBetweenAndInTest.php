<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests BETWEEN, IN with large lists, and complex boolean logic in WHERE
 * through the CTE shadow store.
 *
 * These patterns stress the SQL parser's handling of AND keyword disambiguation
 * (BETWEEN ... AND vs boolean AND), large IN-list parenthesized expressions,
 * and deeply nested boolean combinations.
 */
class SqliteBetweenAndInTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_bi_orders (
            id INTEGER PRIMARY KEY,
            customer TEXT NOT NULL,
            amount REAL NOT NULL,
            order_date TEXT NOT NULL,
            status TEXT NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_bi_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_bi_orders VALUES (1, 'Alice',   50.00,  '2025-01-05', 'shipped')");
        $this->pdo->exec("INSERT INTO sl_bi_orders VALUES (2, 'Bob',     150.00, '2025-02-14', 'pending')");
        $this->pdo->exec("INSERT INTO sl_bi_orders VALUES (3, 'Carol',   300.00, '2025-03-20', 'shipped')");
        $this->pdo->exec("INSERT INTO sl_bi_orders VALUES (4, 'Alice',   75.00,  '2025-04-10', 'cancelled')");
        $this->pdo->exec("INSERT INTO sl_bi_orders VALUES (5, 'Dave',    500.00, '2025-05-01', 'shipped')");
        $this->pdo->exec("INSERT INTO sl_bi_orders VALUES (6, 'Eve',     25.00,  '2025-06-15', 'pending')");
        $this->pdo->exec("INSERT INTO sl_bi_orders VALUES (7, 'Bob',     1000.00,'2025-07-22', 'shipped')");
        $this->pdo->exec("INSERT INTO sl_bi_orders VALUES (8, 'Carol',   200.00, '2025-08-30', 'returned')");
    }

    /**
     * BETWEEN with a numeric range.
     * The CTE rewriter must distinguish the AND inside BETWEEN from boolean AND.
     */
    public function testBetweenNumericRange(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, customer, amount
             FROM sl_bi_orders
             WHERE amount BETWEEN 100 AND 500
             ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertEquals(2, (int) $rows[0]['id']);  // Bob 150
        $this->assertEquals(3, (int) $rows[1]['id']);  // Carol 300
        $this->assertEquals(5, (int) $rows[2]['id']);  // Dave 500
        $this->assertEquals(8, (int) $rows[3]['id']);  // Carol 200
    }

    /**
     * BETWEEN with date string comparisons.
     * Tests string-based date range in WHERE clause.
     */
    public function testBetweenDateStrings(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, customer, order_date
             FROM sl_bi_orders
             WHERE order_date BETWEEN '2025-03-01' AND '2025-06-30'
             ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertEquals(3, (int) $rows[0]['id']);  // 2025-03-20
        $this->assertEquals(4, (int) $rows[1]['id']);  // 2025-04-10
        $this->assertEquals(5, (int) $rows[2]['id']);  // 2025-05-01
        $this->assertEquals(6, (int) $rows[3]['id']);  // 2025-06-15
    }

    /**
     * IN with a moderate-sized list (12 values).
     * A large parenthesized value list stresses the parser's token handling.
     */
    public function testInWithLargeList(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, customer
             FROM sl_bi_orders
             WHERE customer IN (
                 'Alice', 'Bob', 'Carol', 'Dave', 'Eve',
                 'Frank', 'Grace', 'Heidi', 'Ivan', 'Judy',
                 'Karl', 'Liam'
             )
             ORDER BY id"
        );

        // All 8 orders match because all customers are in the list
        $this->assertCount(8, $rows);
    }

    /**
     * NOT BETWEEN combined with IN.
     * Tests the parser handling NOT keyword before BETWEEN alongside IN.
     */
    public function testNotBetweenCombinedWithIn(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, customer, amount, status
             FROM sl_bi_orders
             WHERE amount NOT BETWEEN 100 AND 500
               AND status IN ('shipped', 'pending')
             ORDER BY id"
        );

        // amount NOT BETWEEN 100 AND 500 means amount < 100 OR amount > 500
        // id=1: 50  shipped    -> yes (50 < 100, shipped)
        // id=6: 25  pending    -> yes (25 < 100, pending)
        // id=7: 1000 shipped   -> yes (1000 > 500, shipped)
        $this->assertCount(3, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertEquals(6, (int) $rows[1]['id']);
        $this->assertEquals(7, (int) $rows[2]['id']);
    }

    /**
     * Complex nested boolean: (A AND B) OR (C AND NOT D).
     * Tests deeply nested parenthesized boolean expressions.
     */
    public function testComplexNestedBoolean(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, customer, amount, status
             FROM sl_bi_orders
             WHERE (amount > 200 AND status = 'shipped')
                OR (customer = 'Alice' AND NOT status = 'cancelled')
             ORDER BY id"
        );

        // (amount > 200 AND status = 'shipped'):
        //   id=3: 300 shipped -> yes
        //   id=5: 500 shipped -> yes
        //   id=7: 1000 shipped -> yes
        // (customer = 'Alice' AND NOT status = 'cancelled'):
        //   id=1: Alice shipped -> yes
        //   id=4: Alice cancelled -> no
        $this->assertCount(4, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);  // Alice shipped
        $this->assertEquals(3, (int) $rows[1]['id']);  // Carol 300 shipped
        $this->assertEquals(5, (int) $rows[2]['id']);  // Dave 500 shipped
        $this->assertEquals(7, (int) $rows[3]['id']);  // Bob 1000 shipped
    }

    /**
     * Prepared statement with BETWEEN parameters.
     * Tests that parameter binding works correctly with BETWEEN ... AND.
     */
    public function testPreparedBetweenParameters(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, customer, amount
             FROM sl_bi_orders
             WHERE amount BETWEEN ? AND ?
             ORDER BY id",
            [100, 500]
        );

        $this->assertCount(4, $rows);
        $this->assertEquals(2, (int) $rows[0]['id']);  // Bob 150
        $this->assertEquals(3, (int) $rows[1]['id']);  // Carol 300
        $this->assertEquals(5, (int) $rows[2]['id']);  // Dave 500
        $this->assertEquals(8, (int) $rows[3]['id']);  // Carol 200
    }

    /**
     * Physical isolation: the underlying table must be empty.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_bi_orders")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
