<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests BETWEEN, IN with large lists, and complex boolean logic in WHERE
 * through the PostgreSQL CTE shadow store.
 *
 * These patterns stress the SQL parser's handling of AND keyword disambiguation
 * (BETWEEN ... AND vs boolean AND), large IN-list parenthesized expressions,
 * and deeply nested boolean combinations.
 */
class PostgresBetweenAndInTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_bi_orders (
            id SERIAL PRIMARY KEY,
            customer TEXT NOT NULL,
            amount NUMERIC(10,2) NOT NULL,
            order_date DATE NOT NULL,
            status TEXT NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_bi_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_bi_orders VALUES (1, 'Alice',   50.00,  '2025-01-05', 'shipped')");
        $this->pdo->exec("INSERT INTO pg_bi_orders VALUES (2, 'Bob',     150.00, '2025-02-14', 'pending')");
        $this->pdo->exec("INSERT INTO pg_bi_orders VALUES (3, 'Carol',   300.00, '2025-03-20', 'shipped')");
        $this->pdo->exec("INSERT INTO pg_bi_orders VALUES (4, 'Alice',   75.00,  '2025-04-10', 'cancelled')");
        $this->pdo->exec("INSERT INTO pg_bi_orders VALUES (5, 'Dave',    500.00, '2025-05-01', 'shipped')");
        $this->pdo->exec("INSERT INTO pg_bi_orders VALUES (6, 'Eve',     25.00,  '2025-06-15', 'pending')");
        $this->pdo->exec("INSERT INTO pg_bi_orders VALUES (7, 'Bob',     1000.00,'2025-07-22', 'shipped')");
        $this->pdo->exec("INSERT INTO pg_bi_orders VALUES (8, 'Carol',   200.00, '2025-08-30', 'returned')");
    }

    /**
     * BETWEEN with a numeric range.
     */
    public function testBetweenNumericRange(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, customer, amount
             FROM pg_bi_orders
             WHERE amount BETWEEN 100 AND 500
             ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertEquals(2, (int) $rows[0]['id']);
        $this->assertEquals(3, (int) $rows[1]['id']);
        $this->assertEquals(5, (int) $rows[2]['id']);
        $this->assertEquals(8, (int) $rows[3]['id']);
    }

    /**
     * BETWEEN with DATE type columns.
     */
    public function testBetweenDateRange(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, customer, order_date
             FROM pg_bi_orders
             WHERE order_date BETWEEN '2025-03-01' AND '2025-06-30'
             ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertEquals(3, (int) $rows[0]['id']);
        $this->assertEquals(4, (int) $rows[1]['id']);
        $this->assertEquals(5, (int) $rows[2]['id']);
        $this->assertEquals(6, (int) $rows[3]['id']);
    }

    /**
     * IN with a moderate-sized list (12 values).
     */
    public function testInWithLargeList(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, customer
             FROM pg_bi_orders
             WHERE customer IN (
                 'Alice', 'Bob', 'Carol', 'Dave', 'Eve',
                 'Frank', 'Grace', 'Heidi', 'Ivan', 'Judy',
                 'Karl', 'Liam'
             )
             ORDER BY id"
        );

        $this->assertCount(8, $rows);
    }

    /**
     * NOT BETWEEN combined with IN.
     */
    public function testNotBetweenCombinedWithIn(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, customer, amount, status
             FROM pg_bi_orders
             WHERE amount NOT BETWEEN 100 AND 500
               AND status IN ('shipped', 'pending')
             ORDER BY id"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertEquals(6, (int) $rows[1]['id']);
        $this->assertEquals(7, (int) $rows[2]['id']);
    }

    /**
     * Complex nested boolean: (A AND B) OR (C AND NOT D).
     */
    public function testComplexNestedBoolean(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, customer, amount, status
             FROM pg_bi_orders
             WHERE (amount > 200 AND status = 'shipped')
                OR (customer = 'Alice' AND NOT status = 'cancelled')
             ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertEquals(3, (int) $rows[1]['id']);
        $this->assertEquals(5, (int) $rows[2]['id']);
        $this->assertEquals(7, (int) $rows[3]['id']);
    }

    /**
     * Prepared statement with BETWEEN parameters.
     */
    public function testPreparedBetweenParameters(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, customer, amount
             FROM pg_bi_orders
             WHERE amount BETWEEN ? AND ?
             ORDER BY id",
            [100, 500]
        );

        $this->assertCount(4, $rows);
        $this->assertEquals(2, (int) $rows[0]['id']);
        $this->assertEquals(3, (int) $rows[1]['id']);
        $this->assertEquals(5, (int) $rows[2]['id']);
        $this->assertEquals(8, (int) $rows[3]['id']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) FROM pg_bi_orders")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['count']);
    }
}
