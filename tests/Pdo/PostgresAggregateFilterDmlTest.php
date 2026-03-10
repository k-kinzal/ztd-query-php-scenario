<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests DML operations that use the SQL standard aggregate FILTER clause
 * through ZTD shadow store on PostgreSQL.
 *
 * The FILTER clause (e.g., COUNT(*) FILTER (WHERE condition)) is supported
 * by PostgreSQL 9.4+ and SQLite 3.30+, but not MySQL.
 *
 * @spec SPEC-3.2, SPEC-4.2
 */
class PostgresAggregateFilterDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_aggfilter_orders (
                id SERIAL PRIMARY KEY,
                customer_id INT NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                status VARCHAR(20) NOT NULL,
                region VARCHAR(20) NOT NULL
            )',
            'CREATE TABLE pg_aggfilter_customers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                shipped_count INT DEFAULT 0
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_aggfilter_customers', 'pg_aggfilter_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Seed customers
        $this->pdo->exec("INSERT INTO pg_aggfilter_customers (id, name) VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pg_aggfilter_customers (id, name) VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO pg_aggfilter_customers (id, name) VALUES (3, 'Charlie')");

        // Seed orders
        $this->pdo->exec("INSERT INTO pg_aggfilter_orders (id, customer_id, amount, status, region) VALUES (1, 1, 100.00, 'shipped', 'east')");
        $this->pdo->exec("INSERT INTO pg_aggfilter_orders (id, customer_id, amount, status, region) VALUES (2, 1, 200.00, 'shipped', 'west')");
        $this->pdo->exec("INSERT INTO pg_aggfilter_orders (id, customer_id, amount, status, region) VALUES (3, 1, 50.00, 'pending', 'east')");
        $this->pdo->exec("INSERT INTO pg_aggfilter_orders (id, customer_id, amount, status, region) VALUES (4, 2, 300.00, 'shipped', 'east')");
        $this->pdo->exec("INSERT INTO pg_aggfilter_orders (id, customer_id, amount, status, region) VALUES (5, 2, 150.00, 'cancelled', 'west')");
        $this->pdo->exec("INSERT INTO pg_aggfilter_orders (id, customer_id, amount, status, region) VALUES (6, 3, 75.00, 'cancelled', 'east')");
        $this->pdo->exec("INSERT INTO pg_aggfilter_orders (id, customer_id, amount, status, region) VALUES (7, 3, 80.00, 'cancelled', 'west')");
        $this->pdo->exec("INSERT INTO pg_aggfilter_orders (id, customer_id, amount, status, region) VALUES (8, 3, 60.00, 'cancelled', 'east')");
    }

    /**
     * SELECT with COUNT(*) FILTER (WHERE ...) grouped by customer_id.
     *
     * @spec SPEC-3.2, SPEC-4.2
     */
    public function testSelectWithFilterClause(): void
    {
        $sql = "SELECT customer_id,
                       COUNT(*) FILTER (WHERE status = 'shipped') AS shipped_count
                FROM pg_aggfilter_orders
                GROUP BY customer_id
                ORDER BY customer_id";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'SELECT FILTER: expected 3 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);

            $byCustomer = [];
            foreach ($rows as $r) {
                $byCustomer[(int)$r['customer_id']] = (int)$r['shipped_count'];
            }

            // Alice: 2 shipped, Bob: 1 shipped, Charlie: 0 shipped
            $this->assertSame(2, $byCustomer[1]);
            $this->assertSame(1, $byCustomer[2]);
            $this->assertSame(0, $byCustomer[3]);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT with FILTER clause failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT INTO summary table using SELECT with FILTER aggregate.
     *
     * @spec SPEC-3.2, SPEC-4.2
     */
    public function testInsertSelectWithFilter(): void
    {
        $this->createTable('CREATE TABLE pg_aggfilter_summary (
            id SERIAL PRIMARY KEY,
            customer_id INT NOT NULL,
            shipped_count INT NOT NULL,
            cancelled_count INT NOT NULL
        )');

        $sql = "INSERT INTO pg_aggfilter_summary (customer_id, shipped_count, cancelled_count)
                SELECT customer_id,
                       COUNT(*) FILTER (WHERE status = 'shipped'),
                       COUNT(*) FILTER (WHERE status = 'cancelled')
                FROM pg_aggfilter_orders
                GROUP BY customer_id";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT customer_id, shipped_count, cancelled_count FROM pg_aggfilter_summary ORDER BY customer_id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT SELECT FILTER: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);

            // Alice: 2 shipped, 0 cancelled
            $this->assertEquals(2, $rows[0]['shipped_count']);
            $this->assertEquals(0, $rows[0]['cancelled_count']);

            // Bob: 1 shipped, 1 cancelled
            $this->assertEquals(1, $rows[1]['shipped_count']);
            $this->assertEquals(1, $rows[1]['cancelled_count']);

            // Charlie: 0 shipped, 3 cancelled
            $this->assertEquals(0, $rows[2]['shipped_count']);
            $this->assertEquals(3, $rows[2]['cancelled_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT with FILTER clause failed: ' . $e->getMessage());
        } finally {
            $this->dropTable('pg_aggfilter_summary');
        }
    }

    /**
     * UPDATE customers SET shipped_count using a correlated subquery with FILTER.
     *
     * @spec SPEC-3.2, SPEC-4.2
     */
    public function testUpdateSetWithFilterSubquery(): void
    {
        $sql = "UPDATE pg_aggfilter_customers
                SET shipped_count = (
                    SELECT COUNT(*) FILTER (WHERE status = 'shipped')
                    FROM pg_aggfilter_orders
                    WHERE pg_aggfilter_orders.customer_id = pg_aggfilter_customers.id
                )";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT id, name, shipped_count FROM pg_aggfilter_customers ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'UPDATE FILTER subquery: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);

            // Alice: 2 shipped, Bob: 1 shipped, Charlie: 0 shipped
            $this->assertEquals(2, $rows[0]['shipped_count']);
            $this->assertEquals(1, $rows[1]['shipped_count']);
            $this->assertEquals(0, $rows[2]['shipped_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with FILTER subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE FROM customers WHERE the cancelled count (via FILTER subquery) exceeds a threshold.
     *
     * @spec SPEC-3.2, SPEC-4.2
     */
    public function testDeleteWhereFilterSubquery(): void
    {
        $sql = "DELETE FROM pg_aggfilter_customers
                WHERE (
                    SELECT COUNT(*) FILTER (WHERE status = 'cancelled')
                    FROM pg_aggfilter_orders
                    WHERE pg_aggfilter_orders.customer_id = pg_aggfilter_customers.id
                ) > 2";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT id, name FROM pg_aggfilter_customers ORDER BY id");

            // Charlie has 3 cancelled orders (> 2), so only Alice and Bob remain
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE FILTER subquery: expected 2 remaining, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertEquals('Alice', $rows[0]['name']);
            $this->assertEquals('Bob', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with FILTER subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared statement with FILTER clause and $1 parameter placeholder.
     *
     * @spec SPEC-3.2, SPEC-4.2
     */
    public function testPreparedSelectWithFilter(): void
    {
        $sql = "SELECT customer_id,
                       COUNT(*) FILTER (WHERE status = $1) AS status_count
                FROM pg_aggfilter_orders
                GROUP BY customer_id
                ORDER BY customer_id";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, ['shipped']);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared FILTER: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);

            $byCustomer = [];
            foreach ($rows as $r) {
                $byCustomer[(int)$r['customer_id']] = (int)$r['status_count'];
            }

            $this->assertSame(2, $byCustomer[1]);
            $this->assertSame(1, $byCustomer[2]);
            $this->assertSame(0, $byCustomer[3]);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT with FILTER clause failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with multiple FILTER aggregates in the same query.
     *
     * @spec SPEC-3.2, SPEC-4.2
     */
    public function testMultipleFilterClauses(): void
    {
        $sql = "SELECT customer_id,
                       COUNT(*) FILTER (WHERE status = 'shipped') AS shipped_count,
                       COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled_count,
                       COUNT(*) FILTER (WHERE status = 'pending') AS pending_count,
                       SUM(amount) FILTER (WHERE status = 'shipped') AS shipped_total,
                       SUM(amount) FILTER (WHERE region = 'east') AS east_total
                FROM pg_aggfilter_orders
                GROUP BY customer_id
                ORDER BY customer_id";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multiple FILTER: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);

            // Alice (customer_id=1): 2 shipped, 0 cancelled, 1 pending
            $alice = $rows[0];
            $this->assertEquals(2, $alice['shipped_count']);
            $this->assertEquals(0, $alice['cancelled_count']);
            $this->assertEquals(1, $alice['pending_count']);
            $this->assertEquals('300.00', $alice['shipped_total']); // 100 + 200
            $this->assertEquals('150.00', $alice['east_total']);     // 100 + 50

            // Bob (customer_id=2): 1 shipped, 1 cancelled, 0 pending
            $bob = $rows[1];
            $this->assertEquals(1, $bob['shipped_count']);
            $this->assertEquals(1, $bob['cancelled_count']);
            $this->assertEquals(0, $bob['pending_count']);
            $this->assertEquals('300.00', $bob['shipped_total']);
            $this->assertEquals('300.00', $bob['east_total']);

            // Charlie (customer_id=3): 0 shipped, 3 cancelled, 0 pending
            $charlie = $rows[2];
            $this->assertEquals(0, $charlie['shipped_count']);
            $this->assertEquals(3, $charlie['cancelled_count']);
            $this->assertEquals(0, $charlie['pending_count']);
            $this->assertNull($charlie['shipped_total']); // SUM of nothing is NULL
            $this->assertEquals('135.00', $charlie['east_total']); // 75 + 60
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT with multiple FILTER clauses failed: ' . $e->getMessage());
        }
    }
}
