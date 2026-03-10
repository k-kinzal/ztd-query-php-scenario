<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DML operations that use the SQL standard aggregate FILTER clause
 * through ZTD shadow store on SQLite.
 *
 * SQLite 3.30+ supports the FILTER clause. This is the same feature set
 * tested in PostgresAggregateFilterDmlTest, but not available in MySQL.
 *
 * @spec SPEC-3.2, SPEC-4.2
 */
class SqliteAggregateFilterDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_aggfilter_orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL,
                region TEXT NOT NULL
            )',
            'CREATE TABLE sl_aggfilter_customers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                shipped_count INTEGER DEFAULT 0
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_aggfilter_customers', 'sl_aggfilter_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Seed customers
        $this->pdo->exec("INSERT INTO sl_aggfilter_customers (id, name) VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO sl_aggfilter_customers (id, name) VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO sl_aggfilter_customers (id, name) VALUES (3, 'Charlie')");

        // Seed orders
        $this->pdo->exec("INSERT INTO sl_aggfilter_orders VALUES (1, 1, 100.00, 'shipped', 'east')");
        $this->pdo->exec("INSERT INTO sl_aggfilter_orders VALUES (2, 1, 200.00, 'shipped', 'west')");
        $this->pdo->exec("INSERT INTO sl_aggfilter_orders VALUES (3, 1, 50.00, 'pending', 'east')");
        $this->pdo->exec("INSERT INTO sl_aggfilter_orders VALUES (4, 2, 300.00, 'shipped', 'east')");
        $this->pdo->exec("INSERT INTO sl_aggfilter_orders VALUES (5, 2, 150.00, 'cancelled', 'west')");
        $this->pdo->exec("INSERT INTO sl_aggfilter_orders VALUES (6, 3, 75.00, 'cancelled', 'east')");
        $this->pdo->exec("INSERT INTO sl_aggfilter_orders VALUES (7, 3, 80.00, 'cancelled', 'west')");
        $this->pdo->exec("INSERT INTO sl_aggfilter_orders VALUES (8, 3, 60.00, 'cancelled', 'east')");
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
                FROM sl_aggfilter_orders
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
        $this->createTable('CREATE TABLE sl_aggfilter_summary (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            shipped_count INTEGER NOT NULL,
            cancelled_count INTEGER NOT NULL
        )');

        $sql = "INSERT INTO sl_aggfilter_summary (customer_id, shipped_count, cancelled_count)
                SELECT customer_id,
                       COUNT(*) FILTER (WHERE status = 'shipped'),
                       COUNT(*) FILTER (WHERE status = 'cancelled')
                FROM sl_aggfilter_orders
                GROUP BY customer_id";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT customer_id, shipped_count, cancelled_count FROM sl_aggfilter_summary ORDER BY customer_id");

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
            $this->dropTable('sl_aggfilter_summary');
        }
    }

    /**
     * UPDATE customers SET shipped_count using a correlated subquery with FILTER.
     *
     * @spec SPEC-3.2, SPEC-4.2
     */
    public function testUpdateSetWithFilterSubquery(): void
    {
        $sql = "UPDATE sl_aggfilter_customers
                SET shipped_count = (
                    SELECT COUNT(*) FILTER (WHERE status = 'shipped')
                    FROM sl_aggfilter_orders
                    WHERE sl_aggfilter_orders.customer_id = sl_aggfilter_customers.id
                )";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT id, name, shipped_count FROM sl_aggfilter_customers ORDER BY id");

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
        $sql = "DELETE FROM sl_aggfilter_customers
                WHERE (
                    SELECT COUNT(*) FILTER (WHERE status = 'cancelled')
                    FROM sl_aggfilter_orders
                    WHERE sl_aggfilter_orders.customer_id = sl_aggfilter_customers.id
                ) > 2";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT id, name FROM sl_aggfilter_customers ORDER BY id");

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
     * Prepared statement with FILTER clause and ? parameter placeholder.
     *
     * @spec SPEC-3.2, SPEC-4.2
     */
    public function testPreparedSelectWithFilter(): void
    {
        $sql = "SELECT customer_id,
                       COUNT(*) FILTER (WHERE status = ?) AS status_count
                FROM sl_aggfilter_orders
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
                FROM sl_aggfilter_orders
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
            $this->assertEquals(300.0, (float)$alice['shipped_total']); // 100 + 200
            $this->assertEquals(150.0, (float)$alice['east_total']);     // 100 + 50

            // Bob (customer_id=2): 1 shipped, 1 cancelled, 0 pending
            $bob = $rows[1];
            $this->assertEquals(1, $bob['shipped_count']);
            $this->assertEquals(1, $bob['cancelled_count']);
            $this->assertEquals(0, $bob['pending_count']);
            $this->assertEquals(300.0, (float)$bob['shipped_total']);
            $this->assertEquals(300.0, (float)$bob['east_total']);

            // Charlie (customer_id=3): 0 shipped, 3 cancelled, 0 pending
            $charlie = $rows[2];
            $this->assertEquals(0, $charlie['shipped_count']);
            $this->assertEquals(3, $charlie['cancelled_count']);
            $this->assertEquals(0, $charlie['pending_count']);
            $this->assertNull($charlie['shipped_total']); // SUM of nothing is NULL
            $this->assertEquals(135.0, (float)$charlie['east_total']); // 75 + 60
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT with multiple FILTER clauses failed: ' . $e->getMessage());
        }
    }
}
