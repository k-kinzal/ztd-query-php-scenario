<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests multi-step data transformation workflows through ZTD shadow store.
 * Simulates ETL-like patterns: staged mutations with intermediate reads and verification.
 * @spec SPEC-4.1, SPEC-4.2
 */
class PostgresMultiStepTransformationTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_mst_customers (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                tier TEXT,
                total_spent DOUBLE PRECISION DEFAULT 0,
                order_count INTEGER DEFAULT 0,
                last_order_date DATE
            )',
            'CREATE TABLE pg_mst_orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                amount DOUBLE PRECISION,
                status TEXT,
                created_at DATE
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_mst_orders', 'pg_mst_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_mst_customers VALUES (1, 'Alice', 'alice@example.com', 'bronze', 0, 0, NULL)");
        $this->pdo->exec("INSERT INTO pg_mst_customers VALUES (2, 'Bob', 'bob@example.com', 'bronze', 0, 0, NULL)");
        $this->pdo->exec("INSERT INTO pg_mst_customers VALUES (3, 'Charlie', 'charlie@example.com', 'silver', 500, 5, '2024-01-15')");
        $this->pdo->exec("INSERT INTO pg_mst_customers VALUES (4, 'Diana', 'diana@example.com', 'gold', 2000, 20, '2024-02-28')");
    }

    /**
     * Step 1: Insert orders, Step 2: Update customer aggregates,
     * Step 3: Recalculate tier, Step 4: Verify consistency.
     */
    public function testFullOrderProcessingPipeline(): void
    {
        // Step 1: Record new orders for Alice
        $this->pdo->exec("INSERT INTO pg_mst_orders VALUES (1, 1, 150.00, 'completed', '2024-03-01')");
        $this->pdo->exec("INSERT INTO pg_mst_orders VALUES (2, 1, 250.00, 'completed', '2024-03-05')");
        $this->pdo->exec("INSERT INTO pg_mst_orders VALUES (3, 1, 600.00, 'completed', '2024-03-10')");

        // Verify orders inserted
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_mst_orders WHERE customer_id = 1");
        $this->assertEquals(3, (int) $rows[0]['cnt']);

        // Step 2: Update customer aggregates
        // Note: Using WHERE id IN (SELECT ...) pattern since UPDATE FROM is not supported
        $this->pdo->exec(
            "UPDATE pg_mst_customers SET
                total_spent = total_spent + 1000.00,
                order_count = order_count + 3,
                last_order_date = '2024-03-10'
             WHERE id = 1"
        );

        // Verify aggregate update
        $rows = $this->ztdQuery("SELECT total_spent, order_count FROM pg_mst_customers WHERE id = 1");
        $this->assertEqualsWithDelta(1000.0, (float) $rows[0]['total_spent'], 0.01);
        $this->assertEquals(3, (int) $rows[0]['order_count']);

        // Step 3: Recalculate tiers based on total_spent
        $this->pdo->exec(
            "UPDATE pg_mst_customers SET tier = CASE
                WHEN total_spent >= 1000 THEN 'gold'
                WHEN total_spent >= 500 THEN 'silver'
                ELSE 'bronze'
             END"
        );

        // Step 4: Verify final state
        $rows = $this->ztdQuery(
            "SELECT name, tier, total_spent FROM pg_mst_customers ORDER BY total_spent DESC"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Diana', $rows[0]['name']);
        $this->assertSame('gold', $rows[0]['tier']);
        $this->assertSame('Alice', $rows[1]['name']);
        $this->assertSame('gold', $rows[1]['tier']); // Promoted from bronze
        $this->assertSame('Charlie', $rows[2]['name']);
        $this->assertSame('silver', $rows[2]['tier']); // Unchanged
        $this->assertSame('Bob', $rows[3]['name']);
        $this->assertSame('bronze', $rows[3]['tier']); // Unchanged
    }

    /**
     * Bulk insert then aggregate query across tables.
     */
    public function testBulkInsertThenCrossTableAggregate(): void
    {
        // Insert multiple orders for multiple customers
        $this->pdo->exec("INSERT INTO pg_mst_orders VALUES (1, 1, 100.00, 'completed', '2024-03-01')");
        $this->pdo->exec("INSERT INTO pg_mst_orders VALUES (2, 2, 200.00, 'completed', '2024-03-02')");
        $this->pdo->exec("INSERT INTO pg_mst_orders VALUES (3, 1, 300.00, 'completed', '2024-03-03')");
        $this->pdo->exec("INSERT INTO pg_mst_orders VALUES (4, 3, 150.00, 'completed', '2024-03-04')");
        $this->pdo->exec("INSERT INTO pg_mst_orders VALUES (5, 2, 250.00, 'pending', '2024-03-05')");

        // Cross-table aggregate: revenue per customer tier
        $rows = $this->ztdQuery(
            "SELECT c.tier,
                    COUNT(DISTINCT c.id) AS customer_count,
                    COUNT(o.id) AS order_count,
                    SUM(o.amount) AS total_revenue
             FROM pg_mst_customers c
             JOIN pg_mst_orders o ON c.id = o.customer_id
             WHERE o.status = 'completed'
             GROUP BY c.tier
             ORDER BY total_revenue DESC"
        );

        $this->assertGreaterThanOrEqual(1, count($rows));
        // Bronze customers (Alice + Bob): 100 + 200 + 300 = 600
        $bronze = array_values(array_filter($rows, fn($r) => $r['tier'] === 'bronze'));
        $this->assertCount(1, $bronze);
        $this->assertEqualsWithDelta(600.0, (float) $bronze[0]['total_revenue'], 0.01);
    }

    /**
     * Sequential UPDATE then DELETE then SELECT pipeline.
     */
    public function testUpdateDeleteSelectPipeline(): void
    {
        // Insert test orders
        $this->pdo->exec("INSERT INTO pg_mst_orders VALUES (1, 1, 500.00, 'completed', '2024-03-01')");
        $this->pdo->exec("INSERT INTO pg_mst_orders VALUES (2, 1, 100.00, 'cancelled', '2024-03-02')");
        $this->pdo->exec("INSERT INTO pg_mst_orders VALUES (3, 2, 300.00, 'completed', '2024-03-03')");

        // Step 1: Cancel all pending/completed orders for customer 1 < 200
        $result = $this->pdo->exec(
            "UPDATE pg_mst_orders SET status = 'cancelled' WHERE customer_id = 1 AND amount < 200"
        );
        $this->assertSame(1, $result); // The 100.00 order was already cancelled, but gets re-updated

        // Step 2: Delete all cancelled orders
        $result = $this->pdo->exec(
            "DELETE FROM pg_mst_orders WHERE status = 'cancelled'"
        );
        $this->assertSame(1, $result);

        // Step 3: Verify remaining orders
        $rows = $this->ztdQuery(
            "SELECT o.id, c.name, o.amount, o.status
             FROM pg_mst_orders o
             JOIN pg_mst_customers c ON o.customer_id = c.id
             ORDER BY o.id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEqualsWithDelta(500.0, (float) $rows[0]['amount'], 0.01);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    /**
     * Conditional insert based on aggregate check.
     */
    public function testConditionalInsertBasedOnAggregate(): void
    {
        // Insert orders
        $this->pdo->exec("INSERT INTO pg_mst_orders VALUES (1, 3, 100.00, 'completed', '2024-03-01')");
        $this->pdo->exec("INSERT INTO pg_mst_orders VALUES (2, 3, 200.00, 'completed', '2024-03-02')");

        // Check if Charlie's total exceeds threshold
        $rows = $this->ztdQuery(
            "SELECT SUM(amount) AS total FROM pg_mst_orders
             WHERE customer_id = 3 AND status = 'completed'"
        );
        $total = (float) $rows[0]['total'];
        $this->assertEqualsWithDelta(300.0, $total, 0.01);

        // Conditionally upgrade tier if threshold met
        if ($total >= 200) {
            $this->pdo->exec("UPDATE pg_mst_customers SET tier = 'gold' WHERE id = 3");
        }

        $rows = $this->ztdQuery("SELECT tier FROM pg_mst_customers WHERE id = 3");
        $this->assertSame('gold', $rows[0]['tier']);
    }

    /**
     * Interleaved reads and writes: each mutation is verified before proceeding.
     */
    public function testInterleavedReadsAndWrites(): void
    {
        // Write 1
        $this->pdo->exec("INSERT INTO pg_mst_orders VALUES (1, 1, 100.00, 'completed', '2024-03-01')");
        // Read 1: verify
        $rows = $this->ztdQuery("SELECT COUNT(*) AS c FROM pg_mst_orders");
        $this->assertEquals(1, (int) $rows[0]['c']);

        // Write 2
        $this->pdo->exec("INSERT INTO pg_mst_orders VALUES (2, 2, 200.00, 'completed', '2024-03-02')");
        // Read 2: verify
        $rows = $this->ztdQuery("SELECT COUNT(*) AS c FROM pg_mst_orders");
        $this->assertEquals(2, (int) $rows[0]['c']);

        // Write 3: update
        $this->pdo->exec("UPDATE pg_mst_orders SET amount = 150.00 WHERE id = 1");
        // Read 3: verify specific value
        $rows = $this->ztdQuery("SELECT amount FROM pg_mst_orders WHERE id = 1");
        $this->assertEqualsWithDelta(150.0, (float) $rows[0]['amount'], 0.01);

        // Write 4: delete
        $this->pdo->exec("DELETE FROM pg_mst_orders WHERE id = 2");
        // Read 4: verify deletion
        $rows = $this->ztdQuery("SELECT COUNT(*) AS c FROM pg_mst_orders");
        $this->assertEquals(1, (int) $rows[0]['c']);

        // Final aggregate
        $rows = $this->ztdQuery("SELECT SUM(amount) AS total FROM pg_mst_orders");
        $this->assertEqualsWithDelta(150.0, (float) $rows[0]['total'], 0.01);
    }

    /**
     * Multi-table cleanup: delete orders then reset customer stats.
     */
    public function testMultiTableCleanup(): void
    {
        // Seed orders
        $this->pdo->exec("INSERT INTO pg_mst_orders VALUES (1, 1, 100.00, 'completed', '2024-03-01')");
        $this->pdo->exec("INSERT INTO pg_mst_orders VALUES (2, 1, 200.00, 'completed', '2024-03-02')");

        // Update customer stats
        $this->pdo->exec("UPDATE pg_mst_customers SET total_spent = 300, order_count = 2 WHERE id = 1");

        // Now cleanup: delete all orders
        $this->pdo->exec("DELETE FROM pg_mst_orders WHERE 1=1");

        // Reset customer stats
        $this->pdo->exec("UPDATE pg_mst_customers SET total_spent = 0, order_count = 0, last_order_date = NULL WHERE id = 1");

        // Verify clean state
        $rows = $this->ztdQuery("SELECT COUNT(*) AS c FROM pg_mst_orders");
        $this->assertEquals(0, (int) $rows[0]['c']);

        $rows = $this->ztdQuery("SELECT total_spent, order_count FROM pg_mst_customers WHERE id = 1");
        $this->assertEqualsWithDelta(0.0, (float) $rows[0]['total_spent'], 0.01);
        $this->assertEquals(0, (int) $rows[0]['order_count']);
    }
}
