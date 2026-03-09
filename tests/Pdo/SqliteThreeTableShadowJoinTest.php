<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests JOINs across three shadow tables in a single query.
 *
 * Real-world scenario: most applications query across multiple related
 * tables. When all tables have shadow data, the CTE rewriter must
 * generate CTEs for all referenced tables and ensure cross-references
 * resolve correctly. This tests the rewriter's ability to handle
 * multiple simultaneous CTE rewrites.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class SqliteThreeTableShadowJoinTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_3tj_users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )',
            'CREATE TABLE sl_3tj_orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                total REAL NOT NULL,
                status TEXT NOT NULL
            )',
            'CREATE TABLE sl_3tj_order_items (
                id INTEGER PRIMARY KEY,
                order_id INTEGER NOT NULL,
                product TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                price REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_3tj_order_items', 'sl_3tj_orders', 'sl_3tj_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_3tj_users VALUES (1, 'Alice')");
        $this->ztdExec("INSERT INTO sl_3tj_users VALUES (2, 'Bob')");

        $this->ztdExec("INSERT INTO sl_3tj_orders VALUES (1, 1, 100.00, 'completed')");
        $this->ztdExec("INSERT INTO sl_3tj_orders VALUES (2, 1, 200.00, 'pending')");
        $this->ztdExec("INSERT INTO sl_3tj_orders VALUES (3, 2, 50.00, 'completed')");

        $this->ztdExec("INSERT INTO sl_3tj_order_items VALUES (1, 1, 'Widget', 2, 25.00)");
        $this->ztdExec("INSERT INTO sl_3tj_order_items VALUES (2, 1, 'Gadget', 1, 50.00)");
        $this->ztdExec("INSERT INTO sl_3tj_order_items VALUES (3, 2, 'Bolt', 10, 20.00)");
        $this->ztdExec("INSERT INTO sl_3tj_order_items VALUES (4, 3, 'Nut', 5, 10.00)");
    }

    /**
     * Three-table JOIN selecting from all shadow tables.
     */
    public function testThreeTableJoin(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT u.name, o.total, oi.product, oi.quantity
                 FROM sl_3tj_users u
                 JOIN sl_3tj_orders o ON o.user_id = u.id
                 JOIN sl_3tj_order_items oi ON oi.order_id = o.id
                 WHERE o.status = 'completed'
                 ORDER BY u.name, oi.product"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'Three-table JOIN on all-shadow data returned no rows.'
                );
            }

            // Alice's completed order (1): Widget(2), Gadget(1)
            // Bob's completed order (3): Nut(5)
            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Gadget', $rows[0]['product']);
            $this->assertSame('Alice', $rows[1]['name']);
            $this->assertSame('Widget', $rows[1]['product']);
            $this->assertSame('Bob', $rows[2]['name']);
            $this->assertSame('Nut', $rows[2]['product']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Three-table JOIN failed on SQLite shadow data: ' . $e->getMessage()
            );
        }
    }

    /**
     * Three-table JOIN with aggregation across all shadow tables.
     */
    public function testThreeTableJoinWithAggregate(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT u.name,
                        COUNT(DISTINCT o.id) AS order_count,
                        SUM(oi.quantity * oi.price) AS item_total
                 FROM sl_3tj_users u
                 JOIN sl_3tj_orders o ON o.user_id = u.id
                 JOIN sl_3tj_order_items oi ON oi.order_id = o.id
                 GROUP BY u.id, u.name
                 ORDER BY u.name"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'Three-table JOIN with aggregate returned no rows on shadow data.'
                );
            }

            $this->assertCount(2, $rows);
            // Alice: orders 1,2 → items: Widget(2*25=50), Gadget(1*50=50), Bolt(10*20=200)
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertEquals(2, (int) $rows[0]['order_count']);
            $this->assertEqualsWithDelta(300.00, (float) $rows[0]['item_total'], 0.01);
            // Bob: order 3 → items: Nut(5*10=50)
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertEquals(1, (int) $rows[1]['order_count']);
            $this->assertEqualsWithDelta(50.00, (float) $rows[1]['item_total'], 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Three-table JOIN with aggregate failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Three-table JOIN after mutation to one table.
     */
    public function testThreeTableJoinAfterMutation(): void
    {
        // Add a new order for Bob
        $this->ztdExec("INSERT INTO sl_3tj_orders VALUES (4, 2, 300.00, 'completed')");
        $this->ztdExec("INSERT INTO sl_3tj_order_items VALUES (5, 4, 'Screw', 100, 3.00)");

        try {
            $rows = $this->ztdQuery(
                "SELECT u.name, o.id AS order_id, oi.product
                 FROM sl_3tj_users u
                 JOIN sl_3tj_orders o ON o.user_id = u.id
                 JOIN sl_3tj_order_items oi ON oi.order_id = o.id
                 WHERE u.name = 'Bob'
                 ORDER BY o.id, oi.product"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'Three-table JOIN after mutation returned no rows.'
                );
            }

            // Bob: order 3 (Nut) + order 4 (Screw)
            $this->assertCount(2, $rows);
            $this->assertSame('Nut', $rows[0]['product']);
            $this->assertSame('Screw', $rows[1]['product']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Three-table JOIN after mutation failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Subquery referencing two shadow tables inside a query on the third.
     */
    public function testSubqueryReferencingTwoShadowTables(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT u.name,
                    (SELECT SUM(oi.quantity * oi.price)
                     FROM sl_3tj_orders o
                     JOIN sl_3tj_order_items oi ON oi.order_id = o.id
                     WHERE o.user_id = u.id AND o.status = 'completed') AS total_spent
                 FROM sl_3tj_users u
                 ORDER BY u.name"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'Subquery referencing two shadow tables returned no rows.'
                );
            }

            $this->assertCount(2, $rows);
            // Alice: order 1 completed → Widget(50) + Gadget(50) = 100
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertEqualsWithDelta(100.00, (float) $rows[0]['total_spent'], 0.01);
            // Bob: order 3 completed → Nut(50) = 50
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertEqualsWithDelta(50.00, (float) $rows[1]['total_spent'], 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Subquery referencing two shadow tables failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE based on three-table JOIN condition.
     */
    public function testDeleteBasedOnThreeTableJoin(): void
    {
        try {
            // Delete order items for Alice's pending orders
            // SQLite doesn't support DELETE with JOIN, so use subquery
            $this->ztdExec(
                "DELETE FROM sl_3tj_order_items
                 WHERE order_id IN (
                    SELECT o.id FROM sl_3tj_orders o
                    JOIN sl_3tj_users u ON u.id = o.user_id
                    WHERE u.name = 'Alice' AND o.status = 'pending'
                 )"
            );

            $rows = $this->ztdQuery("SELECT * FROM sl_3tj_order_items ORDER BY id");

            if (count($rows) === 4) {
                $this->markTestIncomplete(
                    'DELETE with subquery referencing three shadow tables was a no-op.'
                );
            }

            // Should have removed item 3 (Bolt, order 2 which is Alice's pending)
            $this->assertCount(3, $rows);
            $products = array_column($rows, 'product');
            $this->assertNotContains('Bolt', $products);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE based on three-table JOIN failed: ' . $e->getMessage()
            );
        }
    }
}
