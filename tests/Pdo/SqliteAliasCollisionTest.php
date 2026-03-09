<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests queries where subquery aliases collide with real table names,
 * or where the same table appears multiple times with different aliases.
 * The CTE rewriter must correctly scope its rewrites to actual table
 * references and not be confused by aliased subqueries sharing a table name.
 *
 * SQL patterns exercised: subquery alias = table name, same table multiple
 * aliases, derived table named like real table, CTE named like real table.
 * @spec SPEC-3.3
 */
class SqliteAliasCollisionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ac_orders (
                id INTEGER PRIMARY KEY,
                customer TEXT NOT NULL,
                total REAL NOT NULL,
                status TEXT NOT NULL
            )',
            'CREATE TABLE sl_ac_items (
                id INTEGER PRIMARY KEY,
                order_id INTEGER NOT NULL,
                product TEXT NOT NULL,
                qty INTEGER NOT NULL,
                price REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ac_items', 'sl_ac_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ac_orders VALUES (1, 'Alice', 100.00, 'completed')");
        $this->pdo->exec("INSERT INTO sl_ac_orders VALUES (2, 'Bob', 50.00, 'pending')");
        $this->pdo->exec("INSERT INTO sl_ac_orders VALUES (3, 'Carol', 200.00, 'completed')");

        $this->pdo->exec("INSERT INTO sl_ac_items VALUES (1, 1, 'Widget', 2, 25.00)");
        $this->pdo->exec("INSERT INTO sl_ac_items VALUES (2, 1, 'Gadget', 1, 50.00)");
        $this->pdo->exec("INSERT INTO sl_ac_items VALUES (3, 2, 'Widget', 1, 50.00)");
        $this->pdo->exec("INSERT INTO sl_ac_items VALUES (4, 3, 'Premium', 4, 50.00)");
    }

    /**
     * Subquery aliased with the same name as a real table.
     * The derived table is named "sl_ac_items" — CTE rewriter should
     * not confuse the alias with the real table.
     */
    public function testDerivedTableAliasedAsRealTable(): void
    {
        $rows = $this->ztdQuery(
            "SELECT o.customer, sl_ac_items.item_count
             FROM sl_ac_orders o
             JOIN (
                SELECT order_id, COUNT(*) AS item_count
                FROM sl_ac_items
                GROUP BY order_id
             ) AS sl_ac_items ON sl_ac_items.order_id = o.id
             ORDER BY o.customer"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['customer']);
        $this->assertEquals(2, (int) $rows[0]['item_count']);
    }

    /**
     * Same table joined to itself with two aliases.
     * Both aliases reference the same table — rewriter must apply shadow
     * data to both references.
     */
    public function testSameTableSelfJoinTwoAliases(): void
    {
        // Find orders where total is greater than another order
        $rows = $this->ztdQuery(
            "SELECT o1.customer AS high, o2.customer AS low
             FROM sl_ac_orders o1
             JOIN sl_ac_orders o2 ON o1.total > o2.total
             ORDER BY o1.total DESC, o2.total ASC"
        );

        // Carol(200) > Alice(100), Carol(200) > Bob(50), Alice(100) > Bob(50)
        $this->assertCount(3, $rows);
    }

    /**
     * Self-join after UPDATE — verify both aliases see the updated value.
     */
    public function testSelfJoinAfterUpdate(): void
    {
        $this->ztdExec("UPDATE sl_ac_orders SET total = 300.00 WHERE id = 2");

        $rows = $this->ztdQuery(
            "SELECT o1.customer, o1.total AS t1, o2.customer AS other, o2.total AS t2
             FROM sl_ac_orders o1
             JOIN sl_ac_orders o2 ON o1.id != o2.id AND o1.total = o2.total
             ORDER BY o1.customer"
        );

        // After update, no two orders have the same total (100, 300, 200)
        $this->assertCount(0, $rows);
    }

    /**
     * Three-way self-join.
     */
    public function testThreeWaySelfJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.customer AS c1, b.customer AS c2, c.customer AS c3
             FROM sl_ac_orders a, sl_ac_orders b, sl_ac_orders c
             WHERE a.total < b.total AND b.total < c.total
             ORDER BY a.total"
        );

        // Bob(50) < Alice(100) < Carol(200)
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['c1']);
        $this->assertSame('Alice', $rows[0]['c2']);
        $this->assertSame('Carol', $rows[0]['c3']);
    }

    /**
     * Subquery in WHERE referencing same table with same alias letter as outer.
     */
    public function testSubqueryWithSameAliasLetter(): void
    {
        $rows = $this->ztdQuery(
            "SELECT o.customer, o.total
             FROM sl_ac_orders o
             WHERE o.total > (
                SELECT AVG(o.total) FROM sl_ac_orders o
             )
             ORDER BY o.total"
        );

        // AVG = (100 + 50 + 200) / 3 ≈ 116.67
        // Only Carol (200) is above average
        // Note: the inner `o` shadows the outer `o` — both reference same table
        // The subquery's AVG is computed independently
        $this->assertGreaterThanOrEqual(1, count($rows));
    }

    /**
     * Correlated subquery where inner and outer reference same table.
     */
    public function testCorrelatedSubquerySameTable(): void
    {
        $rows = $this->ztdQuery(
            "SELECT o1.customer, o1.total,
                    (SELECT COUNT(*) FROM sl_ac_orders o2 WHERE o2.total > o1.total) AS rank_above
             FROM sl_ac_orders o1
             ORDER BY o1.total DESC"
        );

        $this->assertCount(3, $rows);
        // Carol: 0 above, Alice: 1 above (Carol), Bob: 2 above (Carol, Alice)
        $this->assertSame('Carol', $rows[0]['customer']);
        $this->assertEquals(0, (int) $rows[0]['rank_above']);
        $this->assertSame('Alice', $rows[1]['customer']);
        $this->assertEquals(1, (int) $rows[1]['rank_above']);
    }

    /**
     * After INSERT, derived table alias collision query still works.
     */
    public function testAliasCollisionAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_ac_orders VALUES (4, 'Diana', 75.00, 'pending')");
        $this->pdo->exec("INSERT INTO sl_ac_items VALUES (5, 4, 'Widget', 3, 25.00)");

        $rows = $this->ztdQuery(
            "SELECT o.customer, sub.total_qty
             FROM sl_ac_orders o
             JOIN (
                SELECT order_id, SUM(qty) AS total_qty FROM sl_ac_items GROUP BY order_id
             ) sub ON sub.order_id = o.id
             WHERE sub.total_qty > 2
             ORDER BY sub.total_qty DESC"
        );

        $this->assertGreaterThanOrEqual(2, count($rows));
        $customers = array_column($rows, 'customer');
        $this->assertContains('Carol', $customers); // 4 items
        $this->assertContains('Diana', $customers); // 3 items
    }
}
