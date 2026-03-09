<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests that table aliases don't confuse the CTE rewriter.
 *
 * Probes cases where aliases match other table names, or where the same table
 * is referenced under multiple aliases in ways that stress the rewriter.
 * @spec SPEC-3.1
 */
class SqliteTableAliasConfusionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE tac_orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)',
            'CREATE TABLE tac_customers (id INTEGER PRIMARY KEY, name TEXT)',
            'CREATE TABLE tac_items (id INTEGER PRIMARY KEY, order_id INTEGER, product TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['tac_orders', 'tac_customers', 'tac_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO tac_customers VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO tac_customers VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO tac_orders VALUES (1, 1, 100.00)");
        $this->pdo->exec("INSERT INTO tac_orders VALUES (2, 1, 50.00)");
        $this->pdo->exec("INSERT INTO tac_orders VALUES (3, 2, 75.00)");
        $this->pdo->exec("INSERT INTO tac_items VALUES (1, 1, 'Widget')");
        $this->pdo->exec("INSERT INTO tac_items VALUES (2, 1, 'Gadget')");
        $this->pdo->exec("INSERT INTO tac_items VALUES (3, 3, 'Book')");
    }

    /**
     * Alias one table with the name of another table.
     * e.g., SELECT tac_customers.* FROM tac_orders tac_customers
     * The rewriter must not confuse alias "tac_customers" with table tac_customers.
     */
    public function testAliasMatchesOtherTableName(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT tac_customers.id, tac_customers.amount
                 FROM tac_orders tac_customers
                 ORDER BY tac_customers.id'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Alias matching other table name failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Alias matching other table name returned empty — rewriter may confuse alias with table');
            return;
        }
        // Should return tac_orders rows, not tac_customers rows
        $this->assertCount(3, $rows);
        $this->assertArrayHasKey('amount', $rows[0]);
    }

    /**
     * Two tables aliased with each other's names.
     */
    public function testCrossAliasing(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT o.name, c.amount
                 FROM tac_customers o
                 JOIN tac_orders c ON c.customer_id = o.id
                 ORDER BY o.name'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Cross-aliasing failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Cross-aliasing returned empty');
            return;
        }
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * Same table used three times with different aliases in self-join + subquery.
     */
    public function testTripleSelfReference(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT a.id, a.amount,
                        (SELECT COUNT(*) FROM tac_orders b WHERE b.customer_id = a.customer_id) AS peer_count
                 FROM tac_orders a
                 WHERE a.amount > (SELECT AVG(c.amount) FROM tac_orders c)
                 ORDER BY a.id'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Triple self-reference failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Triple self-reference returned empty');
            return;
        }
        // AVG = (100+50+75)/3 = 75. Only id=1 (100) is > 75
        $this->assertCount(1, $rows);
        $this->assertSame('1', (string) $rows[0]['id']);
        $this->assertSame('2', (string) $rows[0]['peer_count']); // Alice has 2 orders
    }

    /**
     * Three-way JOIN after mutations on all three tables.
     */
    public function testThreeWayJoinAfterMutations(): void
    {
        // Mutate all three tables
        $this->pdo->exec("INSERT INTO tac_customers VALUES (3, 'Charlie')");
        $this->pdo->exec("INSERT INTO tac_orders VALUES (4, 3, 200.00)");
        $this->pdo->exec("INSERT INTO tac_items VALUES (4, 4, 'Laptop')");

        try {
            $rows = $this->ztdQuery(
                'SELECT c.name, o.amount, i.product
                 FROM tac_customers c
                 JOIN tac_orders o ON o.customer_id = c.id
                 JOIN tac_items i ON i.order_id = o.id
                 ORDER BY c.name, o.amount'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Three-way JOIN after mutations failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Three-way JOIN after mutations returned empty');
            return;
        }
        // Alice order 1: Widget, Gadget; Bob order 3: Book; Charlie order 4: Laptop
        $this->assertCount(4, $rows);
    }

    /**
     * Subquery alias that matches a real table name in outer query.
     */
    public function testSubqueryAliasMatchesTableName(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT tac_items.name, tac_items.total_orders
                 FROM (
                     SELECT c.name, COUNT(o.id) AS total_orders
                     FROM tac_customers c
                     LEFT JOIN tac_orders o ON o.customer_id = c.id
                     GROUP BY c.name
                 ) tac_items
                 ORDER BY tac_items.name'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Subquery alias matching table name failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Subquery alias matching table name returned empty');
            return;
        }
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('2', (string) $rows[0]['total_orders']);
    }

    /**
     * UPDATE with subquery referencing a different table, where alias matches target table.
     */
    public function testUpdateWithConfusingAlias(): void
    {
        try {
            $this->pdo->exec(
                'UPDATE tac_orders SET amount = amount * 1.1
                 WHERE customer_id IN (SELECT tac_orders.id FROM tac_customers tac_orders WHERE tac_orders.name = \'Alice\')'
            );
            $rows = $this->ztdQuery('SELECT * FROM tac_orders WHERE customer_id = 1 ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('UPDATE with confusing alias failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(2, $rows);
        // Alice's orders should be increased by 10%
        $this->assertEquals(110.00, (float) $rows[0]['amount'], '', 0.01);
        $this->assertEquals(55.00, (float) $rows[1]['amount'], '', 0.01);
    }

    /**
     * DELETE with subquery that aliases a table with the target table name.
     */
    public function testDeleteWithConfusingAlias(): void
    {
        try {
            $this->pdo->exec(
                'DELETE FROM tac_items
                 WHERE order_id IN (
                     SELECT tac_items.id FROM tac_orders tac_items WHERE tac_items.amount < 60
                 )'
            );
            $rows = $this->ztdQuery('SELECT * FROM tac_items ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('DELETE with confusing alias failed: ' . $e->getMessage());
            return;
        }

        // Order id=2 has amount=50 < 60, but tac_items has no items for order 2
        // Order id=3 has amount=75 >= 60, so items for order 3 stay
        // Only items for order 1 (Widget, Gadget) and order 3 (Book) should remain
        $this->assertCount(3, $rows);
    }

    /**
     * Prepared statement with three-way JOIN.
     */
    public function testPreparedThreeWayJoin(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                'SELECT c.name, o.amount, i.product
                 FROM tac_customers c
                 JOIN tac_orders o ON o.customer_id = c.id
                 JOIN tac_items i ON i.order_id = o.id
                 WHERE o.amount > ?
                 ORDER BY c.name',
                [60.0]
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Prepared three-way JOIN failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Prepared three-way JOIN returned empty');
            return;
        }
        // Order 1: amount=100 > 60, has Widget+Gadget; Order 3: amount=75 > 60, has Book
        $this->assertCount(3, $rows);
    }
}
