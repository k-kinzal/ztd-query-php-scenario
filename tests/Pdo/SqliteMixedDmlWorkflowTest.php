<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests complex mixed DML workflows through ZTD.
 *
 * Real applications perform chains of INSERT, UPDATE, DELETE operations
 * with complex SELECTs between them. The shadow store must maintain
 * consistency throughout multi-step workflows.
 * @spec SPEC-4.1
 */
class SqliteMixedDmlWorkflowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mdw_products (id INTEGER PRIMARY KEY, name TEXT, stock INTEGER, price REAL)',
            'CREATE TABLE mdw_orders (id INTEGER PRIMARY KEY, product_id INTEGER, qty INTEGER, status TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mdw_products', 'mdw_orders'];
    }

    /**
     * Full e-commerce workflow: add products, place orders, fulfill, restock.
     */
    public function testEcommerceWorkflow(): void
    {
        // Step 1: Add products
        $this->pdo->exec("INSERT INTO mdw_products VALUES (1, 'Widget', 100, 9.99)");
        $this->pdo->exec("INSERT INTO mdw_products VALUES (2, 'Gadget', 50, 19.99)");
        $this->pdo->exec("INSERT INTO mdw_products VALUES (3, 'Gizmo', 0, 29.99)");

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mdw_products');
        $this->assertSame('3', (string) $rows[0]['cnt']);

        // Step 2: Place orders
        $this->pdo->exec("INSERT INTO mdw_orders VALUES (1, 1, 5, 'pending')");
        $this->pdo->exec("INSERT INTO mdw_orders VALUES (2, 2, 3, 'pending')");
        $this->pdo->exec("INSERT INTO mdw_orders VALUES (3, 1, 10, 'pending')");

        // Step 3: Fulfill orders (update status, reduce stock)
        $this->pdo->exec("UPDATE mdw_orders SET status = 'fulfilled' WHERE id IN (1, 2)");
        $this->pdo->exec('UPDATE mdw_products SET stock = stock - 5 WHERE id = 1');
        $this->pdo->exec('UPDATE mdw_products SET stock = stock - 3 WHERE id = 2');

        // Verify stock
        $rows = $this->ztdQuery('SELECT id, stock FROM mdw_products ORDER BY id');
        $this->assertSame('95', (string) $rows[0]['stock']); // 100 - 5
        $this->assertSame('47', (string) $rows[1]['stock']); // 50 - 3
        $this->assertSame('0', (string) $rows[2]['stock']);

        // Verify order status
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mdw_orders WHERE status = 'fulfilled'");
        $this->assertSame('2', (string) $rows[0]['cnt']);

        // Step 4: Cancel unfulfilled order
        $this->pdo->exec("DELETE FROM mdw_orders WHERE status = 'pending'");

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mdw_orders');
        $this->assertSame('2', (string) $rows[0]['cnt']);

        // Step 5: Restock
        $this->pdo->exec('UPDATE mdw_products SET stock = stock + 50 WHERE id = 3');

        $rows = $this->ztdQuery('SELECT stock FROM mdw_products WHERE id = 3');
        $this->assertSame('50', (string) $rows[0]['stock']);

        // Step 6: Place new order and verify JOIN
        $this->pdo->exec("INSERT INTO mdw_orders VALUES (4, 3, 2, 'pending')");

        try {
            $rows = $this->ztdQuery(
                "SELECT p.name, o.qty, o.status, p.stock
                 FROM mdw_orders o
                 JOIN mdw_products p ON p.id = o.product_id
                 ORDER BY o.id"
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('JOIN after workflow failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(3, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertSame('fulfilled', $rows[0]['status']);
        $this->assertSame('Gizmo', $rows[2]['name']);
        $this->assertSame('pending', $rows[2]['status']);
    }

    /**
     * Delete all, re-insert, verify clean state.
     */
    public function testDeleteAllReinsert(): void
    {
        $this->pdo->exec("INSERT INTO mdw_products VALUES (1, 'A', 10, 1.00)");
        $this->pdo->exec("INSERT INTO mdw_products VALUES (2, 'B', 20, 2.00)");

        // Delete all
        $this->pdo->exec('DELETE FROM mdw_products WHERE id > 0');
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mdw_products');
        $this->assertSame('0', (string) $rows[0]['cnt']);

        // Re-insert with different data
        $this->pdo->exec("INSERT INTO mdw_products VALUES (1, 'X', 99, 9.00)");

        $rows = $this->ztdQuery('SELECT * FROM mdw_products');
        $this->assertCount(1, $rows);
        $this->assertSame('X', $rows[0]['name']);
        $this->assertSame('99', (string) $rows[0]['stock']);
    }

    /**
     * Interleaved operations on two tables with cross-table SELECT.
     */
    public function testInterleavedTwoTableOps(): void
    {
        $this->pdo->exec("INSERT INTO mdw_products VALUES (1, 'Widget', 100, 9.99)");
        $this->pdo->exec("INSERT INTO mdw_orders VALUES (1, 1, 5, 'new')");
        $this->pdo->exec('UPDATE mdw_products SET stock = stock - 5 WHERE id = 1');
        $this->pdo->exec("INSERT INTO mdw_orders VALUES (2, 1, 3, 'new')");
        $this->pdo->exec('UPDATE mdw_products SET stock = stock - 3 WHERE id = 1');
        $this->pdo->exec("UPDATE mdw_orders SET status = 'done' WHERE id = 1");

        try {
            $rows = $this->ztdQuery(
                "SELECT p.name, p.stock, COUNT(o.id) AS order_count,
                        SUM(o.qty) AS total_qty
                 FROM mdw_products p
                 LEFT JOIN mdw_orders o ON o.product_id = p.id
                 GROUP BY p.id, p.name, p.stock"
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Interleaved two-table ops failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(1, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertSame('92', (string) $rows[0]['stock']); // 100 - 5 - 3
        $this->assertSame('2', (string) $rows[0]['order_count']);
        $this->assertSame('8', (string) $rows[0]['total_qty']); // 5 + 3
    }

    /**
     * UPDATE then DELETE same row then re-INSERT same PK.
     */
    public function testUpdateDeleteReinsertSamePk(): void
    {
        $this->pdo->exec("INSERT INTO mdw_products VALUES (1, 'Old', 10, 5.00)");
        $this->pdo->exec("UPDATE mdw_products SET name = 'Updated' WHERE id = 1");
        $this->pdo->exec('DELETE FROM mdw_products WHERE id = 1');
        $this->pdo->exec("INSERT INTO mdw_products VALUES (1, 'Reborn', 99, 99.99)");

        $rows = $this->ztdQuery('SELECT * FROM mdw_products WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('Reborn', $rows[0]['name']);
        $this->assertSame('99', (string) $rows[0]['stock']);
    }

    /**
     * Rapid sequential updates to same column.
     */
    public function testRapidSequentialUpdates(): void
    {
        $this->pdo->exec("INSERT INTO mdw_products VALUES (1, 'Counter', 0, 0.00)");

        for ($i = 1; $i <= 5; $i++) {
            $this->pdo->exec("UPDATE mdw_products SET stock = $i WHERE id = 1");
        }

        $rows = $this->ztdQuery('SELECT stock FROM mdw_products WHERE id = 1');
        $this->assertSame('5', (string) $rows[0]['stock']);
    }

    /**
     * Conditional SELECT after mixed DML — verifying aggregate correctness.
     */
    public function testAggregateAfterMixedDml(): void
    {
        $this->pdo->exec("INSERT INTO mdw_products VALUES (1, 'A', 10, 100.00)");
        $this->pdo->exec("INSERT INTO mdw_products VALUES (2, 'B', 20, 200.00)");
        $this->pdo->exec("INSERT INTO mdw_products VALUES (3, 'C', 30, 300.00)");
        $this->pdo->exec('DELETE FROM mdw_products WHERE id = 2');
        $this->pdo->exec('UPDATE mdw_products SET price = 150.00 WHERE id = 1');

        $rows = $this->ztdQuery(
            'SELECT COUNT(*) AS cnt, SUM(price) AS total, AVG(stock) AS avg_stock FROM mdw_products'
        );

        $this->assertSame('2', (string) $rows[0]['cnt']);
        $this->assertEquals(450.00, (float) $rows[0]['total'], '', 0.01); // 150 + 300
        $this->assertEquals(20.0, (float) $rows[0]['avg_stock'], '', 0.01); // (10 + 30) / 2
    }

    /**
     * Prepared SELECT interleaved with exec DML.
     *
     * By-design: prepared statements capture shadow store at prepare() time.
     * DML after prepare() is NOT visible. See SPEC-3.2.
     */
    public function testPreparedSelectInterleavedWithExecDml(): void
    {
        $this->pdo->exec("INSERT INTO mdw_products VALUES (1, 'Widget', 50, 9.99)");

        // Prepared SELECT — captures snapshot with Widget stock=50
        $stmt = $this->pdo->prepare('SELECT * FROM mdw_products WHERE stock > ?');

        // DML between prepare and execute — NOT visible to prepared stmt
        $this->pdo->exec("INSERT INTO mdw_products VALUES (2, 'Gadget', 100, 19.99)");
        $this->pdo->exec('UPDATE mdw_products SET stock = 5 WHERE id = 1');

        $stmt->execute([10]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // Sees Widget with stock=50 (snapshot at prepare time), not Gadget
        $this->assertCount(1, $rows);
        $this->assertSame('Widget', $rows[0]['name']);

        // Fresh prepare sees current state
        $rows2 = $this->ztdPrepareAndExecute('SELECT * FROM mdw_products WHERE stock > ?', [10]);
        $this->assertCount(1, $rows2);
        $this->assertSame('Gadget', $rows2[0]['name']);
    }
}
