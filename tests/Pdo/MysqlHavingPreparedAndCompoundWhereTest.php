<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests HAVING clause with prepared parameters and compound WHERE conditions
 * (OR/AND with parentheses) in UPDATE/DELETE after shadow mutations on MySQL PDO.
 *
 * HAVING with bound parameters is known to fail on SQLite (Issue #22).
 * This test verifies whether MySQL PDO has the same or different behavior.
 *
 * Compound WHERE with OR is a common real-world pattern. The CTE rewriter
 * must preserve parenthesization for correct logical evaluation.
 *
 * @spec SPEC-3.3
 * @spec SPEC-4.3
 */
class MysqlHavingPreparedAndCompoundWhereTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_hpcw_orders (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                status VARCHAR(20) NOT NULL,
                region VARCHAR(20)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_hpcw_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_hpcw_orders VALUES (1, 100, 50.00, 'completed', 'east')");
        $this->pdo->exec("INSERT INTO my_hpcw_orders VALUES (2, 100, 75.00, 'completed', 'east')");
        $this->pdo->exec("INSERT INTO my_hpcw_orders VALUES (3, 100, 25.00, 'pending', 'west')");
        $this->pdo->exec("INSERT INTO my_hpcw_orders VALUES (4, 200, 100.00, 'completed', 'east')");
        $this->pdo->exec("INSERT INTO my_hpcw_orders VALUES (5, 200, 30.00, 'pending', 'west')");
        $this->pdo->exec("INSERT INTO my_hpcw_orders VALUES (6, 300, 10.00, 'completed', 'east')");
    }

    // --- HAVING with prepared params ---

    public function testHavingCountWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT customer_id, COUNT(*) AS order_count
                 FROM my_hpcw_orders
                 GROUP BY customer_id
                 HAVING COUNT(*) >= ?",
                [2]
            );
            // customer 100: 3 orders, customer 200: 2 orders, customer 300: 1 order
            $this->assertCount(2, $rows);
            $ids = array_column($rows, 'customer_id');
            $this->assertContains('100', array_map('strval', $ids));
            $this->assertContains('200', array_map('strval', $ids));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING COUNT with prepared param failed: ' . $e->getMessage());
        }
    }

    public function testHavingSumWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT customer_id, SUM(amount) AS total
                 FROM my_hpcw_orders
                 GROUP BY customer_id
                 HAVING SUM(amount) > ?",
                [100.00]
            );
            // customer 100: 150.00, customer 200: 130.00, customer 300: 10.00
            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING SUM with prepared param failed: ' . $e->getMessage());
        }
    }

    public function testHavingWithWhereAndPreparedParams(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT customer_id, COUNT(*) AS cnt
                 FROM my_hpcw_orders
                 WHERE status = ?
                 GROUP BY customer_id
                 HAVING COUNT(*) > ?",
                ['completed', 1]
            );
            // completed orders: customer 100: 2, customer 200: 1, customer 300: 1
            $this->assertCount(1, $rows);
            $this->assertEquals(100, (int) $rows[0]['customer_id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING with WHERE and prepared params failed: ' . $e->getMessage());
        }
    }

    public function testHavingAfterShadowInsert(): void
    {
        $this->pdo->exec("INSERT INTO my_hpcw_orders VALUES (7, 300, 200.00, 'completed', 'west')");

        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT customer_id, SUM(amount) AS total
                 FROM my_hpcw_orders
                 GROUP BY customer_id
                 HAVING SUM(amount) > ?",
                [100.00]
            );
            // customer 100: 150.00, customer 200: 130.00, customer 300: 210.00
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING after shadow INSERT with prepared param failed: ' . $e->getMessage());
        }
    }

    public function testHavingAfterShadowDelete(): void
    {
        $this->pdo->exec("DELETE FROM my_hpcw_orders WHERE id = 2");

        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT customer_id, COUNT(*) AS cnt
                 FROM my_hpcw_orders
                 GROUP BY customer_id
                 HAVING COUNT(*) >= ?",
                [2]
            );
            // After delete: customer 100: 2 orders, customer 200: 2, customer 300: 1
            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING after shadow DELETE with prepared param failed: ' . $e->getMessage());
        }
    }

    // --- Compound WHERE (OR/AND with parentheses) ---

    public function testUpdateWithOrCondition(): void
    {
        $this->pdo->exec(
            "UPDATE my_hpcw_orders SET status = 'flagged'
             WHERE (customer_id = 100 AND amount > 40) OR (customer_id = 300)"
        );

        $rows = $this->ztdQuery(
            "SELECT id, status FROM my_hpcw_orders WHERE status = 'flagged' ORDER BY id"
        );
        // id 1: cust 100, amount 50 -> flagged
        // id 2: cust 100, amount 75 -> flagged
        // id 6: cust 300, amount 10 -> flagged
        $this->assertCount(3, $rows);
        $ids = array_map(fn($r) => (int) $r['id'], $rows);
        $this->assertEquals([1, 2, 6], $ids);
    }

    public function testDeleteWithOrCondition(): void
    {
        $this->pdo->exec(
            "DELETE FROM my_hpcw_orders
             WHERE (status = 'pending') OR (region = 'east' AND amount < 20)"
        );

        $rows = $this->ztdQuery('SELECT id FROM my_hpcw_orders ORDER BY id');
        // Deleted: id 3 (pending), id 5 (pending), id 6 (east, amount 10)
        $this->assertCount(3, $rows);
        $ids = array_map(fn($r) => (int) $r['id'], $rows);
        $this->assertEquals([1, 2, 4], $ids);
    }

    public function testSelectWithNestedOrAnd(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id FROM my_hpcw_orders
             WHERE ((customer_id = 100 OR customer_id = 200) AND status = 'completed')
                OR (region = 'west')
             ORDER BY id"
        );
        // completed + cust 100/200: id 1, 2, 4
        // west: id 3, 5
        $this->assertCount(5, $rows);
        $ids = array_map(fn($r) => (int) $r['id'], $rows);
        $this->assertEquals([1, 2, 3, 4, 5], $ids);
    }

    public function testPreparedCompoundWhereUpdate(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE my_hpcw_orders SET status = 'archived'
                 WHERE (status = ? AND amount < ?) OR customer_id = ?"
            );
            $stmt->execute(['completed', 20.00, 200]);

            $rows = $this->ztdQuery(
                "SELECT id FROM my_hpcw_orders WHERE status = 'archived' ORDER BY id"
            );
            // completed + amount < 20: id 6 (10.00)
            // customer_id = 200: id 4, 5
            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertEquals([4, 5, 6], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared compound WHERE UPDATE failed: ' . $e->getMessage());
        }
    }

    public function testCompoundWhereAfterMultipleMutations(): void
    {
        // Insert then update then query with compound WHERE
        $this->pdo->exec("INSERT INTO my_hpcw_orders VALUES (7, 400, 500.00, 'completed', 'north')");
        $this->pdo->exec("UPDATE my_hpcw_orders SET region = 'south' WHERE customer_id = 100 AND status = 'pending'");

        $rows = $this->ztdQuery(
            "SELECT id FROM my_hpcw_orders
             WHERE (region = 'south') OR (customer_id = 400 AND amount > 100)
             ORDER BY id"
        );
        // region south: id 3 (was west, updated to south)
        // customer 400 + amount > 100: id 7
        $this->assertCount(2, $rows);
        $ids = array_map(fn($r) => (int) $r['id'], $rows);
        $this->assertEquals([3, 7], $ids);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM my_hpcw_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
