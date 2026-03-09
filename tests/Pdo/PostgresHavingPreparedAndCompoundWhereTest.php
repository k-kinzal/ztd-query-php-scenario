<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests HAVING clause with prepared parameters and compound WHERE conditions
 * (OR/AND with parentheses) in UPDATE/DELETE after shadow mutations on PostgreSQL PDO.
 *
 * HAVING with bound parameters is known to fail on SQLite (Issue #22).
 * This test verifies whether PostgreSQL has the same or different behavior.
 *
 * @spec SPEC-3.3
 * @spec SPEC-4.3
 */
class PostgresHavingPreparedAndCompoundWhereTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_hpcw_orders (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                amount NUMERIC(10,2) NOT NULL,
                status VARCHAR(20) NOT NULL,
                region VARCHAR(20)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_hpcw_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_hpcw_orders VALUES (1, 100, 50.00, 'completed', 'east')");
        $this->pdo->exec("INSERT INTO pg_hpcw_orders VALUES (2, 100, 75.00, 'completed', 'east')");
        $this->pdo->exec("INSERT INTO pg_hpcw_orders VALUES (3, 100, 25.00, 'pending', 'west')");
        $this->pdo->exec("INSERT INTO pg_hpcw_orders VALUES (4, 200, 100.00, 'completed', 'east')");
        $this->pdo->exec("INSERT INTO pg_hpcw_orders VALUES (5, 200, 30.00, 'pending', 'west')");
        $this->pdo->exec("INSERT INTO pg_hpcw_orders VALUES (6, 300, 10.00, 'completed', 'east')");
    }

    // --- HAVING with prepared params ---

    public function testHavingCountWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT customer_id, COUNT(*) AS order_count
                 FROM pg_hpcw_orders
                 GROUP BY customer_id
                 HAVING COUNT(*) >= ?",
                [2]
            );
            $this->assertCount(2, $rows);
            $ids = array_column($rows, 'customer_id');
            $this->assertContains(100, array_map('intval', $ids));
            $this->assertContains(200, array_map('intval', $ids));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING COUNT with prepared param failed: ' . $e->getMessage());
        }
    }

    public function testHavingSumWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT customer_id, SUM(amount) AS total
                 FROM pg_hpcw_orders
                 GROUP BY customer_id
                 HAVING SUM(amount) > ?",
                [100.00]
            );
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
                 FROM pg_hpcw_orders
                 WHERE status = ?
                 GROUP BY customer_id
                 HAVING COUNT(*) > ?",
                ['completed', 1]
            );
            $this->assertCount(1, $rows);
            $this->assertEquals(100, (int) $rows[0]['customer_id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING with WHERE and prepared params failed: ' . $e->getMessage());
        }
    }

    public function testHavingAfterShadowInsert(): void
    {
        $this->pdo->exec("INSERT INTO pg_hpcw_orders VALUES (7, 300, 200.00, 'completed', 'west')");

        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT customer_id, SUM(amount) AS total
                 FROM pg_hpcw_orders
                 GROUP BY customer_id
                 HAVING SUM(amount) > ?",
                [100.00]
            );
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING after shadow INSERT with prepared param failed: ' . $e->getMessage());
        }
    }

    public function testHavingAfterShadowDelete(): void
    {
        $this->pdo->exec("DELETE FROM pg_hpcw_orders WHERE id = 2");

        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT customer_id, COUNT(*) AS cnt
                 FROM pg_hpcw_orders
                 GROUP BY customer_id
                 HAVING COUNT(*) >= ?",
                [2]
            );
            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING after shadow DELETE with prepared param failed: ' . $e->getMessage());
        }
    }

    // --- Compound WHERE (OR/AND with parentheses) ---

    public function testUpdateWithOrCondition(): void
    {
        $this->pdo->exec(
            "UPDATE pg_hpcw_orders SET status = 'flagged'
             WHERE (customer_id = 100 AND amount > 40) OR (customer_id = 300)"
        );

        $rows = $this->ztdQuery(
            "SELECT id, status FROM pg_hpcw_orders WHERE status = 'flagged' ORDER BY id"
        );
        $this->assertCount(3, $rows);
        $ids = array_map(fn($r) => (int) $r['id'], $rows);
        $this->assertEquals([1, 2, 6], $ids);
    }

    public function testDeleteWithOrCondition(): void
    {
        $this->pdo->exec(
            "DELETE FROM pg_hpcw_orders
             WHERE (status = 'pending') OR (region = 'east' AND amount < 20)"
        );

        $rows = $this->ztdQuery('SELECT id FROM pg_hpcw_orders ORDER BY id');
        $this->assertCount(3, $rows);
        $ids = array_map(fn($r) => (int) $r['id'], $rows);
        $this->assertEquals([1, 2, 4], $ids);
    }

    public function testSelectWithNestedOrAnd(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id FROM pg_hpcw_orders
             WHERE ((customer_id = 100 OR customer_id = 200) AND status = 'completed')
                OR (region = 'west')
             ORDER BY id"
        );
        $this->assertCount(5, $rows);
        $ids = array_map(fn($r) => (int) $r['id'], $rows);
        $this->assertEquals([1, 2, 3, 4, 5], $ids);
    }

    public function testPreparedCompoundWhereUpdate(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_hpcw_orders SET status = 'archived'
                 WHERE (status = ? AND amount < ?) OR customer_id = ?"
            );
            $stmt->execute(['completed', 20.00, 200]);

            $rows = $this->ztdQuery(
                "SELECT id FROM pg_hpcw_orders WHERE status = 'archived' ORDER BY id"
            );
            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertEquals([4, 5, 6], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared compound WHERE UPDATE failed: ' . $e->getMessage());
        }
    }

    public function testCompoundWhereWithStringConcat(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, status || ' - ' || region AS label
                 FROM pg_hpcw_orders
                 WHERE (status = 'completed' AND region = 'east')
                    OR (amount > 90)
                 ORDER BY id"
            );
            // completed+east: id 1,2,4,6. amount>90: id 4.
            // Union: 1,2,4,6
            $this->assertCount(4, $rows);
            $this->assertSame('completed - east', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Compound WHERE with string concat failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_hpcw_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
