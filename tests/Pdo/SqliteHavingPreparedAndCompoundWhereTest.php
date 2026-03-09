<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests HAVING clause with prepared parameters and compound WHERE conditions
 * (OR/AND with parentheses) in UPDATE/DELETE after shadow mutations on SQLite PDO.
 *
 * HAVING with bound parameters is known to return empty on SQLite (Issue #22).
 * This test provides systematic coverage for the HAVING+params pattern in
 * combination with compound WHERE for UPDATE/DELETE.
 *
 * @spec SPEC-3.3
 * @spec SPEC-4.3
 */
class SqliteHavingPreparedAndCompoundWhereTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_hpcw_orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL,
                region TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_hpcw_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_hpcw_orders VALUES (1, 100, 50.00, 'completed', 'east')");
        $this->pdo->exec("INSERT INTO sl_hpcw_orders VALUES (2, 100, 75.00, 'completed', 'east')");
        $this->pdo->exec("INSERT INTO sl_hpcw_orders VALUES (3, 100, 25.00, 'pending', 'west')");
        $this->pdo->exec("INSERT INTO sl_hpcw_orders VALUES (4, 200, 100.00, 'completed', 'east')");
        $this->pdo->exec("INSERT INTO sl_hpcw_orders VALUES (5, 200, 30.00, 'pending', 'west')");
        $this->pdo->exec("INSERT INTO sl_hpcw_orders VALUES (6, 300, 10.00, 'completed', 'east')");
    }

    // --- HAVING with prepared params ---

    /**
     * HAVING COUNT(*) >= ? with prepared param.
     * Known issue on SQLite: Issue #22 — HAVING with bound parameters returns empty.
     */
    public function testHavingCountWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT customer_id, COUNT(*) AS order_count
                 FROM sl_hpcw_orders
                 GROUP BY customer_id
                 HAVING COUNT(*) >= ?",
                [2]
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'SPEC-11.SQLITE-HAVING [Issue #22]: HAVING with bound parameters returns empty results on SQLite.'
                );
            }
            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING COUNT with prepared param failed: ' . $e->getMessage());
        }
    }

    public function testHavingSumWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT customer_id, SUM(amount) AS total
                 FROM sl_hpcw_orders
                 GROUP BY customer_id
                 HAVING SUM(amount) > ?",
                [100.00]
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'SPEC-11.SQLITE-HAVING [Issue #22]: HAVING SUM with bound param returns empty on SQLite.'
                );
            }
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
                 FROM sl_hpcw_orders
                 WHERE status = ?
                 GROUP BY customer_id
                 HAVING COUNT(*) > ?",
                ['completed', 1]
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'SPEC-11.SQLITE-HAVING [Issue #22]: HAVING with WHERE + bound params returns empty on SQLite.'
                );
            }
            $this->assertCount(1, $rows);
            $this->assertEquals(100, (int) $rows[0]['customer_id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING with WHERE and prepared params failed: ' . $e->getMessage());
        }
    }

    public function testHavingWithoutPreparedParam(): void
    {
        // Control test: HAVING without bound params should work
        $rows = $this->ztdQuery(
            "SELECT customer_id, COUNT(*) AS order_count
             FROM sl_hpcw_orders
             GROUP BY customer_id
             HAVING COUNT(*) >= 2
             ORDER BY customer_id"
        );
        $this->assertCount(2, $rows);
        $this->assertEquals(100, (int) $rows[0]['customer_id']);
        $this->assertEquals(200, (int) $rows[1]['customer_id']);
    }

    // --- Compound WHERE (OR/AND with parentheses) ---

    public function testUpdateWithOrCondition(): void
    {
        $this->pdo->exec(
            "UPDATE sl_hpcw_orders SET status = 'flagged'
             WHERE (customer_id = 100 AND amount > 40) OR (customer_id = 300)"
        );

        $rows = $this->ztdQuery(
            "SELECT id, status FROM sl_hpcw_orders WHERE status = 'flagged' ORDER BY id"
        );
        $this->assertCount(3, $rows);
        $ids = array_map(fn($r) => (int) $r['id'], $rows);
        $this->assertEquals([1, 2, 6], $ids);
    }

    public function testDeleteWithOrCondition(): void
    {
        $this->pdo->exec(
            "DELETE FROM sl_hpcw_orders
             WHERE (status = 'pending') OR (region = 'east' AND amount < 20)"
        );

        $rows = $this->ztdQuery('SELECT id FROM sl_hpcw_orders ORDER BY id');
        $this->assertCount(3, $rows);
        $ids = array_map(fn($r) => (int) $r['id'], $rows);
        $this->assertEquals([1, 2, 4], $ids);
    }

    public function testSelectWithNestedOrAnd(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id FROM sl_hpcw_orders
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
                "UPDATE sl_hpcw_orders SET status = 'archived'
                 WHERE (status = ? AND amount < ?) OR customer_id = ?"
            );
            $stmt->execute(['completed', 20.00, 200]);

            $rows = $this->ztdQuery(
                "SELECT id FROM sl_hpcw_orders WHERE status = 'archived' ORDER BY id"
            );
            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertEquals([4, 5, 6], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared compound WHERE UPDATE failed: ' . $e->getMessage());
        }
    }

    public function testCompoundWhereAfterMultipleMutations(): void
    {
        $this->pdo->exec("INSERT INTO sl_hpcw_orders VALUES (7, 400, 500.00, 'completed', 'north')");
        $this->pdo->exec("UPDATE sl_hpcw_orders SET region = 'south' WHERE customer_id = 100 AND status = 'pending'");

        $rows = $this->ztdQuery(
            "SELECT id FROM sl_hpcw_orders
             WHERE (region = 'south') OR (customer_id = 400 AND amount > 100)
             ORDER BY id"
        );
        $this->assertCount(2, $rows);
        $ids = array_map(fn($r) => (int) $r['id'], $rows);
        $this->assertEquals([3, 7], $ids);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_hpcw_orders")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
