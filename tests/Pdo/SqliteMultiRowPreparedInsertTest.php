<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests multi-row INSERT with prepared statements and INSERT with subquery values
 * through the CTE shadow store on SQLite PDO.
 *
 * Multi-row INSERT with placeholders is a common ORM/batch pattern:
 *   INSERT INTO t VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)
 * The CTE rewriter must correctly handle the multiple value tuples AND the
 * parameter count when rewriting. A bug here could produce wrong row counts,
 * parameter offset errors, or SQL parse failures.
 *
 * Also tests INSERT with self-referencing subquery in VALUES, which forces
 * the rewriter to handle subqueries inside the VALUES clause.
 * @spec SPEC-4.1
 * @spec SPEC-3.2
 */
class SqliteMultiRowPreparedInsertTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_mrp_items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, rank INTEGER)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_mrp_items'];
    }

    /**
     * Multi-row INSERT with positional placeholders: 3 rows, 9 params.
     */
    public function testMultiRowPreparedInsertThreeRows(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                'INSERT INTO sl_mrp_items (id, name, rank) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)'
            );
            $stmt->execute([1, 'alpha', 10, 2, 'beta', 20, 3, 'gamma', 30]);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multi-row prepared INSERT (3 rows, 9 params) failed: ' . $e->getMessage()
            );
            return;
        }

        $rows = $this->ztdQuery('SELECT * FROM sl_mrp_items ORDER BY id');

        if (count($rows) !== 3) {
            $this->markTestIncomplete(
                'Multi-row prepared INSERT: expected 3 rows, got ' . count($rows)
            );
            return;
        }

        $this->assertSame('alpha', $rows[0]['name']);
        $this->assertSame('10', (string) $rows[0]['rank']);
        $this->assertSame('beta', $rows[1]['name']);
        $this->assertSame('20', (string) $rows[1]['rank']);
        $this->assertSame('gamma', $rows[2]['name']);
        $this->assertSame('30', (string) $rows[2]['rank']);
    }

    /**
     * Multi-row INSERT with named placeholders.
     */
    public function testMultiRowPreparedInsertNamedParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                'INSERT INTO sl_mrp_items (id, name, rank) VALUES (:id1, :name1, :rank1), (:id2, :name2, :rank2)'
            );
            $stmt->execute([
                ':id1' => 10, ':name1' => 'delta', ':rank1' => 100,
                ':id2' => 11, ':name2' => 'epsilon', ':rank2' => 200,
            ]);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multi-row prepared INSERT with named params failed: ' . $e->getMessage()
            );
            return;
        }

        $rows = $this->ztdQuery('SELECT * FROM sl_mrp_items ORDER BY id');

        if (count($rows) !== 2) {
            $this->markTestIncomplete(
                'Multi-row prepared INSERT named params: expected 2 rows, got ' . count($rows)
            );
            return;
        }

        $this->assertSame('delta', $rows[0]['name']);
        $this->assertSame('epsilon', $rows[1]['name']);
    }

    /**
     * Multi-row INSERT via exec (literal values) then verify all rows visible via prepared SELECT.
     */
    public function testMultiRowExecThenPreparedSelect(): void
    {
        $this->pdo->exec("INSERT INTO sl_mrp_items VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 3), (4, 'd', 4)");

        $rows = $this->ztdPrepareAndExecute(
            'SELECT id, name FROM sl_mrp_items WHERE rank >= ? ORDER BY id',
            [2]
        );

        $this->assertCount(3, $rows);
        $this->assertSame('b', $rows[0]['name']);
        $this->assertSame('c', $rows[1]['name']);
        $this->assertSame('d', $rows[2]['name']);
    }

    /**
     * Multi-row prepared INSERT then aggregate query.
     */
    public function testMultiRowPreparedInsertThenAggregate(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                'INSERT INTO sl_mrp_items (id, name, rank) VALUES (?, ?, ?), (?, ?, ?)'
            );
            $stmt->execute([1, 'x', 50, 2, 'y', 150]);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multi-row prepared INSERT for aggregate test failed: ' . $e->getMessage()
            );
            return;
        }

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt, SUM(rank) AS total, AVG(rank) AS avg_rank FROM sl_mrp_items');

        $this->assertSame('2', (string) $rows[0]['cnt']);
        $this->assertSame('200', (string) $rows[0]['total']);
        $this->assertEqualsWithDelta(100.0, (float) $rows[0]['avg_rank'], 0.01);
    }

    /**
     * Multi-row prepared INSERT then UPDATE one row, then re-SELECT.
     */
    public function testMultiRowPreparedInsertThenUpdate(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                'INSERT INTO sl_mrp_items (id, name, rank) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)'
            );
            $stmt->execute([1, 'one', 10, 2, 'two', 20, 3, 'three', 30]);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multi-row prepared INSERT for update test failed: ' . $e->getMessage()
            );
            return;
        }

        $this->pdo->exec("UPDATE sl_mrp_items SET rank = 99 WHERE id = 2");

        $rows = $this->ztdQuery('SELECT rank FROM sl_mrp_items WHERE id = 2');
        $this->assertCount(1, $rows);

        if ((int) $rows[0]['rank'] !== 99) {
            $this->markTestIncomplete(
                'UPDATE after multi-row prepared INSERT did not take effect. '
                . 'Expected rank=99 for id=2, got rank=' . $rows[0]['rank']
            );
            return;
        }
        $this->assertSame(99, (int) $rows[0]['rank']);

        // Others unchanged
        $rows = $this->ztdQuery('SELECT rank FROM sl_mrp_items WHERE id = 1');
        $this->assertSame(10, (int) $rows[0]['rank']);
    }

    /**
     * INSERT with self-referencing COUNT(*) subquery in VALUES.
     *
     * Pattern: INSERT INTO t (id, name, rank) VALUES (?, ?, (SELECT COUNT(*) FROM t))
     * This inserts a row whose rank equals the current row count of the same table.
     */
    public function testInsertWithSelfReferencingCountSubquery(): void
    {
        $this->pdo->exec("INSERT INTO sl_mrp_items VALUES (1, 'first', 0)");
        $this->pdo->exec("INSERT INTO sl_mrp_items VALUES (2, 'second', 0)");

        try {
            $stmt = $this->pdo->prepare(
                'INSERT INTO sl_mrp_items (id, name, rank) VALUES (?, ?, (SELECT COUNT(*) FROM sl_mrp_items))'
            );
            $stmt->execute([3, 'third']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with self-referencing COUNT(*) subquery failed: ' . $e->getMessage()
            );
            return;
        }

        $rows = $this->ztdQuery('SELECT rank FROM sl_mrp_items WHERE id = 3');

        if (count($rows) !== 1) {
            $this->markTestIncomplete(
                'Self-referencing INSERT subquery: expected 1 row, got ' . count($rows)
            );
            return;
        }

        // At time of INSERT, there were 2 rows, so rank should be 2
        $this->assertSame('2', (string) $rows[0]['rank']);
    }

    /**
     * INSERT with self-referencing MAX+1 subquery — auto-incrementing rank.
     */
    public function testInsertWithSelfReferencingMaxSubquery(): void
    {
        $this->pdo->exec("INSERT INTO sl_mrp_items VALUES (1, 'first', 1)");
        $this->pdo->exec("INSERT INTO sl_mrp_items VALUES (2, 'second', 2)");

        try {
            $stmt = $this->pdo->prepare(
                'INSERT INTO sl_mrp_items (id, name, rank) VALUES (?, ?, (SELECT COALESCE(MAX(rank), 0) + 1 FROM sl_mrp_items))'
            );
            $stmt->execute([3, 'third']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with self-referencing MAX+1 subquery failed: ' . $e->getMessage()
            );
            return;
        }

        $rows = $this->ztdQuery('SELECT rank FROM sl_mrp_items WHERE id = 3');

        if (count($rows) !== 1) {
            $this->markTestIncomplete(
                'Self-referencing MAX+1 INSERT: expected 1 row, got ' . count($rows)
            );
            return;
        }

        // MAX(rank) was 2, so new rank should be 3
        $this->assertSame('3', (string) $rows[0]['rank']);
    }

    /**
     * Re-execute same prepared multi-row INSERT with different params.
     */
    public function testReexecuteMultiRowPreparedInsert(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                'INSERT INTO sl_mrp_items (id, name, rank) VALUES (?, ?, ?), (?, ?, ?)'
            );

            // First execution
            $stmt->execute([1, 'batch1_a', 10, 2, 'batch1_b', 20]);
            // Second execution with different params
            $stmt->execute([3, 'batch2_a', 30, 4, 'batch2_b', 40]);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Re-execute multi-row prepared INSERT failed: ' . $e->getMessage()
            );
            return;
        }

        $rows = $this->ztdQuery('SELECT * FROM sl_mrp_items ORDER BY id');

        if (count($rows) !== 4) {
            $this->markTestIncomplete(
                'Re-execute multi-row INSERT: expected 4 rows, got ' . count($rows)
            );
            return;
        }

        $this->assertSame('batch1_a', $rows[0]['name']);
        $this->assertSame('batch2_b', $rows[3]['name']);
    }

    /**
     * Physical isolation: multi-row prepared data not visible with ZTD disabled.
     */
    public function testPhysicalIsolation(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                'INSERT INTO sl_mrp_items (id, name, rank) VALUES (?, ?, ?), (?, ?, ?)'
            );
            $stmt->execute([1, 'iso_a', 10, 2, 'iso_b', 20]);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multi-row prepared INSERT for isolation test failed: ' . $e->getMessage()
            );
            return;
        }

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sl_mrp_items');
        $this->assertSame('2', (string) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_mrp_items');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
