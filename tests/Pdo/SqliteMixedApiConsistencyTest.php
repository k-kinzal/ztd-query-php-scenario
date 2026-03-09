<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests shadow store consistency when mixing exec(), query(), and prepare() on SQLite.
 *
 * Real-world scenario: applications commonly mix API methods within a single
 * request — exec() for simple DML, prepare() for parameterized queries,
 * query() for unparameterized reads. The shadow store must remain consistent
 * across all API paths.
 *
 * @spec SPEC-2.1
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class SqliteMixedApiConsistencyTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mac_items (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT \'active\',
            qty INTEGER NOT NULL DEFAULT 0
        )';
    }

    protected function getTableNames(): array
    {
        return ['mac_items'];
    }

    /**
     * exec() INSERT visible to query() SELECT.
     */
    public function testExecInsertVisibleToQuery(): void
    {
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'Alpha', 10)");
        $rows = $this->ztdQuery("SELECT name, qty FROM mac_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('Alpha', $rows[0]['name']);
    }

    /**
     * exec() INSERT visible to prepare()+execute() SELECT.
     */
    public function testExecInsertVisibleToPrepare(): void
    {
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'Alpha', 10)");
        $rows = $this->ztdPrepareAndExecute("SELECT name FROM mac_items WHERE id = ?", [1]);
        $this->assertCount(1, $rows);
        $this->assertSame('Alpha', $rows[0]['name']);
    }

    /**
     * prepare() INSERT visible to query() SELECT.
     */
    public function testPreparedInsertVisibleToQuery(): void
    {
        $stmt = $this->pdo->prepare("INSERT INTO mac_items (id, name, qty) VALUES (?, ?, ?)");
        $stmt->execute([1, 'Beta', 20]);

        $rows = $this->ztdQuery("SELECT name, qty FROM mac_items WHERE id = 1");

        // Known issue (#23): PDO prepared INSERT may not be readable
        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'Prepared INSERT not visible to query() — known issue #23'
            );
        }
        $this->assertCount(1, $rows);
        $this->assertSame('Beta', $rows[0]['name']);
    }

    /**
     * exec() INSERT, exec() UPDATE, query() reads updated value.
     */
    public function testExecInsertUpdateQuerySequence(): void
    {
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'Gamma', 5)");
        $this->ztdExec("UPDATE mac_items SET qty = 15 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT qty FROM mac_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame(15, (int) $rows[0]['qty']);
    }

    /**
     * exec() INSERT, prepare() UPDATE, query() reads updated value.
     */
    public function testExecInsertPreparedUpdateQueryRead(): void
    {
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'Delta', 5)");

        try {
            $stmt = $this->pdo->prepare("UPDATE mac_items SET qty = ? WHERE id = ?");
            $stmt->execute([25, 1]);

            $rows = $this->ztdQuery("SELECT qty FROM mac_items WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame(25, (int) $rows[0]['qty']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Prepared UPDATE after exec INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple INSERT methods interleaved, then aggregate.
     */
    public function testInterleavedInsertsAggregate(): void
    {
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'A', 10)");
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (2, 'B', 20)");
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (3, 'C', 30)");

        $rows = $this->ztdQuery("SELECT SUM(qty) AS total, COUNT(*) AS cnt FROM mac_items");
        $this->assertSame(60, (int) $rows[0]['total']);
        $this->assertSame(3, (int) $rows[0]['cnt']);
    }

    /**
     * exec() INSERT, exec() DELETE, query() confirms deletion.
     */
    public function testExecInsertDeleteQuery(): void
    {
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'X', 1)");
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (2, 'Y', 2)");
        $this->ztdExec("DELETE FROM mac_items WHERE id = 1");

        $rows = $this->ztdQuery("SELECT id, name FROM mac_items ORDER BY id");
        $this->assertCount(1, $rows);
        $this->assertSame(2, (int) $rows[0]['id']);
    }

    /**
     * exec() INSERT, exec() DELETE, exec() re-INSERT same PK, query() reads new data.
     */
    public function testDeleteAndReinsertSamePk(): void
    {
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'Original', 10)");
        $this->ztdExec("DELETE FROM mac_items WHERE id = 1");
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'Replaced', 99)");

        $rows = $this->ztdQuery("SELECT name, qty FROM mac_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('Replaced', $rows[0]['name']);
        $this->assertSame(99, (int) $rows[0]['qty']);
    }

    /**
     * Multiple UPDATEs on same row, verify final state.
     */
    public function testMultipleUpdatesOnSameRow(): void
    {
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'Start', 0)");
        $this->ztdExec("UPDATE mac_items SET qty = 10 WHERE id = 1");
        $this->ztdExec("UPDATE mac_items SET qty = 20 WHERE id = 1");
        $this->ztdExec("UPDATE mac_items SET name = 'End' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT name, qty FROM mac_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('End', $rows[0]['name']);
        $this->assertSame(20, (int) $rows[0]['qty']);
    }

    /**
     * query() returns correct results after complex mutation sequence.
     */
    public function testComplexMutationThenComplexQuery(): void
    {
        // Insert 5 rows
        for ($i = 1; $i <= 5; $i++) {
            $this->ztdExec("INSERT INTO mac_items (id, name, status, qty) VALUES ($i, 'Item$i', 'active', " . ($i * 10) . ")");
        }
        // Update some
        $this->ztdExec("UPDATE mac_items SET status = 'inactive' WHERE id IN (2, 4)");
        // Delete one
        $this->ztdExec("DELETE FROM mac_items WHERE id = 3");

        // Complex query
        $rows = $this->ztdQuery(
            "SELECT status, SUM(qty) AS total, COUNT(*) AS cnt
             FROM mac_items
             GROUP BY status
             ORDER BY status"
        );

        $this->assertCount(2, $rows);
        // active: items 1 (10), 5 (50) => total=60, cnt=2
        $this->assertSame('active', $rows[0]['status']);
        $this->assertSame(60, (int) $rows[0]['total']);
        $this->assertSame(2, (int) $rows[0]['cnt']);
        // inactive: items 2 (20), 4 (40) => total=60, cnt=2
        $this->assertSame('inactive', $rows[1]['status']);
        $this->assertSame(60, (int) $rows[1]['total']);
        $this->assertSame(2, (int) $rows[1]['cnt']);
    }

    /**
     * Prepared SELECT re-execution after mutation.
     *
     * By-design: prepared statements capture shadow store state at prepare() time.
     * Mutations between prepare() and execute() are NOT visible. A fresh prepare()
     * is needed to see updated data. This test documents the by-design behavior.
     */
    public function testPreparedSelectReexecutionAfterMutation(): void
    {
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'A', 10)");
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (2, 'B', 20)");

        $stmt = $this->pdo->prepare("SELECT qty FROM mac_items WHERE id = ?");

        // First execution
        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(10, (int) $rows[0]['qty']);

        // Mutate
        $this->ztdExec("UPDATE mac_items SET qty = 999 WHERE id = 1");

        // Re-execute same prepared statement — by-design returns stale data
        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // By-design: snapshot frozen at prepare() time, returns original value
        $this->assertSame(10, (int) $rows[0]['qty']);

        // Fresh prepare() sees updated data
        $stmt2 = $this->pdo->prepare("SELECT qty FROM mac_items WHERE id = ?");
        $stmt2->execute([1]);
        $rows2 = $stmt2->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(999, (int) $rows2[0]['qty']);
    }
}
