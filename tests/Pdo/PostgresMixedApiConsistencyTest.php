<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests shadow store consistency when mixing exec(), query(), and prepare() on PostgreSQL-PDO.
 *
 * @spec SPEC-2.1
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class PostgresMixedApiConsistencyTest extends AbstractPostgresPdoTestCase
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

    public function testExecInsertVisibleToQuery(): void
    {
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'Alpha', 10)");
        $rows = $this->ztdQuery("SELECT name, qty FROM mac_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('Alpha', $rows[0]['name']);
    }

    public function testExecInsertVisibleToPrepare(): void
    {
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'Alpha', 10)");
        $rows = $this->ztdPrepareAndExecute("SELECT name FROM mac_items WHERE id = ?", [1]);
        $this->assertCount(1, $rows);
        $this->assertSame('Alpha', $rows[0]['name']);
    }

    public function testExecInsertUpdateQuerySequence(): void
    {
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'Gamma', 5)");
        $this->ztdExec("UPDATE mac_items SET qty = 15 WHERE id = 1");
        $rows = $this->ztdQuery("SELECT qty FROM mac_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame(15, (int) $rows[0]['qty']);
    }

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

    public function testMultipleUpdatesOnSameRow(): void
    {
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'Start', 0)");
        $this->ztdExec("UPDATE mac_items SET qty = 10 WHERE id = 1");
        $this->ztdExec("UPDATE mac_items SET qty = 20 WHERE id = 1");
        $this->ztdExec("UPDATE mac_items SET name = 'End' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT name, qty FROM mac_items WHERE id = 1");
        $this->assertSame('End', $rows[0]['name']);
        $this->assertSame(20, (int) $rows[0]['qty']);
    }

    public function testComplexMutationThenAggregate(): void
    {
        for ($i = 1; $i <= 5; $i++) {
            $this->ztdExec("INSERT INTO mac_items (id, name, status, qty) VALUES ($i, 'Item$i', 'active', " . ($i * 10) . ")");
        }
        $this->ztdExec("UPDATE mac_items SET status = 'inactive' WHERE id IN (2, 4)");
        $this->ztdExec("DELETE FROM mac_items WHERE id = 3");

        $rows = $this->ztdQuery(
            "SELECT status, SUM(qty) AS total, COUNT(*) AS cnt
             FROM mac_items
             GROUP BY status
             ORDER BY status"
        );
        $this->assertCount(2, $rows);
        $this->assertSame('active', $rows[0]['status']);
        $this->assertSame(60, (int) $rows[0]['total']);
        $this->assertSame(2, (int) $rows[0]['cnt']);
    }

    /**
     * By-design: prepared statements capture snapshot at prepare() time.
     */
    public function testPreparedSelectReexecutionAfterMutation(): void
    {
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'A', 10)");

        $stmt = $this->pdo->prepare("SELECT qty FROM mac_items WHERE id = ?");
        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(10, (int) $rows[0]['qty']);

        $this->ztdExec("UPDATE mac_items SET qty = 999 WHERE id = 1");

        // By-design: re-execution returns stale snapshot
        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(10, (int) $rows[0]['qty']);

        // Fresh prepare() sees updated data
        $stmt2 = $this->pdo->prepare("SELECT qty FROM mac_items WHERE id = ?");
        $stmt2->execute([1]);
        $rows2 = $stmt2->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(999, (int) $rows2[0]['qty']);
    }
}
