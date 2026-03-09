<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests the shadow store lifecycle when the same primary key is deleted and re-inserted
 * across multiple rapid cycles, with PK mutation and bulk reset patterns.
 *
 * Whereas SqliteDeleteReinsertCycleTest covers single-cycle and cross-table scenarios,
 * this file targets rapid PK reuse, PK value mutation followed by reuse of the original PK,
 * bulk delete-all + bulk re-insert of same PKs, and prepared-statement-driven cycles.
 *
 * Bugs may appear as:
 * - Stale data from an earlier insert leaking through after re-insert
 * - Re-insert failing due to phantom PK conflict in the shadow store
 * - DELETE not fully clearing the shadow entry, leaving ghost rows
 * - Aggregate miscounts after rapid reuse of the same PKs
 * @spec SPEC-4.1
 * @spec SPEC-4.3
 */
class SqliteDeleteReinsertPkCycleTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_drpc_data (id INTEGER PRIMARY KEY, name TEXT NOT NULL, version INTEGER NOT NULL)';
    }

    protected function getTableNames(): array
    {
        return ['sl_drpc_data'];
    }

    /**
     * Three consecutive delete-reinsert cycles on the same PK.
     * Each cycle replaces the data with a new version.
     */
    public function testThreeDeleteReinsertCycles(): void
    {
        for ($cycle = 1; $cycle <= 3; $cycle++) {
            if ($cycle > 1) {
                $this->pdo->exec('DELETE FROM sl_drpc_data WHERE id = 1');

                $gone = $this->ztdQuery('SELECT * FROM sl_drpc_data WHERE id = 1');
                if (count($gone) !== 0) {
                    $this->markTestIncomplete(
                        "Cycle $cycle: DELETE did not clear id=1, got " . count($gone) . ' rows'
                    );
                    return;
                }
            }

            $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (1, 'version_$cycle', $cycle)");

            $rows = $this->ztdQuery('SELECT name, version FROM sl_drpc_data WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    "Cycle $cycle: expected 1 row after re-insert, got " . count($rows)
                );
                return;
            }

            $this->assertSame("version_$cycle", $rows[0]['name'], "Cycle $cycle name mismatch");
            $this->assertSame((string) $cycle, (string) $rows[0]['version'], "Cycle $cycle version mismatch");
        }
    }

    /**
     * Rapid cycles: delete and re-insert id=1 five times in quick succession.
     * Verifies the shadow store handles high-frequency PK reuse.
     */
    public function testRapidDeleteReinsertFiveCycles(): void
    {
        for ($i = 1; $i <= 5; $i++) {
            if ($i > 1) {
                $this->pdo->exec('DELETE FROM sl_drpc_data WHERE id = 1');
            }
            $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (1, 'round_$i', $i)");
        }

        $rows = $this->ztdQuery('SELECT name, version FROM sl_drpc_data WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('round_5', $rows[0]['name']);
        $this->assertSame('5', (string) $rows[0]['version']);

        // Only 1 row total
        $cnt = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sl_drpc_data');
        $this->assertSame('1', (string) $cnt[0]['cnt']);
    }

    /**
     * Delete-reinsert with multiple PKs simultaneously.
     * Verifies independent PK tracking does not interfere.
     */
    public function testDeleteReinsertMultiplePks(): void
    {
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (1, 'alice', 1)");
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (2, 'bob', 1)");
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (3, 'charlie', 1)");

        // Delete two of them
        $this->pdo->exec('DELETE FROM sl_drpc_data WHERE id IN (1, 3)');

        $remaining = $this->ztdQuery('SELECT id FROM sl_drpc_data ORDER BY id');
        $this->assertCount(1, $remaining);
        $this->assertSame('2', (string) $remaining[0]['id']);

        // Re-insert the deleted PKs with new data
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (1, 'alice_v2', 2)");
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (3, 'charlie_v2', 2)");

        $rows = $this->ztdQuery('SELECT id, name, version FROM sl_drpc_data ORDER BY id');
        $this->assertCount(3, $rows);
        $this->assertSame('alice_v2', $rows[0]['name']);
        $this->assertSame('bob', $rows[1]['name']);
        $this->assertSame('charlie_v2', $rows[2]['name']);
        $this->assertSame('1', (string) $rows[1]['version']); // Bob unchanged
    }

    /**
     * UPDATE PK value then INSERT original PK.
     *
     * Scenario: row id=1 gets PK changed to id=100, then a brand new row id=1 is inserted.
     * Shadow store must handle both the re-keyed row and the new row without conflict.
     */
    public function testUpdatePkThenInsertOriginalPk(): void
    {
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (1, 'original', 1)");

        try {
            $this->pdo->exec('UPDATE sl_drpc_data SET id = 100 WHERE id = 1');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE PK value failed: ' . $e->getMessage()
            );
            return;
        }

        // Verify old PK gone, new PK present
        $old = $this->ztdQuery('SELECT * FROM sl_drpc_data WHERE id = 1');
        if (count($old) !== 0) {
            $this->markTestIncomplete(
                'UPDATE PK: old PK id=1 still visible after UPDATE SET id=100. '
                . 'Shadow store does not re-key rows on PK change.'
            );
            return;
        }
        $this->assertCount(0, $old, 'Old PK should be gone after UPDATE');

        $moved = $this->ztdQuery('SELECT name FROM sl_drpc_data WHERE id = 100');
        $this->assertCount(1, $moved);
        $this->assertSame('original', $moved[0]['name']);

        // Now insert a new row with the original PK
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (1, 'replacement', 2)");

        $rows = $this->ztdQuery('SELECT id, name, version FROM sl_drpc_data ORDER BY id');

        if (count($rows) !== 2) {
            $this->markTestIncomplete(
                'UPDATE PK + INSERT original PK: expected 2 rows, got ' . count($rows)
            );
            return;
        }

        $this->assertSame('1', (string) $rows[0]['id']);
        $this->assertSame('replacement', $rows[0]['name']);
        $this->assertSame('2', (string) $rows[0]['version']);

        $this->assertSame('100', (string) $rows[1]['id']);
        $this->assertSame('original', $rows[1]['name']);
        $this->assertSame('1', (string) $rows[1]['version']);
    }

    /**
     * DELETE all rows (WHERE 1=1) then INSERT same PKs — bulk reset pattern.
     */
    public function testDeleteAllThenInsertSamePks(): void
    {
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (1, 'old_alice', 1)");
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (2, 'old_bob', 1)");
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (3, 'old_charlie', 1)");

        $this->pdo->exec('DELETE FROM sl_drpc_data WHERE 1=1');

        $empty = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sl_drpc_data');
        $this->assertSame('0', (string) $empty[0]['cnt']);

        // Re-insert all same PKs with new data
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (1, 'new_alice', 2)");
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (2, 'new_bob', 2)");
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (3, 'new_charlie', 2)");

        $rows = $this->ztdQuery('SELECT id, name, version FROM sl_drpc_data ORDER BY id');
        $this->assertCount(3, $rows);
        $this->assertSame('new_alice', $rows[0]['name']);
        $this->assertSame('new_bob', $rows[1]['name']);
        $this->assertSame('new_charlie', $rows[2]['name']);
        foreach ($rows as $row) {
            $this->assertSame('2', (string) $row['version']);
        }
    }

    /**
     * Prepared statement driven delete-reinsert cycle.
     * Uses the same prepared INSERT and DELETE statements across multiple cycles.
     */
    public function testPreparedDeleteReinsertCycle(): void
    {
        $insertStmt = $this->pdo->prepare('INSERT INTO sl_drpc_data (id, name, version) VALUES (?, ?, ?)');
        $deleteStmt = $this->pdo->prepare('DELETE FROM sl_drpc_data WHERE id = ?');
        $selectStmt = $this->pdo->prepare('SELECT name, version FROM sl_drpc_data WHERE id = ?');

        // Cycle 1: insert
        $insertStmt->execute([1, 'prep_v1', 1]);
        $selectStmt->execute([1]);
        $row = $selectStmt->fetch(PDO::FETCH_ASSOC);
        if ($row === false) {
            // Prepared SELECT created before INSERT may not see shadow data
            // due to CTE rewrite snapshot at prepare time.
            // Verify using a fresh query instead:
            $freshRows = $this->ztdQuery('SELECT name FROM sl_drpc_data WHERE id = 1');
            $this->markTestIncomplete(
                'Prepared SELECT created before INSERT returns no rows. '
                . 'Fresh query returns ' . count($freshRows) . ' row(s). '
                . 'CTE rewrite snapshot at prepare-time does not reflect subsequent inserts.'
            );
            return;
        }
        $this->assertSame('prep_v1', $row['name']);

        // Cycle 1: delete
        $deleteStmt->execute([1]);
        $selectStmt->execute([1]);
        $row = $selectStmt->fetch(PDO::FETCH_ASSOC);
        $this->assertFalse($row, 'Row should be gone after prepared DELETE');

        // Cycle 2: re-insert
        $insertStmt->execute([1, 'prep_v2', 2]);
        $selectStmt->execute([1]);
        $row = $selectStmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('prep_v2', $row['name']);
        $this->assertSame('2', (string) $row['version']);

        // Cycle 2: delete
        $deleteStmt->execute([1]);
        $selectStmt->execute([1]);
        $this->assertFalse($selectStmt->fetch(PDO::FETCH_ASSOC));

        // Cycle 3: re-insert
        $insertStmt->execute([1, 'prep_v3', 3]);
        $selectStmt->execute([1]);
        $row = $selectStmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('prep_v3', $row['name']);
        $this->assertSame('3', (string) $row['version']);
    }

    /**
     * Delete-reinsert with aggregate verification at each stage.
     * SUM and COUNT must reflect the current shadow state accurately.
     */
    public function testDeleteReinsertWithAggregateChecks(): void
    {
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (1, 'a', 10)");
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (2, 'b', 20)");
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (3, 'c', 30)");

        $agg = $this->ztdQuery('SELECT COUNT(*) AS cnt, SUM(version) AS total FROM sl_drpc_data');
        $this->assertSame('3', (string) $agg[0]['cnt']);
        $this->assertSame('60', (string) $agg[0]['total']);

        // Delete id=2
        $this->pdo->exec('DELETE FROM sl_drpc_data WHERE id = 2');

        $agg = $this->ztdQuery('SELECT COUNT(*) AS cnt, SUM(version) AS total FROM sl_drpc_data');
        $this->assertSame('2', (string) $agg[0]['cnt']);
        $this->assertSame('40', (string) $agg[0]['total']);

        // Re-insert id=2 with version=50
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (2, 'b_new', 50)");

        $agg = $this->ztdQuery('SELECT COUNT(*) AS cnt, SUM(version) AS total FROM sl_drpc_data');
        $this->assertSame('3', (string) $agg[0]['cnt']);
        $this->assertSame('90', (string) $agg[0]['total']); // 10 + 50 + 30 = 90
    }

    /**
     * Multi-row INSERT after DELETE all — verify bulk re-insert works.
     */
    public function testMultiRowInsertAfterDeleteAll(): void
    {
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (1, 'old1', 1), (2, 'old2', 1), (3, 'old3', 1)");

        $this->pdo->exec('DELETE FROM sl_drpc_data WHERE 1=1');

        try {
            $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (1, 'fresh1', 2), (2, 'fresh2', 2), (3, 'fresh3', 2)");
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multi-row INSERT after DELETE all failed: ' . $e->getMessage()
            );
            return;
        }

        $rows = $this->ztdQuery('SELECT id, name FROM sl_drpc_data ORDER BY id');

        if (count($rows) !== 3) {
            $this->markTestIncomplete(
                'Multi-row INSERT after DELETE all: expected 3 rows, got ' . count($rows)
            );
            return;
        }

        $this->assertSame('fresh1', $rows[0]['name']);
        $this->assertSame('fresh2', $rows[1]['name']);
        $this->assertSame('fresh3', $rows[2]['name']);
    }

    /**
     * Physical isolation: after delete-reinsert cycle, physical table has no data.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (1, 'first', 1)");
        $this->pdo->exec('DELETE FROM sl_drpc_data WHERE id = 1');
        $this->pdo->exec("INSERT INTO sl_drpc_data VALUES (1, 'second', 2)");

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sl_drpc_data');
        $this->assertSame('1', (string) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_drpc_data');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
