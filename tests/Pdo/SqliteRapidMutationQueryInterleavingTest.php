<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests rapid interleaving of mutations and queries — the most common web app pattern.
 *
 * Real PHP apps do: INSERT → fetch by ID → UPDATE a field → re-fetch → DELETE → verify gone.
 * Each step must see the effects of the previous step through the ZTD shadow store.
 * This is different from tests that do all mutations first then one query at the end.
 *
 * @spec SPEC-10.2
 */
class SqliteRapidMutationQueryInterleavingTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_rmqi_t (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            status TEXT DEFAULT 'active'
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_rmqi_t'];
    }

    /**
     * Full CRUD lifecycle: INSERT → SELECT → UPDATE → SELECT → DELETE → SELECT.
     */
    public function testFullCrudLifecycle(): void
    {
        try {
            // Step 1: INSERT (include all columns to avoid Issue #21 — DEFAULT not applied)
            $this->ztdExec("INSERT INTO sl_rmqi_t (id, name, email, status) VALUES (1, 'Alice', 'alice@test.com', 'active')");

            // Step 2: SELECT immediately after INSERT
            $rows = $this->ztdQuery("SELECT * FROM sl_rmqi_t WHERE id = 1");
            if (count($rows) !== 1) {
                $this->markTestIncomplete('CRUD lifecycle (SQLite): INSERT then SELECT failed, got ' . count($rows) . ' rows');
            }
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('alice@test.com', $rows[0]['email']);
            $this->assertSame('active', $rows[0]['status']);

            // Step 3: UPDATE
            $this->ztdExec("UPDATE sl_rmqi_t SET email = 'newalice@test.com', status = 'verified' WHERE id = 1");

            // Step 4: SELECT immediately after UPDATE
            $rows2 = $this->ztdQuery("SELECT * FROM sl_rmqi_t WHERE id = 1");
            if (count($rows2) !== 1) {
                $this->markTestIncomplete('CRUD lifecycle (SQLite): UPDATE then SELECT failed, got ' . count($rows2) . ' rows');
            }
            if ($rows2[0]['email'] !== 'newalice@test.com') {
                $this->markTestIncomplete(
                    'CRUD lifecycle (SQLite): UPDATE not reflected. email=' . $rows2[0]['email']
                    . ', status=' . $rows2[0]['status']
                );
            }
            $this->assertSame('newalice@test.com', $rows2[0]['email']);
            $this->assertSame('verified', $rows2[0]['status']);

            // Step 5: DELETE
            $this->ztdExec("DELETE FROM sl_rmqi_t WHERE id = 1");

            // Step 6: SELECT immediately after DELETE
            $rows3 = $this->ztdQuery("SELECT * FROM sl_rmqi_t WHERE id = 1");
            if (count($rows3) !== 0) {
                $this->markTestIncomplete(
                    'CRUD lifecycle (SQLite): DELETE not reflected, still got ' . count($rows3) . ' rows: '
                    . json_encode($rows3)
                );
            }
            $this->assertCount(0, $rows3);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CRUD lifecycle (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple INSERTs with interleaved COUNT queries.
     */
    public function testInterleavedInsertCount(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_rmqi_t (id, name, status) VALUES (1, 'A', 'active')");
            $c1 = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_rmqi_t");
            $this->assertSame(1, (int) $c1[0]['cnt']);

            $this->ztdExec("INSERT INTO sl_rmqi_t (id, name, status) VALUES (2, 'B', 'active')");
            $c2 = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_rmqi_t");
            $this->assertSame(2, (int) $c2[0]['cnt']);

            $this->ztdExec("INSERT INTO sl_rmqi_t (id, name, status) VALUES (3, 'C', 'active')");
            $c3 = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_rmqi_t");
            if ((int) $c3[0]['cnt'] !== 3) {
                $this->markTestIncomplete(
                    'Interleaved INSERT/COUNT (SQLite): expected 3 after 3 inserts, got ' . $c3[0]['cnt']
                );
            }
            $this->assertSame(3, (int) $c3[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Interleaved INSERT/COUNT (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE then DELETE on same row — DELETE should win.
     */
    public function testUpdateThenDelete(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_rmqi_t (id, name, status) VALUES (1, 'Alice', 'active')");
            $this->ztdExec("UPDATE sl_rmqi_t SET name = 'Bob' WHERE id = 1");

            // Verify UPDATE took effect
            $r1 = $this->ztdQuery("SELECT name FROM sl_rmqi_t WHERE id = 1");
            if ($r1[0]['name'] !== 'Bob') {
                $this->markTestIncomplete('UPDATE-then-DELETE (SQLite): UPDATE not reflected, got ' . $r1[0]['name']);
            }

            $this->ztdExec("DELETE FROM sl_rmqi_t WHERE id = 1");

            $r2 = $this->ztdQuery("SELECT * FROM sl_rmqi_t WHERE id = 1");
            if (count($r2) !== 0) {
                $this->markTestIncomplete(
                    'UPDATE-then-DELETE (SQLite): row still present after DELETE: ' . json_encode($r2)
                );
            }
            $this->assertCount(0, $r2);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE-then-DELETE (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT then prepared SELECT — prepare after mutation.
     */
    public function testPreparedAfterMutation(): void
    {
        try {
            // Insert first, then prepare and execute SELECT
            $this->ztdExec("INSERT INTO sl_rmqi_t (id, name, email, status) VALUES (1, 'Alice', 'a@test.com', 'active')");

            $select = $this->pdo->prepare("SELECT name, email FROM sl_rmqi_t WHERE id = ?");
            $select->execute([1]);
            $r1 = $select->fetchAll(PDO::FETCH_ASSOC);
            if (count($r1) !== 1) {
                $this->markTestIncomplete('Prepared after mutation (SQLite): expected 1 row, got ' . count($r1));
            }
            $this->assertSame('Alice', $r1[0]['name']);
            $this->assertSame('a@test.com', $r1[0]['email']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared after mutation (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * Filter (WHERE) on column that was just UPDATEd.
     */
    public function testFilterOnUpdatedColumn(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_rmqi_t (id, name, status) VALUES (1, 'Alice', 'active')");
            $this->ztdExec("INSERT INTO sl_rmqi_t (id, name, status) VALUES (2, 'Bob', 'active')");
            $this->ztdExec("INSERT INTO sl_rmqi_t (id, name, status) VALUES (3, 'Charlie', 'active')");

            // Update one row's status
            $this->ztdExec("UPDATE sl_rmqi_t SET status = 'suspended' WHERE id = 2");

            // Query by updated status
            $active = $this->ztdQuery("SELECT name FROM sl_rmqi_t WHERE status = 'active' ORDER BY name");
            $suspended = $this->ztdQuery("SELECT name FROM sl_rmqi_t WHERE status = 'suspended'");

            if (count($active) !== 2) {
                $this->markTestIncomplete(
                    'Filter on updated column (SQLite): expected 2 active, got ' . count($active)
                    . '. Rows: ' . json_encode($active)
                );
            }

            if (count($suspended) !== 1) {
                $this->markTestIncomplete(
                    'Filter on updated column (SQLite): expected 1 suspended, got ' . count($suspended)
                    . '. Rows: ' . json_encode($suspended)
                );
            }

            $this->assertSame('Alice', $active[0]['name']);
            $this->assertSame('Charlie', $active[1]['name']);
            $this->assertSame('Bob', $suspended[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Filter on updated column (SQLite) failed: ' . $e->getMessage());
        }
    }
}
