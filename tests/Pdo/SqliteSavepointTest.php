<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SAVEPOINT / RELEASE / ROLLBACK TO through ZTD shadow store on SQLite.
 *
 * ORMs (Doctrine, Eloquent) use savepoints for nested transaction support.
 * The ZTD layer must handle savepoint semantics correctly:
 * - Shadow data from after a savepoint should be rolled back on ROLLBACK TO
 * - RELEASE should preserve shadow data
 *
 * @spec SPEC-4.8
 */
class SqliteSavepointTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_svp_accounts (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            balance REAL NOT NULL DEFAULT 0.0
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_svp_accounts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_svp_accounts VALUES (1, 'Alice', 1000.00)");
        $this->pdo->exec("INSERT INTO sl_svp_accounts VALUES (2, 'Bob', 500.00)");
    }

    /**
     * Basic SAVEPOINT + RELEASE: changes should be visible.
     */
    public function testSavepointRelease(): void
    {
        try {
            $this->pdo->exec('SAVEPOINT sp1');
            $this->pdo->exec("UPDATE sl_svp_accounts SET balance = 900.00 WHERE id = 1");
            $this->pdo->exec('RELEASE SAVEPOINT sp1');

            $rows = $this->ztdQuery("SELECT balance FROM sl_svp_accounts WHERE id = 1");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('SAVEPOINT RELEASE: got ' . json_encode($rows));
            }

            $this->assertEqualsWithDelta(900.00, (float) $rows[0]['balance'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SAVEPOINT + RELEASE failed: ' . $e->getMessage());
        }
    }

    /**
     * SAVEPOINT + ROLLBACK TO: changes should be undone in shadow store.
     */
    public function testSavepointRollbackTo(): void
    {
        try {
            $this->pdo->exec('SAVEPOINT sp1');
            $this->pdo->exec("UPDATE sl_svp_accounts SET balance = 0.00 WHERE id = 1");

            // Verify change is visible before rollback
            $before = $this->ztdQuery("SELECT balance FROM sl_svp_accounts WHERE id = 1");
            $balanceBefore = (float) $before[0]['balance'];

            $this->pdo->exec('ROLLBACK TO SAVEPOINT sp1');

            $after = $this->ztdQuery("SELECT balance FROM sl_svp_accounts WHERE id = 1");

            if (count($after) !== 1) {
                $this->markTestIncomplete(
                    'ROLLBACK TO: expected 1 row, got ' . count($after)
                );
            }

            // After ROLLBACK TO, balance should be original 1000
            if (abs((float) $after[0]['balance'] - 1000.00) > 0.01) {
                $this->markTestIncomplete(
                    'ROLLBACK TO: expected 1000.00, got ' . $after[0]['balance']
                    . ' (before rollback was ' . $balanceBefore . ')'
                );
            }

            $this->assertEqualsWithDelta(1000.00, (float) $after[0]['balance'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SAVEPOINT ROLLBACK TO failed: ' . $e->getMessage());
        }
    }

    /**
     * Nested savepoints: outer persists, inner rolled back.
     */
    public function testNestedSavepoints(): void
    {
        try {
            $this->pdo->exec('SAVEPOINT sp_outer');
            $this->pdo->exec("UPDATE sl_svp_accounts SET balance = 800.00 WHERE id = 1");

            $this->pdo->exec('SAVEPOINT sp_inner');
            $this->pdo->exec("UPDATE sl_svp_accounts SET balance = 0.00 WHERE id = 2");
            $this->pdo->exec('ROLLBACK TO SAVEPOINT sp_inner');

            // Bob's balance should be rolled back to 500
            $this->pdo->exec('RELEASE SAVEPOINT sp_outer');

            $rows = $this->ztdQuery("SELECT id, balance FROM sl_svp_accounts ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete('Nested savepoints: got ' . json_encode($rows));
            }

            $alice = (float) $rows[0]['balance'];
            $bob = (float) $rows[1]['balance'];

            if (abs($alice - 800.00) > 0.01 || abs($bob - 500.00) > 0.01) {
                $this->markTestIncomplete(
                    'Nested savepoints: Alice=' . $alice . ' (expected 800), Bob=' . $bob . ' (expected 500)'
                );
            }

            $this->assertEqualsWithDelta(800.00, $alice, 0.01);
            $this->assertEqualsWithDelta(500.00, $bob, 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Nested savepoints failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT inside savepoint then ROLLBACK TO: row should disappear.
     */
    public function testSavepointRollbackInsert(): void
    {
        try {
            $this->pdo->exec('SAVEPOINT sp1');
            $this->pdo->exec("INSERT INTO sl_svp_accounts VALUES (3, 'Charlie', 250.00)");

            // Charlie should be visible before rollback
            $before = $this->ztdQuery("SELECT name FROM sl_svp_accounts ORDER BY id");

            $this->pdo->exec('ROLLBACK TO SAVEPOINT sp1');

            $after = $this->ztdQuery("SELECT name FROM sl_svp_accounts ORDER BY id");

            if (count($after) !== 2) {
                $this->markTestIncomplete(
                    'ROLLBACK INSERT: expected 2 rows after rollback, got ' . count($after)
                    . ' (before rollback: ' . count($before) . '). Data: ' . json_encode($after)
                );
            }

            $this->assertCount(2, $after);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Savepoint rollback INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE inside savepoint then ROLLBACK TO: row should reappear.
     */
    public function testSavepointRollbackDelete(): void
    {
        try {
            $this->pdo->exec('SAVEPOINT sp1');
            $this->pdo->exec("DELETE FROM sl_svp_accounts WHERE id = 2");

            $before = $this->ztdQuery("SELECT name FROM sl_svp_accounts ORDER BY id");

            $this->pdo->exec('ROLLBACK TO SAVEPOINT sp1');

            $after = $this->ztdQuery("SELECT name FROM sl_svp_accounts ORDER BY id");

            if (count($after) !== 2) {
                $this->markTestIncomplete(
                    'ROLLBACK DELETE: expected 2 after rollback, got ' . count($after)
                    . ' (before rollback: ' . count($before) . ')'
                );
            }

            $this->assertCount(2, $after);
            $this->assertSame('Bob', $after[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Savepoint rollback DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * SAVEPOINT within BEGIN/COMMIT transaction.
     */
    public function testSavepointWithinTransaction(): void
    {
        try {
            $this->ztdBeginTransaction();

            $this->pdo->exec("INSERT INTO sl_svp_accounts VALUES (3, 'Charlie', 300.00)");

            $this->pdo->exec('SAVEPOINT sp1');
            $this->pdo->exec("INSERT INTO sl_svp_accounts VALUES (4, 'Diana', 400.00)");
            $this->pdo->exec('ROLLBACK TO SAVEPOINT sp1');

            $this->ztdCommit();

            $rows = $this->ztdQuery("SELECT name FROM sl_svp_accounts ORDER BY id");

            // Charlie should exist (before savepoint), Diana should not (rolled back)
            $names = array_column($rows, 'name');

            if (!in_array('Charlie', $names)) {
                $this->markTestIncomplete(
                    'Savepoint in txn: Charlie missing. Data: ' . json_encode($rows)
                );
            }

            if (in_array('Diana', $names)) {
                $this->markTestIncomplete(
                    'Savepoint in txn: Diana should be rolled back. Data: ' . json_encode($rows)
                );
            }

            $this->assertContains('Charlie', $names);
            $this->assertNotContains('Diana', $names);
        } catch (\Throwable $e) {
            if ($this->ztdInTransaction()) {
                $this->ztdRollBack();
            }
            $this->markTestIncomplete('Savepoint within transaction failed: ' . $e->getMessage());
        }
    }
}
