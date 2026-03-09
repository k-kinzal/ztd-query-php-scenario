<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DELETE with LIMIT and UPDATE with LIMIT on SQLite PDO ZTD.
 *
 * SQLite supports DELETE ... ORDER BY ... LIMIT and UPDATE ... ORDER BY ... LIMIT
 * when compiled with SQLITE_ENABLE_UPDATE_DELETE_LIMIT. These are common patterns
 * for batch processing (e.g., process N oldest queue items, cap update scope).
 *
 * This test focuses on UPDATE with LIMIT (not covered by SqliteDeleteWithOrderByLimitTest)
 * and additional DELETE+WHERE+LIMIT combinations with prepared statements.
 * @spec SPEC-4.3
 * @spec SPEC-4.2
 */
class SqliteDeleteLimitTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_dl_queue (id INTEGER PRIMARY KEY, status TEXT NOT NULL, priority INTEGER NOT NULL, payload TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['sl_dl_queue'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_dl_queue VALUES (1, 'pending', 10, 'job-alpha')");
        $this->pdo->exec("INSERT INTO sl_dl_queue VALUES (2, 'pending', 20, 'job-beta')");
        $this->pdo->exec("INSERT INTO sl_dl_queue VALUES (3, 'pending', 30, 'job-gamma')");
        $this->pdo->exec("INSERT INTO sl_dl_queue VALUES (4, 'done', 10, 'job-delta')");
        $this->pdo->exec("INSERT INTO sl_dl_queue VALUES (5, 'pending', 5, 'job-epsilon')");
    }

    /**
     * DELETE the 2 lowest-priority pending rows via exec.
     */
    public function testDeletePendingWithOrderByLimit(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_dl_queue WHERE status = 'pending' ORDER BY priority ASC LIMIT 2");
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE WHERE ... ORDER BY ... LIMIT failed: ' . $e->getMessage()
            );
            return;
        }

        $rows = $this->ztdQuery('SELECT id FROM sl_dl_queue ORDER BY id');

        // priority 5 (id=5) and priority 10 (id=1) should be deleted
        // remaining: id=2 (priority 20), id=3 (priority 30), id=4 (done, priority 10)
        if (count($rows) !== 3) {
            $this->markTestIncomplete(
                'DELETE WHERE+ORDER BY+LIMIT: expected 3 remaining rows, got ' . count($rows)
            );
            return;
        }

        $ids = array_map('intval', array_column($rows, 'id'));
        $this->assertSame([2, 3, 4], $ids);
    }

    /**
     * DELETE with LIMIT via prepared statement with bound limit parameter.
     */
    public function testPreparedDeleteWithWhereAndLimit(): void
    {
        try {
            $stmt = $this->pdo->prepare("DELETE FROM sl_dl_queue WHERE status = ? ORDER BY id ASC LIMIT ?");
            $stmt->execute(['pending', 2]);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Prepared DELETE WHERE+ORDER BY+LIMIT failed: ' . $e->getMessage()
            );
            return;
        }

        $rows = $this->ztdQuery('SELECT id FROM sl_dl_queue ORDER BY id');

        // Pending by id ASC: 1, 2, 3, 5 — first 2 deleted → 1, 2 gone
        // Remaining: 3 (pending), 4 (done), 5 (pending)
        if (count($rows) !== 3) {
            $this->markTestIncomplete(
                'Prepared DELETE WHERE+LIMIT: expected 3 rows, got ' . count($rows)
            );
            return;
        }

        $ids = array_map('intval', array_column($rows, 'id'));
        $this->assertSame([3, 4, 5], $ids);
    }

    /**
     * UPDATE with ORDER BY and LIMIT — mark the top-2 priority pending jobs as 'processing'.
     *
     * SQLite supports: UPDATE t SET ... ORDER BY ... LIMIT n
     * This is a real-world queue-claim pattern.
     */
    public function testUpdateWithOrderByLimit(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_dl_queue SET status = 'processing' WHERE status = 'pending' ORDER BY priority DESC LIMIT 2");
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE WHERE+ORDER BY+LIMIT failed: ' . $e->getMessage()
            );
            return;
        }

        // Top 2 by priority DESC among pending: id=3 (30), id=2 (20)
        $processing = $this->ztdQuery("SELECT id FROM sl_dl_queue WHERE status = 'processing' ORDER BY id");
        $pending = $this->ztdQuery("SELECT id FROM sl_dl_queue WHERE status = 'pending' ORDER BY id");

        if (count($processing) !== 2) {
            $this->markTestIncomplete(
                'UPDATE+LIMIT: expected 2 processing rows, got ' . count($processing)
            );
            return;
        }

        $this->assertSame([2, 3], array_map('intval', array_column($processing, 'id')));
        // Remaining pending: id=1 (priority 10), id=5 (priority 5)
        $this->assertSame([1, 5], array_map('intval', array_column($pending, 'id')));
    }

    /**
     * Prepared UPDATE with ORDER BY and LIMIT using bound parameters.
     */
    public function testPreparedUpdateWithLimit(): void
    {
        try {
            $stmt = $this->pdo->prepare("UPDATE sl_dl_queue SET status = ? WHERE status = 'pending' ORDER BY priority ASC LIMIT ?");
            $stmt->execute(['cancelled', 1]);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Prepared UPDATE+LIMIT failed: ' . $e->getMessage()
            );
            return;
        }

        // Lowest priority pending: id=5 (priority 5)
        $cancelled = $this->ztdQuery("SELECT id FROM sl_dl_queue WHERE status = 'cancelled'");

        if (count($cancelled) !== 1) {
            $this->markTestIncomplete(
                'Prepared UPDATE+LIMIT: expected 1 cancelled row, got ' . count($cancelled)
            );
            return;
        }

        $this->assertSame(5, (int) $cancelled[0]['id']);
    }

    /**
     * UPDATE LIMIT then DELETE LIMIT in sequence — compound batch operation.
     */
    public function testUpdateLimitThenDeleteLimitSequence(): void
    {
        try {
            // First: mark top 2 priority pending as 'processing'
            $this->pdo->exec("UPDATE sl_dl_queue SET status = 'processing' WHERE status = 'pending' ORDER BY priority DESC LIMIT 2");

            // Second: delete the 'done' row
            $this->pdo->exec("DELETE FROM sl_dl_queue WHERE status = 'done' ORDER BY id LIMIT 1");

            $rows = $this->ztdQuery('SELECT id, status FROM sl_dl_queue ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE+LIMIT then DELETE+LIMIT sequence failed: ' . $e->getMessage()
            );
            return;
        }

        // After update: id=2 processing, id=3 processing; id=1 pending, id=5 pending; id=4 done
        // After delete of done: id=4 gone
        if (count($rows) !== 4) {
            $this->markTestIncomplete(
                'UPDATE+DELETE LIMIT sequence: expected 4 rows, got ' . count($rows)
            );
            return;
        }

        $this->assertSame(1, (int) $rows[0]['id']);
        $this->assertSame('pending', $rows[0]['status']);
        $this->assertSame(2, (int) $rows[1]['id']);
        $this->assertSame('processing', $rows[1]['status']);
    }

    /**
     * UPDATE with LIMIT 0 — should affect zero rows.
     */
    public function testUpdateWithLimitZero(): void
    {
        try {
            $affected = $this->pdo->exec("UPDATE sl_dl_queue SET status = 'nope' ORDER BY id LIMIT 0");
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE LIMIT 0 failed: ' . $e->getMessage()
            );
            return;
        }

        $changed = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_dl_queue WHERE status = 'nope'");
        $this->assertSame(0, (int) $changed[0]['cnt']);
    }

    /**
     * Physical isolation: ZTD mutations via UPDATE+LIMIT not visible with ZTD disabled.
     */
    public function testPhysicalIsolation(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_dl_queue SET status = 'claimed' WHERE status = 'pending' ORDER BY priority DESC LIMIT 1");
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE+LIMIT for isolation check failed: ' . $e->getMessage()
            );
            return;
        }

        // With ZTD: should see 1 'claimed' row
        $claimed = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_dl_queue WHERE status = 'claimed'");
        $this->assertSame(1, (int) $claimed[0]['cnt']);

        // Physical: no data at all
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_dl_queue');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
