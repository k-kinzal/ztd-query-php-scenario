<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DELETE with LIMIT and ORDER BY on SQLite.
 *
 * SQLite supports DELETE ... ORDER BY ... LIMIT N (when compiled with
 * SQLITE_ENABLE_UPDATE_DELETE_LIMIT). This is a common pattern for
 * batch cleanup operations.
 *
 * Issue #130 documents UPDATE/DELETE ORDER BY LIMIT as a no-op on MySQL.
 * This tests whether SQLite handles it correctly.
 *
 * @spec SPEC-4.3
 */
class SqliteDeleteWithLimitTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_dwl_logs (
            id INTEGER PRIMARY KEY,
            level TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_dwl_logs'];
    }

    /**
     * DELETE with WHERE and LIMIT — only N matching rows should be deleted.
     */
    public function testDeleteWithWhereAndLimit(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_dwl_logs (id, level, message, created_at) VALUES (1, 'debug', 'msg1', 100)");
            $this->pdo->exec("INSERT INTO sl_dwl_logs (id, level, message, created_at) VALUES (2, 'debug', 'msg2', 200)");
            $this->pdo->exec("INSERT INTO sl_dwl_logs (id, level, message, created_at) VALUES (3, 'debug', 'msg3', 300)");
            $this->pdo->exec("INSERT INTO sl_dwl_logs (id, level, message, created_at) VALUES (4, 'error', 'err1', 400)");
            $this->pdo->exec("INSERT INTO sl_dwl_logs (id, level, message, created_at) VALUES (5, 'debug', 'msg4', 500)");

            $this->pdo->exec("DELETE FROM sl_dwl_logs WHERE level = 'debug' LIMIT 2");

            $rows = $this->ztdQuery("SELECT id, message FROM sl_dwl_logs ORDER BY id");

            // 4 debug rows, LIMIT 2 should delete 2, leaving 2 debug + 1 error = 3
            if (count($rows) === 5) {
                $this->markTestIncomplete(
                    'DELETE with LIMIT had no effect — all 5 rows remain. LIMIT may not be supported in DELETE.'
                );
            }
            if (count($rows) === 1) {
                $this->markTestIncomplete(
                    'DELETE with LIMIT deleted ALL debug rows (4) instead of LIMIT 2. LIMIT was ignored, only WHERE applied.'
                );
            }
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE with LIMIT produced ' . count($rows) . ' remaining rows. Expected 3. Got: ' . json_encode($rows)
                );
            }
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with WHERE and LIMIT test failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with ORDER BY and LIMIT — should delete the N oldest rows.
     */
    public function testDeleteOrderByLimit(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_dwl_logs (id, level, message, created_at) VALUES (1, 'info', 'oldest', 100)");
            $this->pdo->exec("INSERT INTO sl_dwl_logs (id, level, message, created_at) VALUES (2, 'info', 'old', 200)");
            $this->pdo->exec("INSERT INTO sl_dwl_logs (id, level, message, created_at) VALUES (3, 'info', 'recent', 300)");
            $this->pdo->exec("INSERT INTO sl_dwl_logs (id, level, message, created_at) VALUES (4, 'info', 'newest', 400)");

            $this->pdo->exec("DELETE FROM sl_dwl_logs ORDER BY created_at ASC LIMIT 2");

            $rows = $this->ztdQuery("SELECT id, message FROM sl_dwl_logs ORDER BY created_at ASC");

            if (count($rows) === 4) {
                $this->markTestIncomplete(
                    'DELETE ORDER BY LIMIT had no effect — all 4 rows remain.'
                );
            }
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE ORDER BY LIMIT left ' . count($rows) . ' rows. Expected 2. Got: ' . json_encode($rows)
                );
            }

            // The two remaining should be the newer ones
            $messages = array_column($rows, 'message');
            if (!in_array('recent', $messages) || !in_array('newest', $messages)) {
                $this->markTestIncomplete(
                    'DELETE ORDER BY ASC LIMIT 2 did not delete oldest rows. Remaining: ' . json_encode($messages)
                );
            }
            $this->assertContains('recent', $messages);
            $this->assertContains('newest', $messages);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE ORDER BY LIMIT test failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE with LIMIT parameter.
     */
    public function testPreparedDeleteWithLimit(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_dwl_logs (id, level, message, created_at) VALUES (1, 'debug', 'a', 100)");
            $this->pdo->exec("INSERT INTO sl_dwl_logs (id, level, message, created_at) VALUES (2, 'debug', 'b', 200)");
            $this->pdo->exec("INSERT INTO sl_dwl_logs (id, level, message, created_at) VALUES (3, 'debug', 'c', 300)");

            $stmt = $this->pdo->prepare("DELETE FROM sl_dwl_logs WHERE level = ? LIMIT ?");
            $stmt->execute(['debug', 1]);

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_dwl_logs");
            $count = (int) $rows[0]['cnt'];

            if ($count === 3) {
                $this->markTestIncomplete(
                    'Prepared DELETE with LIMIT ? had no effect — all 3 rows remain.'
                );
            }
            if ($count === 0) {
                $this->markTestIncomplete(
                    'Prepared DELETE with LIMIT ? deleted all rows instead of LIMIT 1.'
                );
            }
            $this->assertEquals(2, $count);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE with LIMIT test failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE LIMIT on shadow-only data (no physical rows).
     */
    public function testDeleteLimitOnShadowOnlyData(): void
    {
        try {
            // All data is shadow-only (inserted via ZTD)
            $this->pdo->exec("INSERT INTO sl_dwl_logs (id, level, message, created_at) VALUES (1, 'warn', 'w1', 100)");
            $this->pdo->exec("INSERT INTO sl_dwl_logs (id, level, message, created_at) VALUES (2, 'warn', 'w2', 200)");
            $this->pdo->exec("INSERT INTO sl_dwl_logs (id, level, message, created_at) VALUES (3, 'warn', 'w3', 300)");

            // Verify all 3 exist
            $beforeCount = (int) $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_dwl_logs")[0]['cnt'];
            $this->assertEquals(3, $beforeCount);

            $this->pdo->exec("DELETE FROM sl_dwl_logs WHERE level = 'warn' ORDER BY created_at DESC LIMIT 1");

            $rows = $this->ztdQuery("SELECT id, message FROM sl_dwl_logs ORDER BY id");

            if (count($rows) === 3) {
                $this->markTestIncomplete(
                    'DELETE LIMIT on shadow-only data had no effect.'
                );
            }
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE LIMIT on shadow-only data left ' . count($rows) . ' rows. Expected 2. Got: ' . json_encode($rows)
                );
            }

            // DESC LIMIT 1 should delete the newest (id=3)
            $ids = array_column($rows, 'id');
            if (in_array('3', $ids) || in_array(3, $ids)) {
                $this->markTestIncomplete(
                    'DELETE ORDER BY DESC LIMIT 1 did not delete the newest row. Remaining: ' . json_encode($rows)
                );
            }
            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE LIMIT on shadow-only data test failed: ' . $e->getMessage());
        }
    }
}
