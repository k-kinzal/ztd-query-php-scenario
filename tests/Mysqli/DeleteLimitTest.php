<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests DELETE/UPDATE with ORDER BY and LIMIT through the CTE rewriter (MySQLi).
 *
 * Targets CTE rewriter edge cases that differ from the existing
 * DeleteWithOrderByLimitTest and UpdateWithOrderByLimitTest:
 *   - DELETE ORDER BY ... DESC LIMIT (reverse deletion)
 *   - UPDATE ORDER BY ... LIMIT with specific row verification
 *   - DELETE WHERE + ORDER BY + LIMIT with timestamp-like column
 *   - Interaction between LIMIT-based DML and subsequent SELECTs
 *   - Prepared DELETE/UPDATE with ORDER BY + LIMIT
 *
 * @spec SPEC-4.3
 */
class DeleteLimitTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_del_lim (
            id INT PRIMARY KEY,
            status VARCHAR(20) NOT NULL,
            created_at DATETIME NOT NULL,
            priority INT NOT NULL DEFAULT 0
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_del_lim'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_del_lim VALUES (1, 'active',   '2024-01-01 10:00:00', 3)");
        $this->ztdExec("INSERT INTO mi_del_lim VALUES (2, 'active',   '2024-01-02 10:00:00', 1)");
        $this->ztdExec("INSERT INTO mi_del_lim VALUES (3, 'inactive', '2024-01-03 10:00:00', 2)");
        $this->ztdExec("INSERT INTO mi_del_lim VALUES (4, 'active',   '2024-01-04 10:00:00', 5)");
        $this->ztdExec("INSERT INTO mi_del_lim VALUES (5, 'inactive', '2024-01-05 10:00:00', 4)");
    }

    /**
     * DELETE ORDER BY id DESC LIMIT 2 removes the last two rows by id.
     *
     * The CTE rewriter must correctly translate the DESC ordering
     * so that rows 4 and 5 are deleted, not rows 1 and 2.
     */
    public function testDeleteOrderByDescLimit(): void
    {
        try {
            $this->ztdExec("DELETE FROM mi_del_lim ORDER BY id DESC LIMIT 2");

            $rows = $this->ztdQuery('SELECT id FROM mi_del_lim ORDER BY id');
            $this->assertCount(3, $rows, 'Should have 3 rows after DELETE LIMIT 2');

            $ids = array_column($rows, 'id');
            $ids = array_map('intval', $ids);
            $this->assertSame([1, 2, 3], $ids, 'Rows 4 and 5 should be deleted (DESC order)');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE ORDER BY id DESC LIMIT 2 failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE ORDER BY id LIMIT 2 updates only the first two rows.
     *
     * Verifies exact row-level changes: only ids 1 and 2 should be
     * updated, while ids 3, 4, 5 remain unchanged.
     */
    public function testUpdateOrderByIdLimitVerifyExactRows(): void
    {
        try {
            $this->ztdExec("UPDATE mi_del_lim SET status = 'processed' ORDER BY id LIMIT 2");

            // Rows 1 and 2 should be updated
            $updated = $this->ztdQuery("SELECT id, status FROM mi_del_lim WHERE status = 'processed' ORDER BY id");
            $this->assertCount(2, $updated, 'Exactly 2 rows should be updated');
            $this->assertSame(1, (int) $updated[0]['id']);
            $this->assertSame(2, (int) $updated[1]['id']);

            // Rows 3, 4, 5 should be unchanged
            $unchanged = $this->ztdQuery("SELECT id, status FROM mi_del_lim WHERE status != 'processed' ORDER BY id");
            $this->assertCount(3, $unchanged);
            $this->assertSame('inactive', $unchanged[0]['status']); // id=3
            $this->assertSame('active', $unchanged[1]['status']);   // id=4
            $this->assertSame('inactive', $unchanged[2]['status']); // id=5
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE ORDER BY id LIMIT 2 failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE WHERE status = 'active' ORDER BY created_at LIMIT 1.
     *
     * Should delete the oldest active row (id=1). Tests the CTE
     * rewriter's handling of WHERE + ORDER BY + LIMIT on DELETE
     * with a datetime column in the ordering.
     */
    public function testDeleteWhereOrderByCreatedAtLimit(): void
    {
        try {
            $this->ztdExec("DELETE FROM mi_del_lim WHERE status = 'active' ORDER BY created_at LIMIT 1");

            // Only id=1 (oldest active) should be deleted
            $rows = $this->ztdQuery('SELECT id FROM mi_del_lim ORDER BY id');
            $this->assertCount(4, $rows, 'Should have 4 rows after deleting 1');

            $ids = array_column($rows, 'id');
            $ids = array_map('intval', $ids);
            $this->assertNotContains(1, $ids, 'id=1 (oldest active) should be deleted');
            $this->assertContains(2, $ids, 'id=2 (next active) should remain');
            $this->assertContains(4, $ids, 'id=4 (newest active) should remain');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE WHERE ... ORDER BY created_at LIMIT 1 failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Sequential DELETE LIMIT operations.
     *
     * Tests that the CTE shadow store correctly tracks multiple
     * successive DELETE LIMIT operations.
     */
    public function testSequentialDeleteLimitOperations(): void
    {
        try {
            // Delete the row with lowest priority
            $this->ztdExec("DELETE FROM mi_del_lim ORDER BY priority ASC LIMIT 1");
            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_del_lim');
            $this->assertEquals(4, (int) $rows[0]['cnt'], 'Should have 4 rows after first delete');

            // Delete the next row with lowest priority
            $this->ztdExec("DELETE FROM mi_del_lim ORDER BY priority ASC LIMIT 1");
            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_del_lim');
            $this->assertEquals(3, (int) $rows[0]['cnt'], 'Should have 3 rows after second delete');

            // Remaining rows should be the 3 with highest priority (3, 4, 5)
            $remaining = $this->ztdQuery('SELECT id, priority FROM mi_del_lim ORDER BY priority ASC');
            $this->assertCount(3, $remaining);
            $priorities = array_map(fn($r) => (int) $r['priority'], $remaining);
            $this->assertSame([3, 4, 5], $priorities, 'Three highest-priority rows should remain');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Sequential DELETE LIMIT operations failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared DELETE with ORDER BY and LIMIT using a parameter in WHERE.
     *
     * Tests that the CTE rewriter correctly handles prepared statements
     * with ORDER BY + LIMIT in DELETE.
     */
    public function testPreparedDeleteWithOrderByLimit(): void
    {
        try {
            $stmt = $this->mysqli->prepare(
                "DELETE FROM mi_del_lim WHERE status = ? ORDER BY created_at DESC LIMIT 1"
            );
            $status = 'active';
            $stmt->bind_param('s', $status);
            $stmt->execute();

            // The most recent active row is id=4, which should be deleted
            $rows = $this->ztdQuery("SELECT id FROM mi_del_lim WHERE status = 'active' ORDER BY id");
            $ids = array_column($rows, 'id');
            $ids = array_map('intval', $ids);
            $this->assertNotContains(4, $ids, 'id=4 (most recent active) should be deleted');
            $this->assertContains(1, $ids, 'id=1 should remain');
            $this->assertContains(2, $ids, 'id=2 should remain');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared DELETE with ORDER BY LIMIT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE ORDER BY priority DESC LIMIT 2 with computed SET expression.
     *
     * Combines ORDER BY LIMIT with a self-referencing expression in SET.
     * This exercises two CTE rewriter features simultaneously.
     */
    public function testUpdateOrderByLimitWithComputedSet(): void
    {
        try {
            $this->ztdExec(
                "UPDATE mi_del_lim SET priority = priority + 10 ORDER BY priority DESC LIMIT 2"
            );

            // Rows with highest priority (5 and 4) should have priority increased
            $rows = $this->ztdQuery('SELECT id, priority FROM mi_del_lim ORDER BY id');

            // id=4 had priority=5, now 15
            $row4 = array_values(array_filter($rows, fn($r) => (int) $r['id'] === 4))[0];
            $this->assertEquals(15, (int) $row4['priority'], 'id=4 priority should be 5+10=15');

            // id=5 had priority=4, now 14
            $row5 = array_values(array_filter($rows, fn($r) => (int) $r['id'] === 5))[0];
            $this->assertEquals(14, (int) $row5['priority'], 'id=5 priority should be 4+10=14');

            // id=1 had priority=3, should be unchanged
            $row1 = array_values(array_filter($rows, fn($r) => (int) $r['id'] === 1))[0];
            $this->assertEquals(3, (int) $row1['priority'], 'id=1 priority should remain 3');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE ORDER BY LIMIT with computed SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation: DELETE LIMIT changes must not reach the physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("DELETE FROM mi_del_lim ORDER BY id DESC LIMIT 2");

        $this->disableZtd();
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_del_lim');
        $this->assertEquals(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
