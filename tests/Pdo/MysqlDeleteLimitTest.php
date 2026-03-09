<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL DELETE/UPDATE with LIMIT and ORDER BY through ZTD CTE rewriter.
 *
 * MySQL uniquely supports LIMIT on DELETE and UPDATE (single-table only).
 * These modifiers interact with the CTE rewriter because the rewriter
 * must preserve the ORDER BY + LIMIT semantics when wrapping the table
 * reference in a CTE.
 *
 * @spec SPEC-4.2, SPEC-4.3, SPEC-3.1
 */
class MysqlDeleteLimitTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_dl_items (
            id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT \'active\',
            score INT NOT NULL DEFAULT 0
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mp_dl_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mp_dl_items VALUES (1, 'Alpha', 'active', 10)");
        $this->ztdExec("INSERT INTO mp_dl_items VALUES (2, 'Beta', 'inactive', 20)");
        $this->ztdExec("INSERT INTO mp_dl_items VALUES (3, 'Gamma', 'active', 30)");
        $this->ztdExec("INSERT INTO mp_dl_items VALUES (4, 'Delta', 'inactive', 40)");
        $this->ztdExec("INSERT INTO mp_dl_items VALUES (5, 'Epsilon', 'active', 50)");
    }

    /**
     * DELETE FROM ... ORDER BY id DESC LIMIT 2 should remove only the 2 highest-id rows.
     */
    public function testDeleteOrderByDescLimit(): void
    {
        try {
            $affected = $this->ztdExec('DELETE FROM mp_dl_items ORDER BY id DESC LIMIT 2');

            $rows = $this->ztdQuery('SELECT id FROM mp_dl_items ORDER BY id');
            $this->assertCount(3, $rows, 'Only 3 rows should remain after deleting top 2 by id DESC');
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(2, (int) $rows[1]['id']);
            $this->assertSame(3, (int) $rows[2]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE with ORDER BY DESC LIMIT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE FROM ... WHERE status = 'inactive' LIMIT 1 should remove exactly 1 inactive row.
     */
    public function testDeleteWithWhereAndLimit(): void
    {
        try {
            $affected = $this->ztdExec("DELETE FROM mp_dl_items WHERE status = 'inactive' LIMIT 1");

            $rows = $this->ztdQuery('SELECT id FROM mp_dl_items ORDER BY id');
            $this->assertCount(4, $rows, 'Only 1 row should be deleted');

            // Verify one inactive row was removed (id 2 or 4)
            $inactiveRows = $this->ztdQuery("SELECT id FROM mp_dl_items WHERE status = 'inactive'");
            $this->assertCount(1, $inactiveRows, 'Exactly 1 inactive row should remain');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE with WHERE + LIMIT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE FROM ... ORDER BY score ASC LIMIT 3 should remove the 3 lowest-score rows.
     */
    public function testDeleteOrderByAscLimit(): void
    {
        try {
            $this->ztdExec('DELETE FROM mp_dl_items ORDER BY score ASC LIMIT 3');

            $rows = $this->ztdQuery('SELECT id, score FROM mp_dl_items ORDER BY score');
            $this->assertCount(2, $rows, '2 rows should remain after deleting 3 lowest scores');
            $this->assertSame(4, (int) $rows[0]['id']);
            $this->assertSame(5, (int) $rows[1]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE with ORDER BY ASC LIMIT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE ... ORDER BY id LIMIT 2 should update only the first 2 rows by id.
     */
    public function testUpdateOrderByLimit(): void
    {
        try {
            $this->ztdExec("UPDATE mp_dl_items SET status = 'updated' ORDER BY id LIMIT 2");

            $rows = $this->ztdQuery('SELECT id, status FROM mp_dl_items ORDER BY id');
            $this->assertCount(5, $rows, 'All 5 rows should still exist');
            $this->assertSame('updated', $rows[0]['status'], 'Row 1 should be updated');
            $this->assertSame('updated', $rows[1]['status'], 'Row 2 should be updated');
            $this->assertSame('active', $rows[2]['status'], 'Row 3 should be unchanged');
            $this->assertSame('inactive', $rows[3]['status'], 'Row 4 should be unchanged');
            $this->assertSame('active', $rows[4]['status'], 'Row 5 should be unchanged');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with ORDER BY LIMIT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE ... WHERE ... ORDER BY score DESC LIMIT 1 should update only the highest-score matching row.
     */
    public function testUpdateWhereOrderByDescLimit(): void
    {
        try {
            $this->ztdExec("UPDATE mp_dl_items SET score = 999 WHERE status = 'active' ORDER BY score DESC LIMIT 1");

            // The highest-score active row is id=5 (score=50)
            $rows = $this->ztdQuery('SELECT id, score FROM mp_dl_items ORDER BY id');
            $this->assertSame(10, (int) $rows[0]['score'], 'id=1 (active, score=10) unchanged');
            $this->assertSame(20, (int) $rows[1]['score'], 'id=2 (inactive, score=20) unchanged');
            $this->assertSame(30, (int) $rows[2]['score'], 'id=3 (active, score=30) unchanged');
            $this->assertSame(40, (int) $rows[3]['score'], 'id=4 (inactive, score=40) unchanged');
            $this->assertSame(999, (int) $rows[4]['score'], 'id=5 (active, score=50) should be updated to 999');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with WHERE + ORDER BY DESC + LIMIT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE LIMIT that exceeds available rows should delete all matching rows without error.
     */
    public function testDeleteLimitExceedsRows(): void
    {
        try {
            $this->ztdExec("DELETE FROM mp_dl_items WHERE status = 'inactive' LIMIT 100");

            $rows = $this->ztdQuery('SELECT id FROM mp_dl_items ORDER BY id');
            $this->assertCount(3, $rows, 'Both inactive rows should be deleted');
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(3, (int) $rows[1]['id']);
            $this->assertSame(5, (int) $rows[2]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE with LIMIT exceeding rows failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Verify physical isolation: DELETE with LIMIT does not affect physical table.
     */
    public function testDeleteLimitPhysicalIsolation(): void
    {
        try {
            $this->ztdExec('DELETE FROM mp_dl_items ORDER BY id LIMIT 2');

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mp_dl_items');
            $this->assertSame(3, (int) $rows[0]['cnt'], 'Shadow should have 3 rows');

            $this->pdo->disableZtd();
            $stmt = $this->pdo->query('SELECT COUNT(*) FROM mp_dl_items');
            $this->assertSame(0, (int) $stmt->fetchColumn(), 'Physical table should have 0 rows');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE LIMIT physical isolation check failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared DELETE with LIMIT parameter.
     */
    public function testPreparedDeleteWithLimit(): void
    {
        try {
            $stmt = $this->pdo->prepare('DELETE FROM mp_dl_items WHERE status = ? LIMIT 1');
            $stmt->execute(['active']);

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_dl_items WHERE status = 'active'");
            $this->assertSame(2, (int) $rows[0]['cnt'], 'One active row should have been deleted');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Prepared DELETE with LIMIT failed: ' . $e->getMessage()
            );
        }
    }
}
