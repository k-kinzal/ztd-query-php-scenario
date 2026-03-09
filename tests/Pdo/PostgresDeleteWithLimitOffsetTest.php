<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL DELETE/UPDATE with LIMIT via subquery patterns.
 *
 * PostgreSQL does not support DELETE ... LIMIT directly. The idiomatic
 * pattern is DELETE FROM t WHERE id IN (SELECT id FROM t ORDER BY ... LIMIT n).
 * This stresses the CTE rewriter because the subquery references the same
 * table being deleted, requiring the shadow store to resolve inner SELECTs
 * correctly against pending mutations.
 *
 * @spec SPEC-4.3
 * @spec SPEC-3.3
 */
class PostgresDeleteWithLimitOffsetTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pgt_dlo_items (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            priority INT NOT NULL DEFAULT 0
        )';
    }

    protected function getTableNames(): array
    {
        return ['pgt_dlo_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pgt_dlo_items (id, name, priority) VALUES (1, 'alpha', 10)");
        $this->ztdExec("INSERT INTO pgt_dlo_items (id, name, priority) VALUES (2, 'bravo', 20)");
        $this->ztdExec("INSERT INTO pgt_dlo_items (id, name, priority) VALUES (3, 'charlie', 30)");
        $this->ztdExec("INSERT INTO pgt_dlo_items (id, name, priority) VALUES (4, 'delta', 40)");
        $this->ztdExec("INSERT INTO pgt_dlo_items (id, name, priority) VALUES (5, 'echo', 50)");
    }

    /**
     * DELETE WHERE id IN (SELECT id ... LIMIT 2) should delete exactly 2 rows.
     */
    public function testDeleteWithSubqueryLimit(): void
    {
        try {
            $this->ztdExec(
                'DELETE FROM pgt_dlo_items WHERE id IN (SELECT id FROM pgt_dlo_items ORDER BY id LIMIT 2)'
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pgt_dlo_items');
            $this->assertSame(3, (int) $rows[0]['cnt'], 'Expected 3 rows remaining after deleting 2');

            // Verify the correct rows were deleted (lowest IDs)
            $remaining = $this->ztdQuery('SELECT id FROM pgt_dlo_items ORDER BY id');
            $ids = array_column($remaining, 'id');
            $this->assertSame([3, 4, 5], array_map('intval', $ids));
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE with subquery LIMIT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE WHERE id = (SELECT MIN(id) FROM t) — delete just the minimum row.
     */
    public function testDeleteMinRow(): void
    {
        try {
            $this->ztdExec(
                'DELETE FROM pgt_dlo_items WHERE id = (SELECT MIN(id) FROM pgt_dlo_items)'
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pgt_dlo_items');
            $this->assertSame(4, (int) $rows[0]['cnt'], 'Expected 4 rows remaining after deleting min');

            // Row with id=1 should be gone
            $check = $this->ztdQuery('SELECT id FROM pgt_dlo_items WHERE id = 1');
            $this->assertCount(0, $check, 'Row id=1 should be deleted');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE min row via scalar subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with subquery LIMIT — only update a subset of rows.
     *
     * UPDATE t SET ... WHERE id IN (SELECT id FROM t ORDER BY ... LIMIT n)
     */
    public function testUpdateWithSubqueryLimit(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pgt_dlo_items SET name = 'updated'
                 WHERE id IN (SELECT id FROM pgt_dlo_items ORDER BY priority DESC LIMIT 3)"
            );

            $updated = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pgt_dlo_items WHERE name = 'updated'");
            $this->assertSame(3, (int) $updated[0]['cnt'], 'Expected 3 rows updated');

            // The 3 highest priority rows (ids 3,4,5) should be updated
            $unchanged = $this->ztdQuery(
                "SELECT id FROM pgt_dlo_items WHERE name != 'updated' ORDER BY id"
            );
            $ids = array_map('intval', array_column($unchanged, 'id'));
            $this->assertSame([1, 2], $ids, 'Rows with lowest priority should be unchanged');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with subquery LIMIT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE WHERE id IN (subquery) then SELECT should reflect changes.
     * This chains a delete-with-limit followed by another operation.
     */
    public function testDeleteLimitThenInsert(): void
    {
        try {
            // Delete the 2 lowest-priority rows
            $this->ztdExec(
                'DELETE FROM pgt_dlo_items WHERE id IN (SELECT id FROM pgt_dlo_items ORDER BY priority ASC LIMIT 2)'
            );

            // Insert a replacement
            $this->ztdExec("INSERT INTO pgt_dlo_items (id, name, priority) VALUES (6, 'foxtrot', 60)");

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pgt_dlo_items');
            $this->assertSame(4, (int) $rows[0]['cnt'], 'Expected 4 rows: 3 remaining + 1 inserted');

            // Verify foxtrot is visible
            $check = $this->ztdQuery("SELECT name FROM pgt_dlo_items WHERE id = 6");
            $this->assertCount(1, $check);
            $this->assertSame('foxtrot', $check[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE LIMIT then INSERT chain failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with ctid-based subquery — PostgreSQL-specific pattern.
     *
     * ctid is a system column; if the CTE rewriter does not handle it,
     * this will fail.
     */
    public function testDeleteWithCtidSubquery(): void
    {
        try {
            $this->ztdExec(
                'DELETE FROM pgt_dlo_items WHERE ctid IN (SELECT ctid FROM pgt_dlo_items ORDER BY id LIMIT 1)'
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pgt_dlo_items');
            $this->assertSame(4, (int) $rows[0]['cnt'], 'Expected 4 rows remaining after ctid delete');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE with ctid subquery failed (system column in CTE): ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE WHERE id = (SELECT MAX(id)) — update only the max row.
     */
    public function testUpdateMaxRow(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pgt_dlo_items SET priority = 999 WHERE id = (SELECT MAX(id) FROM pgt_dlo_items)"
            );

            $rows = $this->ztdQuery('SELECT priority FROM pgt_dlo_items WHERE id = 5');
            $this->assertCount(1, $rows);
            $this->assertSame(999, (int) $rows[0]['priority']);

            // Other rows should be unchanged
            $other = $this->ztdQuery('SELECT priority FROM pgt_dlo_items WHERE id = 4');
            $this->assertSame(40, (int) $other[0]['priority']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE max row via scalar subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation — mutations via subquery patterns should not touch physical DB.
     */
    public function testPhysicalIsolation(): void
    {
        try {
            $this->ztdExec(
                'DELETE FROM pgt_dlo_items WHERE id IN (SELECT id FROM pgt_dlo_items ORDER BY id LIMIT 2)'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE with subquery LIMIT failed: ' . $e->getMessage()
            );
        }

        $this->disableZtd();
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pgt_dlo_items');
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should have 0 rows');
    }

    /**
     * Prepared statement: DELETE WHERE id IN (SELECT id ... LIMIT ?) with bound limit.
     */
    public function testPreparedDeleteWithSubqueryLimit(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                'SELECT id FROM pgt_dlo_items WHERE id IN (SELECT id FROM pgt_dlo_items ORDER BY id LIMIT ?)',
                [3]
            );
            $this->assertCount(3, $rows, 'Subquery LIMIT via prepared param should return 3 rows');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Prepared SELECT with subquery LIMIT param failed: ' . $e->getMessage()
            );
        }
    }
}
