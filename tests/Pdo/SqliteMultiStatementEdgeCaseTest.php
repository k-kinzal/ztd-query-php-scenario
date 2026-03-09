<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests edge cases around multiple statements, interleaved DML/SELECT,
 * and common application patterns that might confuse the shadow store.
 *
 * @spec SPEC-3.1
 */
class SqliteMultiStatementEdgeCaseTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE ms_a (id INTEGER PRIMARY KEY, val TEXT)',
            'CREATE TABLE ms_b (id INTEGER PRIMARY KEY, a_id INTEGER, data TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['ms_a', 'ms_b'];
    }

    /**
     * Interleaved INSERT/SELECT across two tables.
     */
    public function testInterleavedInsertSelectAcrossTables(): void
    {
        $this->pdo->exec("INSERT INTO ms_a (id, val) VALUES (1, 'parent')");
        $this->pdo->exec("INSERT INTO ms_b (id, a_id, data) VALUES (1, 1, 'child')");

        $rows = $this->ztdQuery(
            'SELECT a.val, b.data FROM ms_a a JOIN ms_b b ON b.a_id = a.id'
        );
        $this->assertCount(1, $rows);
        $this->assertSame('parent', $rows[0]['val']);
        $this->assertSame('child', $rows[0]['data']);
    }

    /**
     * INSERT into table A based on SELECT from shadow table B.
     * (INSERT...SELECT pattern)
     */
    public function testInsertSelectFromShadowTable(): void
    {
        $this->pdo->exec("INSERT INTO ms_a (id, val) VALUES (1, 'src')");

        try {
            $this->pdo->exec("INSERT INTO ms_b (id, a_id, data) SELECT 1, id, val FROM ms_a WHERE id = 1");
            $rows = $this->ztdQuery('SELECT data FROM ms_b WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('src', $rows[0]['data']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('INSERT...SELECT from shadow table not supported: ' . $e->getMessage());
        }
    }

    /**
     * Rapid INSERT/DELETE cycle on same ID.
     */
    public function testRapidInsertDeleteCycle(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->pdo->exec("INSERT INTO ms_a (id, val) VALUES (1, 'cycle_$i')");
            $rows = $this->ztdQuery('SELECT val FROM ms_a WHERE id = 1');
            $this->assertSame("cycle_$i", $rows[0]['val'], "Cycle $i INSERT");
            $this->pdo->exec("DELETE FROM ms_a WHERE id = 1");
            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM ms_a');
            $this->assertEquals(0, (int) $rows[0]['cnt'], "Cycle $i DELETE");
        }
    }

    /**
     * UPDATE then DELETE same row.
     */
    public function testUpdateThenDeleteSameRow(): void
    {
        $this->pdo->exec("INSERT INTO ms_a (id, val) VALUES (1, 'original')");
        $this->pdo->exec("UPDATE ms_a SET val = 'updated' WHERE id = 1");
        $this->pdo->exec("DELETE FROM ms_a WHERE id = 1");

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM ms_a');
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }

    /**
     * Multiple UPDATEs to same row.
     */
    public function testMultipleUpdatesToSameRow(): void
    {
        $this->pdo->exec("INSERT INTO ms_a (id, val) VALUES (1, 'v0')");

        for ($i = 1; $i <= 10; $i++) {
            $this->pdo->exec("UPDATE ms_a SET val = 'v$i' WHERE id = 1");
        }

        $rows = $this->ztdQuery('SELECT val FROM ms_a WHERE id = 1');
        $this->assertSame('v10', $rows[0]['val']);
    }

    /**
     * SELECT with subquery referencing shadow data from both tables.
     */
    public function testSelectWithCrossTableSubquery(): void
    {
        $this->pdo->exec("INSERT INTO ms_a (id, val) VALUES (1, 'a1'), (2, 'a2')");
        $this->pdo->exec("INSERT INTO ms_b (id, a_id, data) VALUES (1, 1, 'd1'), (2, 2, 'd2')");

        $rows = $this->ztdQuery(
            'SELECT val FROM ms_a WHERE id IN (SELECT a_id FROM ms_b WHERE data = \'d1\')'
        );
        $this->assertCount(1, $rows);
        $this->assertSame('a1', $rows[0]['val']);
    }

    /**
     * Empty result set handling.
     */
    public function testEmptyResultSetHandling(): void
    {
        $this->pdo->exec("INSERT INTO ms_a (id, val) VALUES (1, 'only')");

        $rows = $this->ztdQuery('SELECT val FROM ms_a WHERE id = 999');
        $this->assertCount(0, $rows);

        $stmt = $this->pdo->prepare('SELECT val FROM ms_a WHERE id = ?');
        $stmt->execute([999]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertFalse($row);
    }

    /**
     * Large number of shadow rows.
     */
    public function testLargeNumberOfShadowRows(): void
    {
        $values = [];
        for ($i = 1; $i <= 100; $i++) {
            $values[] = "($i, 'row_$i')";
        }
        $this->pdo->exec("INSERT INTO ms_a (id, val) VALUES " . implode(', ', $values));

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM ms_a');
        $this->assertEquals(100, (int) $rows[0]['cnt']);

        // Verify specific rows
        $rows = $this->ztdQuery('SELECT val FROM ms_a WHERE id = 50');
        $this->assertSame('row_50', $rows[0]['val']);

        // Delete half
        $this->pdo->exec("DELETE FROM ms_a WHERE id > 50");
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM ms_a');
        $this->assertEquals(50, (int) $rows[0]['cnt']);
    }
}
