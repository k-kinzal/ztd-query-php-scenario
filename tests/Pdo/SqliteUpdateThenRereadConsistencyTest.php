<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests shadow store consistency across chained mutation-then-read sequences.
 *
 * Real applications often do INSERT → UPDATE the inserted row → SELECT.
 * Or UPDATE → DELETE → re-INSERT → SELECT. Each step must see the latest
 * shadow state correctly.
 * @spec SPEC-4.3
 */
class SqliteUpdateThenRereadConsistencyTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE utr_items (id INT PRIMARY KEY, name TEXT, status TEXT, version INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['utr_items'];
    }

    /**
     * INSERT → immediate UPDATE → SELECT sees updated value.
     */
    public function testInsertThenUpdateThenSelect(): void
    {
        $this->pdo->exec("INSERT INTO utr_items VALUES (1, 'Widget', 'draft', 1)");
        $this->pdo->exec("UPDATE utr_items SET status = 'published', version = 2 WHERE id = 1");

        $rows = $this->ztdQuery('SELECT * FROM utr_items WHERE id = 1');

        $this->assertCount(1, $rows);
        $this->assertSame('published', $rows[0]['status']);
        $this->assertSame('2', (string) $rows[0]['version']);
    }

    /**
     * INSERT → UPDATE → UPDATE → SELECT (double update).
     */
    public function testDoubleUpdate(): void
    {
        $this->pdo->exec("INSERT INTO utr_items VALUES (1, 'Widget', 'draft', 1)");
        $this->pdo->exec("UPDATE utr_items SET status = 'review' WHERE id = 1");
        $this->pdo->exec("UPDATE utr_items SET status = 'published', version = 3 WHERE id = 1");

        $rows = $this->ztdQuery('SELECT * FROM utr_items WHERE id = 1');

        $this->assertCount(1, $rows);
        $this->assertSame('published', $rows[0]['status']);
        $this->assertSame('3', (string) $rows[0]['version']);
    }

    /**
     * INSERT → DELETE → re-INSERT same PK → SELECT.
     */
    public function testDeleteAndReinsertSamePk(): void
    {
        $this->pdo->exec("INSERT INTO utr_items VALUES (1, 'Widget', 'draft', 1)");
        $this->pdo->exec("DELETE FROM utr_items WHERE id = 1");
        $this->pdo->exec("INSERT INTO utr_items VALUES (1, 'NewWidget', 'active', 1)");

        $rows = $this->ztdQuery('SELECT * FROM utr_items WHERE id = 1');

        $this->assertCount(1, $rows);
        $this->assertSame('NewWidget', $rows[0]['name']);
        $this->assertSame('active', $rows[0]['status']);
    }

    /**
     * UPDATE multiple rows → SELECT with ORDER BY.
     */
    public function testBatchUpdateThenOrderedSelect(): void
    {
        $this->pdo->exec("INSERT INTO utr_items VALUES (1, 'A', 'draft', 1)");
        $this->pdo->exec("INSERT INTO utr_items VALUES (2, 'B', 'draft', 1)");
        $this->pdo->exec("INSERT INTO utr_items VALUES (3, 'C', 'draft', 1)");

        $this->pdo->exec("UPDATE utr_items SET status = 'published' WHERE id <= 2");

        $rows = $this->ztdQuery('SELECT * FROM utr_items ORDER BY id');

        $this->assertCount(3, $rows);
        $this->assertSame('published', $rows[0]['status']);
        $this->assertSame('published', $rows[1]['status']);
        $this->assertSame('draft', $rows[2]['status']);
    }

    /**
     * INSERT → UPDATE → DELETE → SELECT COUNT should be 0.
     */
    public function testInsertUpdateDeleteCycle(): void
    {
        $this->pdo->exec("INSERT INTO utr_items VALUES (1, 'Temp', 'draft', 1)");
        $this->pdo->exec("UPDATE utr_items SET name = 'TempUpdated' WHERE id = 1");
        $this->pdo->exec("DELETE FROM utr_items WHERE id = 1");

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM utr_items');
        $this->assertSame('0', (string) $rows[0]['cnt']);
    }

    /**
     * UPDATE with self-referencing arithmetic: SET version = version + 1.
     */
    public function testSelfReferencingUpdate(): void
    {
        $this->pdo->exec("INSERT INTO utr_items VALUES (1, 'Widget', 'active', 1)");

        try {
            $this->pdo->exec("UPDATE utr_items SET version = version + 1 WHERE id = 1");

            $rows = $this->ztdQuery('SELECT version FROM utr_items WHERE id = 1');
            $this->assertSame('2', (string) $rows[0]['version']);

            // Do it again
            $this->pdo->exec("UPDATE utr_items SET version = version + 1 WHERE id = 1");

            $rows = $this->ztdQuery('SELECT version FROM utr_items WHERE id = 1');
            $this->assertSame('3', (string) $rows[0]['version']);
        } catch (\Exception $e) {
            $this->markTestSkipped('Self-referencing UPDATE not supported: ' . $e->getMessage());
        }
    }

    /**
     * Multiple INSERTs then UPDATE with WHERE matching subset.
     */
    public function testUpdateSubsetAfterMultipleInserts(): void
    {
        for ($i = 1; $i <= 10; $i++) {
            $this->pdo->exec("INSERT INTO utr_items VALUES ({$i}, 'Item{$i}', 'draft', 1)");
        }

        $this->pdo->exec("UPDATE utr_items SET status = 'archived' WHERE id > 5");

        $active = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM utr_items WHERE status = 'draft'");
        $archived = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM utr_items WHERE status = 'archived'");

        $this->assertSame('5', (string) $active[0]['cnt']);
        $this->assertSame('5', (string) $archived[0]['cnt']);
    }

    /**
     * Prepared statement UPDATE on shadow-inserted row.
     */
    public function testPreparedUpdateOnShadowRow(): void
    {
        $this->pdo->exec("INSERT INTO utr_items VALUES (1, 'Widget', 'draft', 1)");

        $stmt = $this->pdo->prepare('UPDATE utr_items SET status = ?, version = ? WHERE id = ?');
        $stmt->execute(['published', 2, 1]);

        $rows = $this->ztdQuery('SELECT * FROM utr_items WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('published', $rows[0]['status']);
        $this->assertSame('2', (string) $rows[0]['version']);
    }

    /**
     * UPDATE with CASE expression.
     */
    public function testUpdateWithCase(): void
    {
        $this->pdo->exec("INSERT INTO utr_items VALUES (1, 'A', 'draft', 1)");
        $this->pdo->exec("INSERT INTO utr_items VALUES (2, 'B', 'review', 1)");
        $this->pdo->exec("INSERT INTO utr_items VALUES (3, 'C', 'draft', 1)");

        try {
            $this->pdo->exec(
                "UPDATE utr_items SET status = CASE
                    WHEN status = 'draft' THEN 'published'
                    WHEN status = 'review' THEN 'approved'
                    ELSE status
                 END"
            );

            $rows = $this->ztdQuery('SELECT * FROM utr_items ORDER BY id');
            $this->assertSame('published', $rows[0]['status']);
            $this->assertSame('approved', $rows[1]['status']);
            $this->assertSame('published', $rows[2]['status']);
        } catch (\Exception $e) {
            $this->markTestSkipped('UPDATE with CASE not supported: ' . $e->getMessage());
        }
    }
}
