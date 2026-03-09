<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests LIMIT/OFFSET accuracy after shadow mutations change row count.
 *
 * Verifies that the correct subset of rows is returned after INSERT/DELETE/UPDATE
 * operations change the total number of rows in the shadow store.
 *
 * @spec SPEC-3.1
 */
class SqliteLimitOffsetMutationAccuracyTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE lo_t (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['lo_t'];
    }

    private function seedFiveRows(): void
    {
        $this->pdo->exec("INSERT INTO lo_t (id, name, score) VALUES
            (1, 'Alice', 90),
            (2, 'Bob', 80),
            (3, 'Charlie', 70),
            (4, 'Diana', 60),
            (5, 'Eve', 50)");
    }

    /**
     * LIMIT 3 OFFSET 2 after inserting 5 rows returns correct subset.
     */
    public function testLimitOffsetReturnsCorrectSubset(): void
    {
        $this->seedFiveRows();

        $rows = $this->ztdQuery('SELECT name FROM lo_t ORDER BY id LIMIT 3 OFFSET 2');
        $this->assertCount(3, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
        $this->assertSame('Diana', $rows[1]['name']);
        $this->assertSame('Eve', $rows[2]['name']);
    }

    /**
     * After DELETE, LIMIT returns fewer rows when offset exceeds remaining.
     */
    public function testLimitAfterDeleteReducesResults(): void
    {
        $this->seedFiveRows();

        // Delete 2 rows (Alice, Bob)
        $this->pdo->exec("DELETE FROM lo_t WHERE id IN (1, 2)");

        // LIMIT 3 OFFSET 1 — only 3 rows total, skip 1, return 2
        $rows = $this->ztdQuery('SELECT name FROM lo_t ORDER BY id LIMIT 3 OFFSET 1');
        $this->assertCount(2, $rows);
        $this->assertSame('Diana', $rows[0]['name']);
        $this->assertSame('Eve', $rows[1]['name']);
    }

    /**
     * After INSERT, LIMIT returns the new rows.
     */
    public function testLimitAfterInsertIncludesNewRows(): void
    {
        $this->seedFiveRows();

        // Insert 2 more rows
        $this->pdo->exec("INSERT INTO lo_t (id, name, score) VALUES (6, 'Frank', 40), (7, 'Grace', 30)");

        $rows = $this->ztdQuery('SELECT name FROM lo_t ORDER BY id LIMIT 3 OFFSET 5');
        $this->assertCount(2, $rows);
        $this->assertSame('Frank', $rows[0]['name']);
        $this->assertSame('Grace', $rows[1]['name']);
    }

    /**
     * After UPDATE, LIMIT with ORDER BY reflects updated values.
     */
    public function testLimitWithOrderByAfterUpdate(): void
    {
        $this->seedFiveRows();

        // Make Eve the top scorer
        $this->pdo->exec("UPDATE lo_t SET score = 999 WHERE id = 5");

        $rows = $this->ztdQuery('SELECT name, score FROM lo_t ORDER BY score DESC LIMIT 1');
        $this->assertCount(1, $rows);
        $this->assertSame('Eve', $rows[0]['name']);
        $this->assertEquals(999, (int) $rows[0]['score']);
    }

    /**
     * Prepared LIMIT/OFFSET with shadow mutations.
     */
    public function testPreparedLimitOffsetAfterMutation(): void
    {
        $this->seedFiveRows();

        $stmt = $this->pdo->prepare('SELECT name FROM lo_t ORDER BY id LIMIT ? OFFSET ?');
        $stmt->execute([2, 1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    /**
     * OFFSET beyond available rows returns empty.
     */
    public function testOffsetBeyondRowCountReturnsEmpty(): void
    {
        $this->seedFiveRows();
        $this->pdo->exec("DELETE FROM lo_t WHERE id > 2"); // Keep only 2 rows

        $rows = $this->ztdQuery('SELECT name FROM lo_t ORDER BY id LIMIT 10 OFFSET 5');
        $this->assertCount(0, $rows);
    }

    /**
     * LIMIT 1 for pagination — each page returns correct single row.
     */
    public function testSingleRowPagination(): void
    {
        $this->seedFiveRows();

        for ($i = 0; $i < 5; $i++) {
            $rows = $this->ztdQuery("SELECT name FROM lo_t ORDER BY id LIMIT 1 OFFSET $i");
            $this->assertCount(1, $rows, "Page $i should have 1 row");
        }

        // Page beyond data
        $rows = $this->ztdQuery('SELECT name FROM lo_t ORDER BY id LIMIT 1 OFFSET 5');
        $this->assertCount(0, $rows);
    }

    /**
     * Interleaved INSERT/DELETE with LIMIT verification.
     */
    public function testInterleavedMutationsWithLimit(): void
    {
        $this->pdo->exec("INSERT INTO lo_t (id, name, score) VALUES (1, 'A', 10), (2, 'B', 20)");

        $rows = $this->ztdQuery('SELECT name FROM lo_t ORDER BY id LIMIT 10');
        $this->assertCount(2, $rows);

        $this->pdo->exec("INSERT INTO lo_t (id, name, score) VALUES (3, 'C', 30)");
        $rows = $this->ztdQuery('SELECT name FROM lo_t ORDER BY id LIMIT 10');
        $this->assertCount(3, $rows);

        $this->pdo->exec("DELETE FROM lo_t WHERE id = 1");
        $rows = $this->ztdQuery('SELECT name FROM lo_t ORDER BY id LIMIT 10');
        $this->assertCount(2, $rows);
        $this->assertSame('B', $rows[0]['name']);
        $this->assertSame('C', $rows[1]['name']);

        $this->pdo->exec("INSERT INTO lo_t (id, name, score) VALUES (4, 'D', 40), (5, 'E', 50)");
        $rows = $this->ztdQuery('SELECT name FROM lo_t ORDER BY id LIMIT 2 OFFSET 1');
        $this->assertCount(2, $rows);
        $this->assertSame('C', $rows[0]['name']);
        $this->assertSame('D', $rows[1]['name']);
    }
}
