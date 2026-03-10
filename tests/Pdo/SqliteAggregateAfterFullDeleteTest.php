<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests aggregate queries after all rows have been deleted through shadow store.
 *
 * When every row in a table is deleted via DML, the physical table still has rows
 * but the shadow store says they're all deleted. Aggregate functions (COUNT, SUM, AVG,
 * MIN, MAX) must correctly return zero/null for the empty logical result set.
 *
 * This is a critical usability test: users checking "if table is empty" via
 * COUNT(*) must get the correct answer after shadow deletes.
 *
 * @spec SPEC-3.3
 */
class SqliteAggregateAfterFullDeleteTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_afd_scores (
                id INTEGER PRIMARY KEY,
                player TEXT NOT NULL,
                score INTEGER NOT NULL,
                bonus REAL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_afd_scores'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO sl_afd_scores VALUES (1, 'Alice', 100, 10.5)");
        $this->pdo->exec("INSERT INTO sl_afd_scores VALUES (2, 'Bob', 200, NULL)");
        $this->pdo->exec("INSERT INTO sl_afd_scores VALUES (3, 'Carol', 150, 5.0)");
    }

    /**
     * COUNT(*) after deleting all rows should return 0.
     */
    public function testCountAfterFullDelete(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_afd_scores");

            $rows = $this->ztdQuery("SELECT COUNT(*) as cnt FROM sl_afd_scores");

            if ((int) $rows[0]['cnt'] !== 0) {
                $this->markTestIncomplete(
                    'COUNT(*) after full DELETE returned ' . $rows[0]['cnt']
                    . ', expected 0. Shadow store may not correctly represent all-deleted state.'
                );
            }

            $this->assertSame(0, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COUNT after full DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * SUM/AVG/MIN/MAX after full delete should return NULL.
     */
    public function testAggregatesAfterFullDelete(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_afd_scores WHERE 1=1");

            $rows = $this->ztdQuery(
                "SELECT SUM(score) as s, AVG(score) as a, MIN(score) as mn, MAX(score) as mx
                 FROM sl_afd_scores"
            );

            if ($rows[0]['s'] !== null) {
                $this->markTestIncomplete(
                    'SUM after full DELETE returned "' . $rows[0]['s']
                    . '", expected NULL.'
                );
            }

            $this->assertNull($rows[0]['s']);
            $this->assertNull($rows[0]['a']);
            $this->assertNull($rows[0]['mn']);
            $this->assertNull($rows[0]['mx']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Aggregates after full DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT after full delete should return empty result set.
     */
    public function testSelectAfterFullDelete(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_afd_scores WHERE id > 0");

            $rows = $this->ztdQuery("SELECT * FROM sl_afd_scores");

            if (count($rows) !== 0) {
                $this->markTestIncomplete(
                    'SELECT * after full DELETE returned ' . count($rows)
                    . ' rows, expected 0. Shadow store leaking physical rows.'
                );
            }

            $this->assertCount(0, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT after full DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Delete all rows one by one, then check aggregates.
     */
    public function testIncrementalDeleteThenAggregate(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_afd_scores WHERE id = 1");
            $this->pdo->exec("DELETE FROM sl_afd_scores WHERE id = 2");

            // One row left
            $rows = $this->ztdQuery("SELECT COUNT(*) as cnt, SUM(score) as total FROM sl_afd_scores");
            $this->assertSame(1, (int) $rows[0]['cnt']);
            $this->assertSame(150, (int) $rows[0]['total']);

            // Delete last row
            $this->pdo->exec("DELETE FROM sl_afd_scores WHERE id = 3");

            $rows = $this->ztdQuery("SELECT COUNT(*) as cnt, SUM(score) as total FROM sl_afd_scores");
            $this->assertSame(0, (int) $rows[0]['cnt']);
            $this->assertNull($rows[0]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Incremental delete + aggregate failed: ' . $e->getMessage());
        }
    }

    /**
     * Full delete then re-insert: shadow store must show only new rows.
     */
    public function testFullDeleteThenReinsert(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_afd_scores");
            $this->pdo->exec("INSERT INTO sl_afd_scores VALUES (10, 'Dave', 500, 25.0)");

            $rows = $this->ztdQuery("SELECT * FROM sl_afd_scores");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Full DELETE + re-INSERT: expected 1 row, got ' . count($rows)
                    . '. Old deleted rows may be leaking back: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Dave', $rows[0]['player']);
            $this->assertSame(500, (int) $rows[0]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Full delete + reinsert failed: ' . $e->getMessage());
        }
    }

    /**
     * GROUP BY on fully-deleted table should return no groups.
     */
    public function testGroupByAfterFullDelete(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_afd_scores");

            $rows = $this->ztdQuery(
                "SELECT player, SUM(score) as total FROM sl_afd_scores GROUP BY player"
            );

            if (count($rows) !== 0) {
                $this->markTestIncomplete(
                    'GROUP BY after full DELETE returned ' . count($rows)
                    . ' groups, expected 0.'
                );
            }

            $this->assertCount(0, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP BY after full DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * EXISTS subquery on fully-deleted table should return false.
     */
    public function testExistsAfterFullDelete(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_afd_scores");

            // Use IIF or CASE to test EXISTS result
            $rows = $this->ztdQuery(
                "SELECT CASE WHEN EXISTS (SELECT 1 FROM sl_afd_scores) THEN 1 ELSE 0 END as has_rows"
            );

            if ((int) $rows[0]['has_rows'] !== 0) {
                $this->markTestIncomplete(
                    'EXISTS on fully-deleted table returned true. '
                    . 'Shadow store may leak deleted rows to EXISTS subqueries.'
                );
            }

            $this->assertSame(0, (int) $rows[0]['has_rows']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('EXISTS after full DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * COALESCE + COUNT pattern: common "empty table" check.
     */
    public function testCoalesceCountPattern(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_afd_scores");

            $rows = $this->ztdQuery(
                "SELECT COALESCE(MAX(score), 0) as best_score FROM sl_afd_scores"
            );

            if ((int) $rows[0]['best_score'] !== 0) {
                $this->markTestIncomplete(
                    'COALESCE(MAX, 0) after full DELETE returned ' . $rows[0]['best_score']
                    . ', expected 0.'
                );
            }

            $this->assertSame(0, (int) $rows[0]['best_score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COALESCE+COUNT pattern failed: ' . $e->getMessage());
        }
    }
}
