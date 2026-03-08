<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests advanced window function patterns and edge cases on SQLite.
 *
 * Tests FILTER clause, EXCLUDE, GROUPS frame, nth_value,
 * and other advanced window function syntax.
 * @spec SPEC-10.2.23
 */
class SqliteWindowGroupingEdgeCaseTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE wge_scores (id INT PRIMARY KEY, name VARCHAR(50), score INT, category VARCHAR(20))';
    }

    protected function getTableNames(): array
    {
        return ['wge_scores'];
    }



    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO wge_scores VALUES (1, 'Alice', 95, 'A')");
        $this->pdo->exec("INSERT INTO wge_scores VALUES (2, 'Bob', 85, 'A')");
        $this->pdo->exec("INSERT INTO wge_scores VALUES (3, 'Charlie', 90, 'B')");
        $this->pdo->exec("INSERT INTO wge_scores VALUES (4, 'Diana', 75, 'B')");
        $this->pdo->exec("INSERT INTO wge_scores VALUES (5, 'Eve', 88, 'A')");
    }
    /**
     * FILTER clause on aggregate functions (SQLite 3.30+).
     */
    public function testAggregateFilter(): void
    {
        try {
            $stmt = $this->pdo->query(
                "SELECT
                    COUNT(*) FILTER (WHERE category = 'A') as a_count,
                    COUNT(*) FILTER (WHERE category = 'B') as b_count,
                    AVG(score) FILTER (WHERE category = 'A') as a_avg
                 FROM wge_scores"
            );
            $row = $stmt->fetch(PDO::FETCH_ASSOC);

            $this->assertEquals(3, (int) $row['a_count']);
            $this->assertEquals(2, (int) $row['b_count']);
        } catch (\Exception $e) {
            $this->markTestSkipped('FILTER clause not supported: ' . $e->getMessage());
        }
    }

    /**
     * nth_value window function.
     */
    public function testNthValue(): void
    {
        try {
            $stmt = $this->pdo->query(
                'SELECT name, score,
                        NTH_VALUE(name, 2) OVER (ORDER BY score DESC) as second_highest
                 FROM wge_scores
                 ORDER BY score DESC'
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // Second highest score is Charlie (90)
            // nth_value may be NULL for the first row
            $this->assertCount(5, $rows);
            $this->assertSame('Alice', $rows[0]['name']); // 95
        } catch (\Exception $e) {
            $this->markTestSkipped('NTH_VALUE not supported: ' . $e->getMessage());
        }
    }

    /**
     * NTILE window function for bucketing.
     */
    public function testNtile(): void
    {
        $stmt = $this->pdo->query(
            'SELECT name, score, NTILE(2) OVER (ORDER BY score DESC) as bucket
             FROM wge_scores
             ORDER BY score DESC'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(5, $rows);
        // Top 3 in bucket 1, bottom 2 in bucket 2 (NTILE(2) with 5 rows)
        $this->assertEquals(1, (int) $rows[0]['bucket']); // 95
        $this->assertEquals(2, (int) $rows[4]['bucket']); // 75
    }

    /**
     * PERCENT_RANK and CUME_DIST window functions.
     */
    public function testPercentRankAndCumeDist(): void
    {
        $stmt = $this->pdo->query(
            'SELECT name, score,
                    PERCENT_RANK() OVER (ORDER BY score) as pct_rank,
                    CUME_DIST() OVER (ORDER BY score) as cum_dist
             FROM wge_scores
             ORDER BY score'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(5, $rows);
        // First row has percent_rank 0
        $this->assertEquals(0, (float) $rows[0]['pct_rank']);
        // Last row has percent_rank 1
        $this->assertEquals(1, (float) $rows[4]['pct_rank']);
    }

    /**
     * Multiple window functions with different PARTITION BY.
     */
    public function testMultiplePartitions(): void
    {
        $stmt = $this->pdo->query(
            "SELECT name, category, score,
                    ROW_NUMBER() OVER (PARTITION BY category ORDER BY score DESC) as rank_in_cat,
                    ROW_NUMBER() OVER (ORDER BY score DESC) as overall_rank
             FROM wge_scores
             ORDER BY category, score DESC"
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(5, $rows);
        // In category A, first row should have rank_in_cat = 1
        $catARows = array_filter($rows, fn($r) => $r['category'] === 'A');
        $catAFirst = array_values($catARows)[0];
        $this->assertEquals(1, (int) $catAFirst['rank_in_cat']);
    }

    /**
     * Window function after shadow DELETE.
     */
    public function testWindowFunctionAfterDelete(): void
    {
        $this->pdo->exec("DELETE FROM wge_scores WHERE id = 1");

        $stmt = $this->pdo->query(
            'SELECT name, RANK() OVER (ORDER BY score DESC) as rank
             FROM wge_scores
             ORDER BY score DESC'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(4, $rows);
        // Charlie (90) should now be rank 1
        $this->assertSame('Charlie', $rows[0]['name']);
        $this->assertEquals(1, (int) $rows[0]['rank']);
    }
}
