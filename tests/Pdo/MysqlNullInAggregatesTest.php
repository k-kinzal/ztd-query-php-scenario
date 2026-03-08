<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Extended NULL handling: NULL behavior in aggregates, JOINs, arithmetic,
 * BETWEEN, and string expressions through the shadow store.
 * @spec SPEC-3.7
 */
class MysqlNullInAggregatesTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE scores (id INT AUTO_INCREMENT PRIMARY KEY, student VARCHAR(50), subject VARCHAR(50), score INT)',
            'CREATE TABLE profiles (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50), city VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['scores', 'profiles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO scores (id, student, subject, score) VALUES (1, 'Alice', 'Math', 90)");
        $this->pdo->exec("INSERT INTO scores (id, student, subject, score) VALUES (2, 'Alice', 'English', NULL)");
        $this->pdo->exec("INSERT INTO scores (id, student, subject, score) VALUES (3, 'Bob', 'Math', 80)");
        $this->pdo->exec("INSERT INTO scores (id, student, subject, score) VALUES (4, 'Bob', 'English', NULL)");
        $this->pdo->exec("INSERT INTO scores (id, student, subject, score) VALUES (5, 'Charlie', 'Math', NULL)");
        $this->pdo->exec("INSERT INTO scores (id, student, subject, score) VALUES (6, 'Charlie', 'English', NULL)");

        $this->pdo->exec("INSERT INTO profiles (id, name, city) VALUES (1, 'Alice', 'NYC')");
        $this->pdo->exec("INSERT INTO profiles (id, name, city) VALUES (2, 'Bob', NULL)");
        $this->pdo->exec("INSERT INTO profiles (id, name, city) VALUES (3, 'Dave', 'LA')");
    }

    public function testCountStarVsCountColumn(): void
    {
        $rows = $this->ztdQuery("
            SELECT student,
                   COUNT(*) AS total_rows,
                   COUNT(score) AS scored_rows
            FROM scores
            GROUP BY student
            ORDER BY student
        ");
        // Alice: 2 rows, 1 non-null score
        $this->assertSame(2, (int) $rows[0]['total_rows']);
        $this->assertSame(1, (int) $rows[0]['scored_rows']);
        // Charlie: 2 rows, 0 non-null scores
        $this->assertSame(2, (int) $rows[2]['total_rows']);
        $this->assertSame(0, (int) $rows[2]['scored_rows']);
    }

    public function testCountDistinctWithNulls(): void
    {
        $rows = $this->ztdQuery("SELECT COUNT(DISTINCT score) AS distinct_scores FROM scores");
        // 90, 80 -> 2 distinct (NULL not counted)
        $this->assertSame(2, (int) $rows[0]['distinct_scores']);
    }

    public function testSumAvgWithAllNullGroup(): void
    {
        $rows = $this->ztdQuery("
            SELECT student,
                   SUM(score) AS total,
                   AVG(score) AS average
            FROM scores
            GROUP BY student
            ORDER BY student
        ");
        // Alice: SUM=90, AVG=90
        $this->assertSame(90, (int) $rows[0]['total']);
        $this->assertEqualsWithDelta(90.0, (float) $rows[0]['average'], 0.01);
        // Charlie: all NULL -> SUM=NULL, AVG=NULL
        $this->assertNull($rows[2]['total']);
        $this->assertNull($rows[2]['average']);
    }

    public function testMinMaxWithNulls(): void
    {
        $rows = $this->ztdQuery("
            SELECT MIN(score) AS min_score, MAX(score) AS max_score FROM scores
        ");
        $this->assertSame(80, (int) $rows[0]['min_score']);
        $this->assertSame(90, (int) $rows[0]['max_score']);
    }

    public function testMinMaxAllNull(): void
    {
        $rows = $this->ztdQuery("
            SELECT MIN(score) AS mn, MAX(score) AS mx
            FROM scores
            WHERE student = 'Charlie'
        ");
        $this->assertNull($rows[0]['mn']);
        $this->assertNull($rows[0]['mx']);
    }

    public function testHavingWithNullAggregate(): void
    {
        // Students with at least one non-null score
        $rows = $this->ztdQuery("
            SELECT student, COUNT(score) AS scored
            FROM scores
            GROUP BY student
            HAVING COUNT(score) > 0
            ORDER BY student
        ");
        $this->assertCount(2, $rows); // Alice (1), Bob (1); Charlie excluded (0)
        $this->assertSame('Alice', $rows[0]['student']);
        $this->assertSame('Bob', $rows[1]['student']);
    }

    public function testHavingSumNullExclusion(): void
    {
        // Students where SUM is not null (i.e., has at least one score)
        $rows = $this->ztdQuery("
            SELECT student, SUM(score) AS total
            FROM scores
            GROUP BY student
            HAVING SUM(score) IS NOT NULL
            ORDER BY student
        ");
        $this->assertCount(2, $rows);
    }

    public function testNullInJoinCondition(): void
    {
        // LEFT JOIN where join column has NULLs
        $rows = $this->ztdQuery("
            SELECT s.student, p.city
            FROM scores s
            LEFT JOIN profiles p ON s.student = p.name
            WHERE s.subject = 'Math'
            ORDER BY s.student
        ");
        $this->assertCount(3, $rows);
        $this->assertSame('NYC', $rows[0]['city']); // Alice
        $this->assertNull($rows[1]['city']); // Bob has NULL city
        $this->assertNull($rows[2]['city']); // Charlie not in profiles
    }

    public function testNullInArithmetic(): void
    {
        $rows = $this->ztdQuery("
            SELECT student, subject, score, score + 10 AS boosted
            FROM scores
            ORDER BY id
        ");
        $this->assertSame(100, (int) $rows[0]['boosted']); // 90 + 10
        $this->assertNull($rows[1]['boosted']); // NULL + 10 = NULL
    }

    public function testNullInBetween(): void
    {
        $rows = $this->ztdQuery("
            SELECT COUNT(*) AS cnt FROM scores WHERE score BETWEEN 70 AND 100
        ");
        // Only 90 and 80 match; NULLs are excluded from BETWEEN
        $this->assertSame(2, (int) $rows[0]['cnt']);
    }

    public function testNullComparisonNeverTrue(): void
    {
        // NULL = NULL should not match
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM scores WHERE score = NULL");
        $this->assertSame(0, (int) $rows[0]['cnt']);

        // NULL != value should not match either
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM scores WHERE score != 90");
        // Only Bob's 80 matches; NULLs are excluded
        $this->assertSame(1, (int) $rows[0]['cnt']);
    }

    public function testCoalesceInAggregate(): void
    {
        $rows = $this->ztdQuery("
            SELECT student, SUM(COALESCE(score, 0)) AS total
            FROM scores
            GROUP BY student
            ORDER BY student
        ");
        $this->assertSame(90, (int) $rows[0]['total']); // Alice: 90 + 0
        $this->assertSame(80, (int) $rows[1]['total']); // Bob: 80 + 0
        $this->assertSame(0, (int) $rows[2]['total']); // Charlie: 0 + 0
    }

    public function testNullUpdateThenAggregate(): void
    {
        // Set Bob's math score to NULL
        $this->pdo->exec("UPDATE scores SET score = NULL WHERE id = 3");

        $rows = $this->ztdQuery("
            SELECT COUNT(score) AS non_null, COUNT(*) AS total FROM scores
        ");
        $this->assertSame(1, (int) $rows[0]['non_null']); // Only Alice Math
        $this->assertSame(6, (int) $rows[0]['total']);
    }

    public function testNullInCaseWithAggregate(): void
    {
        $rows = $this->ztdQuery("
            SELECT student,
                   SUM(CASE WHEN score IS NOT NULL THEN 1 ELSE 0 END) AS has_score,
                   SUM(CASE WHEN score IS NULL THEN 1 ELSE 0 END) AS missing
            FROM scores
            GROUP BY student
            ORDER BY student
        ");
        $this->assertSame(1, (int) $rows[0]['has_score']); // Alice
        $this->assertSame(1, (int) $rows[0]['missing']);
        $this->assertSame(0, (int) $rows[2]['has_score']); // Charlie
        $this->assertSame(2, (int) $rows[2]['missing']);
    }

    public function testGroupConcatWithNulls(): void
    {
        $rows = $this->ztdQuery("
            SELECT student, GROUP_CONCAT(score SEPARATOR ',') AS scores_csv
            FROM scores
            GROUP BY student
            ORDER BY student
        ");
        $this->assertSame('90', $rows[0]['scores_csv']); // Alice: 90 (NULL omitted)
        $this->assertNull($rows[2]['scores_csv']); // Charlie: all NULL
    }

    public function testPreparedNullInWhereIsNull(): void
    {
        // Prepared: find students with no score at all
        $rows = $this->ztdPrepareAndExecute(
            "SELECT DISTINCT student FROM scores WHERE score IS NULL ORDER BY student",
            []
        );
        $this->assertCount(3, $rows); // All have at least one NULL
    }
}
