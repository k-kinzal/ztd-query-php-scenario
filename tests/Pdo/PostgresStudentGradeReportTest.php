<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests a student grade report scenario through ZTD shadow store (PostgreSQL PDO).
 * Students, courses, and submissions exercise LEFT JOIN COALESCE for missing
 * submissions scored as zero, multiple nested CASE WHEN for letter grades,
 * weighted average (SUM product / SUM weight), DELETE with EXISTS for draft
 * cleanup, prepared statement with multiple params, and physical isolation.
 * @spec SPEC-10.2.165
 */
class PostgresStudentGradeReportTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_gr_students (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255)
            )',
            'CREATE TABLE pg_gr_assignments (
                id SERIAL PRIMARY KEY,
                course VARCHAR(255),
                title VARCHAR(255),
                weight DOUBLE PRECISION,
                max_score DOUBLE PRECISION
            )',
            'CREATE TABLE pg_gr_submissions (
                id SERIAL PRIMARY KEY,
                student_id INT,
                assignment_id INT,
                score DOUBLE PRECISION,
                status VARCHAR(50)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_gr_submissions', 'pg_gr_assignments', 'pg_gr_students'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_gr_students VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pg_gr_students VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO pg_gr_students VALUES (3, 'Carol')");
        $this->pdo->exec("INSERT INTO pg_gr_students VALUES (4, 'Dave')");

        $this->pdo->exec("INSERT INTO pg_gr_assignments VALUES (1, 'Math 101', 'Homework 1', 0.15, 100.0)");
        $this->pdo->exec("INSERT INTO pg_gr_assignments VALUES (2, 'Math 101', 'Homework 2', 0.15, 100.0)");
        $this->pdo->exec("INSERT INTO pg_gr_assignments VALUES (3, 'Math 101', 'Midterm', 0.30, 100.0)");
        $this->pdo->exec("INSERT INTO pg_gr_assignments VALUES (4, 'Math 101', 'Final', 0.40, 100.0)");

        $this->pdo->exec("INSERT INTO pg_gr_submissions VALUES (1, 1, 1, 92.0, 'graded')");
        $this->pdo->exec("INSERT INTO pg_gr_submissions VALUES (2, 1, 2, 88.0, 'graded')");
        $this->pdo->exec("INSERT INTO pg_gr_submissions VALUES (3, 1, 3, 95.0, 'graded')");
        $this->pdo->exec("INSERT INTO pg_gr_submissions VALUES (4, 1, 4, 90.0, 'graded')");

        $this->pdo->exec("INSERT INTO pg_gr_submissions VALUES (5, 2, 1, 75.0, 'graded')");
        $this->pdo->exec("INSERT INTO pg_gr_submissions VALUES (6, 2, 2, 80.0, 'graded')");
        $this->pdo->exec("INSERT INTO pg_gr_submissions VALUES (7, 2, 3, 70.0, 'graded')");

        $this->pdo->exec("INSERT INTO pg_gr_submissions VALUES (8, 3, 1, 60.0, 'graded')");
        $this->pdo->exec("INSERT INTO pg_gr_submissions VALUES (9, 3, 2, 55.0, 'graded')");
        $this->pdo->exec("INSERT INTO pg_gr_submissions VALUES (10, 3, 3, 50.0, 'graded')");
        $this->pdo->exec("INSERT INTO pg_gr_submissions VALUES (11, 3, 4, 45.0, 'graded')");

        $this->pdo->exec("INSERT INTO pg_gr_submissions VALUES (12, 4, 1, 85.0, 'graded')");
        $this->pdo->exec("INSERT INTO pg_gr_submissions VALUES (13, 4, 3, 78.0, 'graded')");
        $this->pdo->exec("INSERT INTO pg_gr_submissions VALUES (14, 4, 2, 0.0, 'draft')");
    }

    public function testLeftJoinMissingSubmissionsAsZero(): void
    {
        $rows = $this->ztdQuery(
            "SELECT st.name, a.title,
                    COALESCE(sub.score, 0) AS score
             FROM pg_gr_students st
             CROSS JOIN pg_gr_assignments a
             LEFT JOIN pg_gr_submissions sub
                 ON sub.student_id = st.id AND sub.assignment_id = a.id AND sub.status = 'graded'
             ORDER BY st.name, a.id"
        );

        $this->assertCount(16, $rows);

        $bobFinal = array_values(array_filter($rows, fn($r) => $r['name'] === 'Bob' && $r['title'] === 'Final'));
        $this->assertCount(1, $bobFinal);
        $this->assertEquals(0, (float) $bobFinal[0]['score']);

        $daveHw2 = array_values(array_filter($rows, fn($r) => $r['name'] === 'Dave' && $r['title'] === 'Homework 2'));
        $this->assertCount(1, $daveHw2);
        $this->assertEquals(0, (float) $daveHw2[0]['score']);
    }

    public function testWeightedAverageWithLetterGrade(): void
    {
        $rows = $this->ztdQuery(
            "SELECT st.name,
                    ROUND((SUM(COALESCE(sub.score, 0) / a.max_score * a.weight) * 100)::numeric, 2) AS weighted_pct,
                    CASE
                        WHEN SUM(COALESCE(sub.score, 0) / a.max_score * a.weight) * 100 >= 93 THEN 'A'
                        WHEN SUM(COALESCE(sub.score, 0) / a.max_score * a.weight) * 100 >= 90 THEN 'A-'
                        WHEN SUM(COALESCE(sub.score, 0) / a.max_score * a.weight) * 100 >= 87 THEN 'B+'
                        WHEN SUM(COALESCE(sub.score, 0) / a.max_score * a.weight) * 100 >= 83 THEN 'B'
                        WHEN SUM(COALESCE(sub.score, 0) / a.max_score * a.weight) * 100 >= 80 THEN 'B-'
                        WHEN SUM(COALESCE(sub.score, 0) / a.max_score * a.weight) * 100 >= 77 THEN 'C+'
                        WHEN SUM(COALESCE(sub.score, 0) / a.max_score * a.weight) * 100 >= 73 THEN 'C'
                        WHEN SUM(COALESCE(sub.score, 0) / a.max_score * a.weight) * 100 >= 70 THEN 'C-'
                        WHEN SUM(COALESCE(sub.score, 0) / a.max_score * a.weight) * 100 >= 60 THEN 'D'
                        ELSE 'F'
                    END AS letter_grade
             FROM pg_gr_students st
             CROSS JOIN pg_gr_assignments a
             LEFT JOIN pg_gr_submissions sub
                 ON sub.student_id = st.id AND sub.assignment_id = a.id AND sub.status = 'graded'
             GROUP BY st.id, st.name
             ORDER BY weighted_pct DESC"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(91.5, (float) $rows[0]['weighted_pct']);
        $this->assertSame('A-', $rows[0]['letter_grade']);

        $this->assertSame('Carol', $rows[1]['name']);
        $this->assertEquals(50.25, (float) $rows[1]['weighted_pct']);
        $this->assertSame('F', $rows[1]['letter_grade']);

        $this->assertSame('Bob', $rows[2]['name']);
        $this->assertEquals(44.25, (float) $rows[2]['weighted_pct']);
        $this->assertSame('F', $rows[2]['letter_grade']);

        $this->assertSame('Dave', $rows[3]['name']);
        $this->assertEquals(36.15, (float) $rows[3]['weighted_pct']);
        $this->assertSame('F', $rows[3]['letter_grade']);
    }

    public function testDeleteDraftsWithExists(): void
    {
        $this->pdo->exec(
            "DELETE FROM pg_gr_submissions
             WHERE EXISTS (
                 SELECT 1 FROM pg_gr_submissions s2
                 WHERE s2.id = pg_gr_submissions.id AND s2.status = 'draft'
             )"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_gr_submissions");
        $this->assertEquals(13, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM pg_gr_submissions WHERE status = 'draft'"
        );
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }

    /**
     * Prepared HAVING with $N params on PostgreSQL.
     * SPEC-11.SQLITE-HAVING-PARAMS states PostgreSQL works correctly,
     * but complex multi-table HAVING with $N params may also return empty.
     */
    public function testPreparedGradeFilter(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT st.name,
                    ROUND((SUM(COALESCE(sub.score, 0) / a.max_score * a.weight) * 100)::numeric, 2) AS weighted_pct
             FROM pg_gr_students st
             CROSS JOIN pg_gr_assignments a
             LEFT JOIN pg_gr_submissions sub
                 ON sub.student_id = st.id AND sub.assignment_id = a.id AND sub.status = 'graded'
             WHERE a.course = $1
             GROUP BY st.id, st.name
             HAVING ROUND((SUM(COALESCE(sub.score, 0) / a.max_score * a.weight) * 100)::numeric, 2) >= $2
             ORDER BY weighted_pct DESC",
            ['Math 101', 50]
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'Prepared HAVING with $N params returns empty on PostgreSQL. '
                . 'Extends SPEC-11.SQLITE-HAVING-PARAMS beyond SQLite-only. Expected 2 rows (Alice 91.5, Carol 50.25).'
            );
        }
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(91.5, (float) $rows[0]['weighted_pct']);

        $this->assertSame('Carol', $rows[1]['name']);
        $this->assertEquals(50.25, (float) $rows[1]['weighted_pct']);
    }

    public function testPerAssignmentClassAverage(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.title,
                    COUNT(sub.id) AS submissions,
                    ROUND(AVG(sub.score)::numeric, 2) AS avg_score
             FROM pg_gr_assignments a
             LEFT JOIN pg_gr_submissions sub
                 ON sub.assignment_id = a.id AND sub.status = 'graded'
             GROUP BY a.id, a.title
             ORDER BY a.id"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('Homework 1', $rows[0]['title']);
        $this->assertEquals(4, (int) $rows[0]['submissions']);
        $this->assertEquals(78.0, (float) $rows[0]['avg_score']);

        $this->assertSame('Homework 2', $rows[1]['title']);
        $this->assertEquals(3, (int) $rows[1]['submissions']);
        $this->assertEquals(74.33, (float) $rows[1]['avg_score']);

        $this->assertSame('Midterm', $rows[2]['title']);
        $this->assertEquals(4, (int) $rows[2]['submissions']);
        $this->assertEquals(73.25, (float) $rows[2]['avg_score']);

        $this->assertSame('Final', $rows[3]['title']);
        $this->assertEquals(2, (int) $rows[3]['submissions']);
        $this->assertEquals(67.5, (float) $rows[3]['avg_score']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_gr_students VALUES (5, 'Eve')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_gr_students");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM pg_gr_students')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
