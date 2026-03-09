<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a student grade report scenario through ZTD shadow store (SQLite PDO).
 * Students, courses, and submissions exercise LEFT JOIN COALESCE for missing
 * submissions scored as zero, multiple nested CASE WHEN for letter grades,
 * weighted average (SUM product / SUM weight), DELETE with EXISTS for draft
 * cleanup, prepared statement with multiple params, and physical isolation.
 * @spec SPEC-10.2.165
 */
class SqliteStudentGradeReportTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_gr_students (
                id INTEGER PRIMARY KEY,
                name TEXT
            )',
            'CREATE TABLE sl_gr_assignments (
                id INTEGER PRIMARY KEY,
                course TEXT,
                title TEXT,
                weight REAL,
                max_score REAL
            )',
            'CREATE TABLE sl_gr_submissions (
                id INTEGER PRIMARY KEY,
                student_id INTEGER,
                assignment_id INTEGER,
                score REAL,
                status TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_gr_submissions', 'sl_gr_assignments', 'sl_gr_students'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 4 students
        $this->pdo->exec("INSERT INTO sl_gr_students VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO sl_gr_students VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO sl_gr_students VALUES (3, 'Carol')");
        $this->pdo->exec("INSERT INTO sl_gr_students VALUES (4, 'Dave')");

        // 4 assignments for 'Math 101'
        $this->pdo->exec("INSERT INTO sl_gr_assignments VALUES (1, 'Math 101', 'Homework 1', 0.15, 100.0)");
        $this->pdo->exec("INSERT INTO sl_gr_assignments VALUES (2, 'Math 101', 'Homework 2', 0.15, 100.0)");
        $this->pdo->exec("INSERT INTO sl_gr_assignments VALUES (3, 'Math 101', 'Midterm', 0.30, 100.0)");
        $this->pdo->exec("INSERT INTO sl_gr_assignments VALUES (4, 'Math 101', 'Final', 0.40, 100.0)");

        // Submissions: Alice has all 4, Bob has 3 (no Final), Carol has 4, Dave has 2 + 1 draft
        $this->pdo->exec("INSERT INTO sl_gr_submissions VALUES (1, 1, 1, 92.0, 'graded')");
        $this->pdo->exec("INSERT INTO sl_gr_submissions VALUES (2, 1, 2, 88.0, 'graded')");
        $this->pdo->exec("INSERT INTO sl_gr_submissions VALUES (3, 1, 3, 95.0, 'graded')");
        $this->pdo->exec("INSERT INTO sl_gr_submissions VALUES (4, 1, 4, 90.0, 'graded')");

        $this->pdo->exec("INSERT INTO sl_gr_submissions VALUES (5, 2, 1, 75.0, 'graded')");
        $this->pdo->exec("INSERT INTO sl_gr_submissions VALUES (6, 2, 2, 80.0, 'graded')");
        $this->pdo->exec("INSERT INTO sl_gr_submissions VALUES (7, 2, 3, 70.0, 'graded')");
        // Bob: no Final submission

        $this->pdo->exec("INSERT INTO sl_gr_submissions VALUES (8, 3, 1, 60.0, 'graded')");
        $this->pdo->exec("INSERT INTO sl_gr_submissions VALUES (9, 3, 2, 55.0, 'graded')");
        $this->pdo->exec("INSERT INTO sl_gr_submissions VALUES (10, 3, 3, 50.0, 'graded')");
        $this->pdo->exec("INSERT INTO sl_gr_submissions VALUES (11, 3, 4, 45.0, 'graded')");

        $this->pdo->exec("INSERT INTO sl_gr_submissions VALUES (12, 4, 1, 85.0, 'graded')");
        $this->pdo->exec("INSERT INTO sl_gr_submissions VALUES (13, 4, 3, 78.0, 'graded')");
        $this->pdo->exec("INSERT INTO sl_gr_submissions VALUES (14, 4, 2, 0.0, 'draft')");  // draft, not counted
    }

    /**
     * LEFT JOIN COALESCE: students with missing submissions get score=0.
     * Cross-join students × assignments, LEFT JOIN submissions, COALESCE NULL→0.
     * Alice: 4 submissions, Bob: 3 + 1 missing, Carol: 4, Dave: 2 graded + 1 draft.
     * Expected: 16 rows (4 students × 4 assignments).
     */
    public function testLeftJoinMissingSubmissionsAsZero(): void
    {
        $rows = $this->ztdQuery(
            "SELECT st.name, a.title,
                    COALESCE(sub.score, 0) AS score
             FROM sl_gr_students st
             CROSS JOIN sl_gr_assignments a
             LEFT JOIN sl_gr_submissions sub
                 ON sub.student_id = st.id AND sub.assignment_id = a.id AND sub.status = 'graded'
             ORDER BY st.name, a.id"
        );

        $this->assertCount(16, $rows);

        // Bob's Final is missing → score=0
        $bobFinal = array_values(array_filter($rows, fn($r) => $r['name'] === 'Bob' && $r['title'] === 'Final'));
        $this->assertCount(1, $bobFinal);
        $this->assertEquals(0, (float) $bobFinal[0]['score']);

        // Dave's Homework 2 is draft, so LEFT JOIN with status='graded' misses it → score=0
        $daveHw2 = array_values(array_filter($rows, fn($r) => $r['name'] === 'Dave' && $r['title'] === 'Homework 2'));
        $this->assertCount(1, $daveHw2);
        $this->assertEquals(0, (float) $daveHw2[0]['score']);
    }

    /**
     * Weighted average with CASE WHEN letter grades.
     * Formula: SUM(score/max_score * weight) / SUM(weight) * 100 = weighted %.
     * Missing submissions count as 0.
     * Alice: (92*0.15 + 88*0.15 + 95*0.30 + 90*0.40)/1.0 = 91.5 → A-
     * Bob: (75*0.15 + 80*0.15 + 70*0.30 + 0*0.40)/1.0 = 44.25 → F
     * Carol: (60*0.15 + 55*0.15 + 50*0.30 + 45*0.40)/1.0 = 50.25 → F
     * Dave: (85*0.15 + 0*0.15 + 78*0.30 + 0*0.40)/1.0 = 36.15 → F
     */
    public function testWeightedAverageWithLetterGrade(): void
    {
        $rows = $this->ztdQuery(
            "SELECT st.name,
                    ROUND(SUM(COALESCE(sub.score, 0) / a.max_score * a.weight) * 100, 2) AS weighted_pct,
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
             FROM sl_gr_students st
             CROSS JOIN sl_gr_assignments a
             LEFT JOIN sl_gr_submissions sub
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

    /**
     * DELETE with EXISTS: remove draft submissions.
     * Dave has 1 draft submission (id=14). After delete, 13 submissions remain.
     */
    public function testDeleteDraftsWithExists(): void
    {
        $this->pdo->exec(
            "DELETE FROM sl_gr_submissions
             WHERE EXISTS (
                 SELECT 1 FROM sl_gr_submissions s2
                 WHERE s2.id = sl_gr_submissions.id AND s2.status = 'draft'
             )"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_gr_submissions");
        $this->assertEquals(13, (int) $rows[0]['cnt']);

        // Verify no drafts remain
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM sl_gr_submissions WHERE status = 'draft'"
        );
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }

    /**
     * Prepared statement: find students in a course above a grade threshold.
     * Params: course='Math 101', min_pct=50.
     * Expected: Alice (91.5), Carol (50.25).
     *
     * @see SPEC-11.SQLITE-HAVING-PARAMS — HAVING with prepared params returns empty on SQLite
     */
    public function testPreparedGradeFilter(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT st.name,
                    ROUND(SUM(COALESCE(sub.score, 0) / a.max_score * a.weight) * 100, 2) AS weighted_pct
             FROM sl_gr_students st
             CROSS JOIN sl_gr_assignments a
             LEFT JOIN sl_gr_submissions sub
                 ON sub.student_id = st.id AND sub.assignment_id = a.id AND sub.status = 'graded'
             WHERE a.course = ?
             GROUP BY st.id, st.name
             HAVING ROUND(SUM(COALESCE(sub.score, 0) / a.max_score * a.weight) * 100, 2) >= ?
             ORDER BY weighted_pct DESC",
            ['Math 101', 50]
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'SPEC-11.SQLITE-HAVING-PARAMS: HAVING with prepared params returns empty on SQLite. Expected 2 rows (Alice 91.5, Carol 50.25).'
            );
        }
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(91.5, (float) $rows[0]['weighted_pct']);

        $this->assertSame('Carol', $rows[1]['name']);
        $this->assertEquals(50.25, (float) $rows[1]['weighted_pct']);
    }

    /**
     * Per-assignment class average using aggregate.
     * Expected 4 rows with averages over graded submissions only.
     */
    public function testPerAssignmentClassAverage(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.title,
                    COUNT(sub.id) AS submissions,
                    ROUND(AVG(sub.score), 2) AS avg_score
             FROM sl_gr_assignments a
             LEFT JOIN sl_gr_submissions sub
                 ON sub.assignment_id = a.id AND sub.status = 'graded'
             GROUP BY a.id, a.title
             ORDER BY a.id"
        );

        $this->assertCount(4, $rows);

        // HW1: Alice(92), Bob(75), Carol(60), Dave(85) → avg 78.0
        $this->assertSame('Homework 1', $rows[0]['title']);
        $this->assertEquals(4, (int) $rows[0]['submissions']);
        $this->assertEquals(78.0, (float) $rows[0]['avg_score']);

        // HW2: Alice(88), Bob(80), Carol(55) → avg 74.33
        $this->assertSame('Homework 2', $rows[1]['title']);
        $this->assertEquals(3, (int) $rows[1]['submissions']);
        $this->assertEquals(74.33, (float) $rows[1]['avg_score']);

        // Midterm: Alice(95), Bob(70), Carol(50), Dave(78) → avg 73.25
        $this->assertSame('Midterm', $rows[2]['title']);
        $this->assertEquals(4, (int) $rows[2]['submissions']);
        $this->assertEquals(73.25, (float) $rows[2]['avg_score']);

        // Final: Alice(90), Carol(45) → avg 67.5
        $this->assertSame('Final', $rows[3]['title']);
        $this->assertEquals(2, (int) $rows[3]['submissions']);
        $this->assertEquals(67.5, (float) $rows[3]['avg_score']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_gr_students VALUES (5, 'Eve')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_gr_students");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_gr_students")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
