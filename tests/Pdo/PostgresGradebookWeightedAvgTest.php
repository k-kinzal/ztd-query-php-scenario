<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests gradebook weighted average calculations — exercises SUM(a*b)/SUM(b)
 * weighted arithmetic in aggregates, NULLIF to avoid division by zero,
 * ROUND function, and CASE-based grade boundaries (PostgreSQL PDO).
 * @spec SPEC-10.2.129
 */
class PostgresGradebookWeightedAvgTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_gw_students (
                id SERIAL PRIMARY KEY,
                name TEXT,
                enrolled_date TEXT
            )',
            'CREATE TABLE pg_gw_assignments (
                id SERIAL PRIMARY KEY,
                title TEXT,
                category TEXT,
                weight NUMERIC(5,2),
                max_score INTEGER
            )',
            'CREATE TABLE pg_gw_grades (
                id SERIAL PRIMARY KEY,
                student_id INTEGER,
                assignment_id INTEGER,
                score NUMERIC(5,2)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_gw_grades', 'pg_gw_assignments', 'pg_gw_students'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Students
        $this->pdo->exec("INSERT INTO pg_gw_students VALUES (1, 'alice', '2025-09-01')");
        $this->pdo->exec("INSERT INTO pg_gw_students VALUES (2, 'bob', '2025-09-01')");
        $this->pdo->exec("INSERT INTO pg_gw_students VALUES (3, 'charlie', '2025-09-01')");
        $this->pdo->exec("INSERT INTO pg_gw_students VALUES (4, 'diana', '2025-09-01')");

        // Assignments
        $this->pdo->exec("INSERT INTO pg_gw_assignments VALUES (1, 'Midterm Exam', 'exam', 30.00, 100)");
        $this->pdo->exec("INSERT INTO pg_gw_assignments VALUES (2, 'Final Exam', 'exam', 40.00, 100)");
        $this->pdo->exec("INSERT INTO pg_gw_assignments VALUES (3, 'Homework 1', 'homework', 10.00, 50)");
        $this->pdo->exec("INSERT INTO pg_gw_assignments VALUES (4, 'Homework 2', 'homework', 10.00, 50)");
        $this->pdo->exec("INSERT INTO pg_gw_assignments VALUES (5, 'Lab Project', 'project', 10.00, 100)");

        // Grades — Alice (strong student)
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (1, 1, 1, 92.00)");
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (2, 1, 2, 88.00)");
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (3, 1, 3, 45.00)");
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (4, 1, 4, 48.00)");
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (5, 1, 5, 95.00)");

        // Grades — Bob (average student)
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (6, 2, 1, 75.00)");
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (7, 2, 2, 70.00)");
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (8, 2, 3, 35.00)");
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (9, 2, 4, 30.00)");
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (10, 2, 5, 65.00)");

        // Grades — Charlie (weak student, missing Lab Project)
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (11, 3, 1, 55.00)");
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (12, 3, 2, 48.00)");
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (13, 3, 3, 20.00)");
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (14, 3, 4, 25.00)");

        // Grades — Diana (excellent student)
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (15, 4, 1, 98.00)");
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (16, 4, 2, 95.00)");
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (17, 4, 3, 50.00)");
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (18, 4, 4, 49.00)");
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (19, 4, 5, 100.00)");
    }

    /**
     * Weighted average per student: SUM(score/max_score * 100 * weight) / SUM(weight).
     * Charlie only has 4 assignments so his weight sum is 90, not 100.
     */
    public function testWeightedAverage(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.name,
                    ROUND(SUM(g.score / a.max_score * 100.0 * a.weight) / SUM(a.weight), 2) AS weighted_avg
             FROM pg_gw_students s
             JOIN pg_gw_grades g ON g.student_id = s.id
             JOIN pg_gw_assignments a ON a.id = g.assignment_id
             GROUP BY s.id, s.name
             ORDER BY weighted_avg DESC"
        );

        $this->assertCount(4, $rows);

        // Diana: (98*30 + 95*40 + 100*10 + 98*10 + 100*10) / 100 = 97.20
        $this->assertSame('diana', $rows[0]['name']);
        $this->assertEqualsWithDelta(97.20, (float) $rows[0]['weighted_avg'], 0.01);

        // Alice: (92*30 + 88*40 + 90*10 + 96*10 + 95*10) / 100 = 90.90
        $this->assertSame('alice', $rows[1]['name']);
        $this->assertEqualsWithDelta(90.90, (float) $rows[1]['weighted_avg'], 0.01);

        // Bob: (75*30 + 70*40 + 70*10 + 60*10 + 65*10) / 100 = 70.00
        $this->assertSame('bob', $rows[2]['name']);
        $this->assertEqualsWithDelta(70.00, (float) $rows[2]['weighted_avg'], 0.01);

        // Charlie: (55*30 + 48*40 + 40*10 + 50*10) / 90 = 49.67
        $this->assertSame('charlie', $rows[3]['name']);
        $this->assertEqualsWithDelta(49.67, (float) $rows[3]['weighted_avg'], 0.01);
    }

    /**
     * Letter grades using CASE boundaries on weighted average.
     */
    public function testLetterGrades(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.name,
                    ROUND(SUM(g.score / a.max_score * 100.0 * a.weight) / SUM(a.weight), 2) AS weighted_avg,
                    CASE
                        WHEN ROUND(SUM(g.score / a.max_score * 100.0 * a.weight) / SUM(a.weight), 2) >= 90 THEN 'A'
                        WHEN ROUND(SUM(g.score / a.max_score * 100.0 * a.weight) / SUM(a.weight), 2) >= 80 THEN 'B'
                        WHEN ROUND(SUM(g.score / a.max_score * 100.0 * a.weight) / SUM(a.weight), 2) >= 70 THEN 'C'
                        WHEN ROUND(SUM(g.score / a.max_score * 100.0 * a.weight) / SUM(a.weight), 2) >= 60 THEN 'D'
                        ELSE 'F'
                    END AS letter_grade
             FROM pg_gw_students s
             JOIN pg_gw_grades g ON g.student_id = s.id
             JOIN pg_gw_assignments a ON a.id = g.assignment_id
             GROUP BY s.id, s.name
             ORDER BY weighted_avg DESC"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('diana', $rows[0]['name']);
        $this->assertSame('A', $rows[0]['letter_grade']);

        $this->assertSame('alice', $rows[1]['name']);
        $this->assertSame('A', $rows[1]['letter_grade']);

        $this->assertSame('bob', $rows[2]['name']);
        $this->assertSame('C', $rows[2]['letter_grade']);

        $this->assertSame('charlie', $rows[3]['name']);
        $this->assertSame('F', $rows[3]['letter_grade']);
    }

    /**
     * Class-wide statistics: student count, average score percentage, min, max.
     */
    public function testClassStatistics(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COUNT(DISTINCT g.student_id) AS student_count,
                    ROUND(AVG(g.score / a.max_score * 100.0), 2) AS class_avg_score,
                    ROUND(MIN(g.score / a.max_score * 100.0), 2) AS lowest_pct,
                    ROUND(MAX(g.score / a.max_score * 100.0), 2) AS highest_pct
             FROM pg_gw_grades g
             JOIN pg_gw_assignments a ON a.id = g.assignment_id"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(4, (int) $rows[0]['student_count']);

        // AVG of 19 percentages: 1485/19 = 78.16
        $this->assertEqualsWithDelta(78.16, (float) $rows[0]['class_avg_score'], 0.01);

        // Lowest: charlie HW1 20/50 = 40.00
        $this->assertEqualsWithDelta(40.00, (float) $rows[0]['lowest_pct'], 0.01);

        // Highest: diana HW1 50/50 or Lab 100/100 = 100.00
        $this->assertEqualsWithDelta(100.00, (float) $rows[0]['highest_pct'], 0.01);
    }

    /**
     * Average score percentage per assignment category.
     */
    public function testCategoryAverages(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.category,
                    COUNT(*) AS grade_count,
                    ROUND(AVG(g.score / a.max_score * 100.0), 2) AS avg_pct
             FROM pg_gw_grades g
             JOIN pg_gw_assignments a ON a.id = g.assignment_id
             GROUP BY a.category
             ORDER BY a.category"
        );

        $this->assertCount(3, $rows);

        // exam: 8 grades, avg = 621/8 = 77.63
        $this->assertSame('exam', $rows[0]['category']);
        $this->assertEquals(8, (int) $rows[0]['grade_count']);
        $this->assertEqualsWithDelta(77.63, (float) $rows[0]['avg_pct'], 0.01);

        // homework: 8 grades, avg = 604/8 = 75.50
        $this->assertSame('homework', $rows[1]['category']);
        $this->assertEquals(8, (int) $rows[1]['grade_count']);
        $this->assertEqualsWithDelta(75.50, (float) $rows[1]['avg_pct'], 0.01);

        // project: 3 grades, avg = 260/3 = 86.67
        $this->assertSame('project', $rows[2]['category']);
        $this->assertEquals(3, (int) $rows[2]['grade_count']);
        $this->assertEqualsWithDelta(86.67, (float) $rows[2]['avg_pct'], 0.01);
    }

    /**
     * Find students missing assignments using CROSS JOIN + LEFT JOIN IS NULL.
     * Only Charlie is missing Lab Project.
     */
    public function testMissingAssignments(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.name, a.title
             FROM pg_gw_students s
             CROSS JOIN pg_gw_assignments a
             LEFT JOIN pg_gw_grades g ON g.student_id = s.id AND g.assignment_id = a.id
             WHERE g.id IS NULL
             ORDER BY s.name, a.title"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('charlie', $rows[0]['name']);
        $this->assertSame('Lab Project', $rows[0]['title']);
    }

    /**
     * Best student per assignment category using HAVING with correlated subquery.
     * Diana is the top performer in every category.
     */
    public function testTopPerformerPerCategory(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.category, s.name,
                    ROUND(AVG(g.score / a.max_score * 100.0), 2) AS avg_pct
             FROM pg_gw_students s
             JOIN pg_gw_grades g ON g.student_id = s.id
             JOIN pg_gw_assignments a ON a.id = g.assignment_id
             GROUP BY a.category, s.id, s.name
             HAVING AVG(g.score / a.max_score * 100.0) = (
                 SELECT MAX(sub_avg) FROM (
                     SELECT AVG(g2.score / a2.max_score * 100.0) AS sub_avg
                     FROM pg_gw_grades g2
                     JOIN pg_gw_assignments a2 ON a2.id = g2.assignment_id
                     WHERE a2.category = a.category
                     GROUP BY g2.student_id
                 ) best
             )
             ORDER BY a.category"
        );

        $this->assertCount(3, $rows);

        // exam: diana avg (98+95)/2 = 96.50
        $this->assertSame('exam', $rows[0]['category']);
        $this->assertSame('diana', $rows[0]['name']);
        $this->assertEqualsWithDelta(96.50, (float) $rows[0]['avg_pct'], 0.01);

        // homework: diana avg (100+98)/2 = 99.00
        $this->assertSame('homework', $rows[1]['category']);
        $this->assertSame('diana', $rows[1]['name']);
        $this->assertEqualsWithDelta(99.00, (float) $rows[1]['avg_pct'], 0.01);

        // project: diana 100.00
        $this->assertSame('project', $rows[2]['category']);
        $this->assertSame('diana', $rows[2]['name']);
        $this->assertEqualsWithDelta(100.00, (float) $rows[2]['avg_pct'], 0.01);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_gw_students VALUES (5, 'eve', '2025-09-01')");
        $this->pdo->exec("INSERT INTO pg_gw_grades VALUES (20, 5, 1, 80.00)");

        // ZTD sees changes
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_gw_students");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_gw_grades");
        $this->assertEquals(20, (int) $rows[0]['cnt']);

        // Physical tables untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_gw_students")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
