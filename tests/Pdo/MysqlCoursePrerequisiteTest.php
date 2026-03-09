<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests academic course enrollment with prerequisite validation.
 * SQL patterns exercised: NOT EXISTS for prerequisite checking, COUNT DISTINCT completion tracking,
 * multi-table JOIN, prepared BETWEEN date filter, LEFT JOIN missing prerequisites (MySQL PDO).
 * @spec SPEC-10.2.145
 */
class MysqlCoursePrerequisiteTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_cp_courses (
                id INT AUTO_INCREMENT PRIMARY KEY,
                code VARCHAR(20),
                title VARCHAR(100),
                credits INT
            )',
            'CREATE TABLE mp_cp_prerequisites (
                id INT AUTO_INCREMENT PRIMARY KEY,
                course_id INT,
                required_course_id INT
            )',
            'CREATE TABLE mp_cp_students (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                enrollment_date TEXT
            )',
            'CREATE TABLE mp_cp_completions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                student_id INT,
                course_id INT,
                grade VARCHAR(2),
                completed_date TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_cp_completions', 'mp_cp_prerequisites', 'mp_cp_students', 'mp_cp_courses'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_cp_courses VALUES (1, 'CS101', 'Intro to CS', 3)");
        $this->pdo->exec("INSERT INTO mp_cp_courses VALUES (2, 'CS201', 'Data Structures', 4)");
        $this->pdo->exec("INSERT INTO mp_cp_courses VALUES (3, 'CS301', 'Algorithms', 4)");
        $this->pdo->exec("INSERT INTO mp_cp_courses VALUES (4, 'CS401', 'Machine Learning', 3)");
        $this->pdo->exec("INSERT INTO mp_cp_courses VALUES (5, 'MATH101', 'Calculus I', 4)");
        $this->pdo->exec("INSERT INTO mp_cp_courses VALUES (6, 'MATH201', 'Linear Algebra', 3)");

        $this->pdo->exec("INSERT INTO mp_cp_prerequisites VALUES (1, 2, 1)");
        $this->pdo->exec("INSERT INTO mp_cp_prerequisites VALUES (2, 3, 2)");
        $this->pdo->exec("INSERT INTO mp_cp_prerequisites VALUES (3, 3, 5)");
        $this->pdo->exec("INSERT INTO mp_cp_prerequisites VALUES (4, 4, 3)");
        $this->pdo->exec("INSERT INTO mp_cp_prerequisites VALUES (5, 4, 6)");

        $this->pdo->exec("INSERT INTO mp_cp_students VALUES (1, 'Alice', '2024-09-01')");
        $this->pdo->exec("INSERT INTO mp_cp_students VALUES (2, 'Bob', '2024-09-01')");
        $this->pdo->exec("INSERT INTO mp_cp_students VALUES (3, 'Carol', '2025-01-15')");

        $this->pdo->exec("INSERT INTO mp_cp_completions VALUES (1, 1, 1, 'A', '2025-01-15')");
        $this->pdo->exec("INSERT INTO mp_cp_completions VALUES (2, 1, 2, 'B+', '2025-05-20')");
        $this->pdo->exec("INSERT INTO mp_cp_completions VALUES (3, 1, 5, 'A-', '2025-05-20')");
        $this->pdo->exec("INSERT INTO mp_cp_completions VALUES (4, 2, 1, 'B', '2025-01-15')");
        $this->pdo->exec("INSERT INTO mp_cp_completions VALUES (5, 2, 5, 'A', '2025-05-20')");
        $this->pdo->exec("INSERT INTO mp_cp_completions VALUES (6, 2, 6, 'B+', '2025-05-20')");
        $this->pdo->exec("INSERT INTO mp_cp_completions VALUES (7, 3, 1, 'A', '2025-05-20')");
    }

    public function testStudentTranscript(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.name, c.code, c.title, comp.grade, comp.completed_date
             FROM mp_cp_completions comp
             JOIN mp_cp_students s ON s.id = comp.student_id
             JOIN mp_cp_courses c ON c.id = comp.course_id
             WHERE s.name = 'Alice'
             ORDER BY comp.completed_date, c.code"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('CS101', $rows[0]['code']);
        $this->assertSame('A', $rows[0]['grade']);
        $this->assertSame('CS201', $rows[1]['code']);
        $this->assertSame('MATH101', $rows[2]['code']);
    }

    public function testCreditsEarnedPerStudent(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.name, SUM(c.credits) AS total_credits, COUNT(*) AS courses_completed
             FROM mp_cp_completions comp
             JOIN mp_cp_students s ON s.id = comp.student_id
             JOIN mp_cp_courses c ON c.id = comp.course_id
             GROUP BY s.name
             ORDER BY s.name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(11, (int) $rows[0]['total_credits']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(10, (int) $rows[1]['total_credits']);
        $this->assertSame('Carol', $rows[2]['name']);
        $this->assertEquals(3, (int) $rows[2]['total_credits']);
    }

    public function testEligibilityViaNotExists(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.name
             FROM mp_cp_students s
             WHERE NOT EXISTS (
                 SELECT 1 FROM mp_cp_prerequisites p
                 WHERE p.course_id = 3
                   AND NOT EXISTS (
                       SELECT 1 FROM mp_cp_completions comp
                       WHERE comp.student_id = s.id
                         AND comp.course_id = p.required_course_id
                   )
             )
             ORDER BY s.name"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testMissingPrerequisites(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.name, c.code AS missing_course
             FROM mp_cp_students s
             JOIN mp_cp_prerequisites p ON p.course_id = 4
             JOIN mp_cp_courses c ON c.id = p.required_course_id
             LEFT JOIN mp_cp_completions comp ON comp.student_id = s.id AND comp.course_id = p.required_course_id
             WHERE comp.id IS NULL
             ORDER BY s.name, c.code"
        );

        $this->assertCount(5, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('CS301', $rows[0]['missing_course']);
        $this->assertSame('Alice', $rows[1]['name']);
        $this->assertSame('MATH201', $rows[1]['missing_course']);
        $this->assertSame('Bob', $rows[2]['name']);
        $this->assertSame('CS301', $rows[2]['missing_course']);
    }

    public function testPrerequisiteCompletionCount(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.name,
                    COUNT(DISTINCT p.required_course_id) AS required_count,
                    COUNT(DISTINCT comp.course_id) AS completed_count
             FROM mp_cp_students s
             JOIN mp_cp_prerequisites p ON p.course_id = 3
             LEFT JOIN mp_cp_completions comp ON comp.student_id = s.id AND comp.course_id = p.required_course_id
             GROUP BY s.name
             ORDER BY s.name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['required_count']);
        $this->assertEquals(2, (int) $rows[0]['completed_count']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(2, (int) $rows[1]['required_count']);
        $this->assertEquals(1, (int) $rows[1]['completed_count']);
        $this->assertSame('Carol', $rows[2]['name']);
        $this->assertEquals(2, (int) $rows[2]['required_count']);
        $this->assertEquals(0, (int) $rows[2]['completed_count']);
    }

    public function testPreparedCompletionsByDateRange(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT s.name, c.code, comp.grade
             FROM mp_cp_completions comp
             JOIN mp_cp_students s ON s.id = comp.student_id
             JOIN mp_cp_courses c ON c.id = comp.course_id
             WHERE comp.completed_date BETWEEN ? AND ?
             ORDER BY s.name, c.code",
            ['2025-05-01', '2025-05-31']
        );

        $this->assertCount(5, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('CS201', $rows[0]['code']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_cp_completions VALUES (8, 2, 2, 'A', '2025-09-15')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cp_completions");
        $this->assertEquals(8, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_cp_completions")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
