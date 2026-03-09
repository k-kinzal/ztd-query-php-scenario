<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests a class enrollment workflow through ZTD shadow store (MySQL PDO).
 * Covers capacity checks via COUNT, EXISTS for prerequisite validation,
 * waitlist promotion, course roster queries, and physical isolation.
 * @spec SPEC-10.2.82
 */
class MysqlClassEnrollmentTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_ce_courses (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                max_capacity INT,
                prerequisite_id INT NULL
            )',
            'CREATE TABLE mp_ce_enrollments (
                id INT PRIMARY KEY,
                course_id INT,
                student_name VARCHAR(255),
                status VARCHAR(20),
                enrolled_at DATETIME
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_ce_enrollments', 'mp_ce_courses'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_ce_courses VALUES (1, 'Intro to CS', 3, NULL)");
        $this->pdo->exec("INSERT INTO mp_ce_courses VALUES (2, 'Advanced CS', 2, 1)");
        $this->pdo->exec("INSERT INTO mp_ce_courses VALUES (3, 'Seminar', 5, NULL)");

        $this->pdo->exec("INSERT INTO mp_ce_enrollments VALUES (1, 1, 'Alice', 'enrolled', '2026-02-01 09:00:00')");
        $this->pdo->exec("INSERT INTO mp_ce_enrollments VALUES (2, 1, 'Bob', 'enrolled', '2026-02-02 10:00:00')");
        $this->pdo->exec("INSERT INTO mp_ce_enrollments VALUES (3, 1, 'Charlie', 'completed', '2026-02-03 11:00:00')");
        $this->pdo->exec("INSERT INTO mp_ce_enrollments VALUES (4, 2, 'Charlie', 'enrolled', '2026-03-01 09:00:00')");
    }

    /**
     * INSERT an enrollment, verify via JOIN with courses.
     */
    public function testEnrollStudent(): void
    {
        $this->pdo->exec("INSERT INTO mp_ce_enrollments VALUES (5, 3, 'Diana', 'enrolled', '2026-03-05 09:00:00')");

        $rows = $this->ztdQuery(
            "SELECT e.student_name, e.status, c.name AS course_name
             FROM mp_ce_enrollments e
             JOIN mp_ce_courses c ON c.id = e.course_id
             WHERE e.id = 5"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Diana', $rows[0]['student_name']);
        $this->assertSame('enrolled', $rows[0]['status']);
        $this->assertSame('Seminar', $rows[0]['course_name']);
    }

    /**
     * COUNT enrolled students vs max_capacity for a course.
     */
    public function testCapacityCheck(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name AS course_name,
                    c.max_capacity,
                    COUNT(e.id) AS enrolled_count,
                    c.max_capacity - COUNT(e.id) AS spots_remaining
             FROM mp_ce_courses c
             LEFT JOIN mp_ce_enrollments e ON e.course_id = c.id AND e.status = 'enrolled'
             GROUP BY c.id, c.name, c.max_capacity
             ORDER BY c.id"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(3, (int) $rows[0]['max_capacity']);
        $this->assertEquals(2, (int) $rows[0]['enrolled_count']);
        $this->assertEquals(1, (int) $rows[0]['spots_remaining']);
        $this->assertEquals(2, (int) $rows[1]['max_capacity']);
        $this->assertEquals(1, (int) $rows[1]['enrolled_count']);
        $this->assertEquals(1, (int) $rows[1]['spots_remaining']);
        $this->assertEquals(5, (int) $rows[2]['max_capacity']);
        $this->assertEquals(0, (int) $rows[2]['enrolled_count']);
        $this->assertEquals(5, (int) $rows[2]['spots_remaining']);
    }

    /**
     * EXISTS subquery: check if a student completed the prerequisite course.
     */
    public function testPrerequisiteValidation(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT EXISTS(
                SELECT 1 FROM mp_ce_enrollments
                WHERE course_id = (SELECT prerequisite_id FROM mp_ce_courses WHERE id = ?)
                  AND student_name = ?
                  AND status = 'completed'
             ) AS has_prereq",
            [2, 'Charlie']
        );
        $this->assertEquals(1, (int) $rows[0]['has_prereq']);

        $rows = $this->ztdPrepareAndExecute(
            "SELECT EXISTS(
                SELECT 1 FROM mp_ce_enrollments
                WHERE course_id = (SELECT prerequisite_id FROM mp_ce_courses WHERE id = ?)
                  AND student_name = ?
                  AND status = 'completed'
             ) AS has_prereq",
            [2, 'Alice']
        );
        $this->assertEquals(0, (int) $rows[0]['has_prereq']);
    }

    /**
     * When capacity is full, INSERT with status='waitlisted'.
     */
    public function testWaitlistWhenFull(): void
    {
        $this->pdo->exec("INSERT INTO mp_ce_enrollments VALUES (5, 1, 'Diana', 'enrolled', '2026-03-05 09:00:00')");

        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM mp_ce_enrollments WHERE course_id = 1 AND status = 'enrolled'"
        );
        $this->assertEquals(3, (int) $rows[0]['cnt']);

        $this->pdo->exec("INSERT INTO mp_ce_enrollments VALUES (6, 1, 'Eve', 'waitlisted', '2026-03-06 09:00:00')");

        $rows = $this->ztdQuery("SELECT student_name, status FROM mp_ce_enrollments WHERE id = 6");
        $this->assertSame('waitlisted', $rows[0]['status']);
    }

    /**
     * When a spot opens, promote the first waitlisted student to enrolled.
     */
    public function testPromoteFromWaitlist(): void
    {
        $this->pdo->exec("INSERT INTO mp_ce_enrollments VALUES (5, 1, 'Diana', 'enrolled', '2026-03-05 09:00:00')");
        $this->pdo->exec("INSERT INTO mp_ce_enrollments VALUES (6, 1, 'Eve', 'waitlisted', '2026-03-06 09:00:00')");
        $this->pdo->exec("INSERT INTO mp_ce_enrollments VALUES (7, 1, 'Frank', 'waitlisted', '2026-03-07 10:00:00')");

        $affected = $this->pdo->exec("UPDATE mp_ce_enrollments SET status = 'dropped' WHERE id = 5");
        $this->assertSame(1, $affected);

        $waitlisted = $this->ztdQuery(
            "SELECT id FROM mp_ce_enrollments
             WHERE course_id = 1 AND status = 'waitlisted'
             ORDER BY enrolled_at
             LIMIT 1"
        );
        $this->assertCount(1, $waitlisted);
        $promoteId = (int) $waitlisted[0]['id'];
        $this->assertEquals(6, $promoteId);

        $affected = $this->pdo->exec("UPDATE mp_ce_enrollments SET status = 'enrolled' WHERE id = {$promoteId}");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT status FROM mp_ce_enrollments WHERE id = 6");
        $this->assertSame('enrolled', $rows[0]['status']);

        $rows = $this->ztdQuery("SELECT status FROM mp_ce_enrollments WHERE id = 7");
        $this->assertSame('waitlisted', $rows[0]['status']);
    }

    /**
     * Course roster: JOIN courses and enrollments, filter enrolled, order by name.
     */
    public function testCourseRoster(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name AS course_name, e.student_name
             FROM mp_ce_enrollments e
             JOIN mp_ce_courses c ON c.id = e.course_id
             WHERE e.status = 'enrolled'
             ORDER BY c.name, e.student_name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Advanced CS', $rows[0]['course_name']);
        $this->assertSame('Charlie', $rows[0]['student_name']);
        $this->assertSame('Intro to CS', $rows[1]['course_name']);
        $this->assertSame('Alice', $rows[1]['student_name']);
        $this->assertSame('Intro to CS', $rows[2]['course_name']);
        $this->assertSame('Bob', $rows[2]['student_name']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_ce_enrollments VALUES (5, 3, 'Eve', 'enrolled', '2026-03-06 09:00:00')");
        $this->pdo->exec("UPDATE mp_ce_enrollments SET status = 'dropped' WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_ce_enrollments");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM mp_ce_enrollments')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
