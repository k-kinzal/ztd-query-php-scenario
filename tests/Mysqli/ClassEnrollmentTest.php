<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests a class enrollment workflow through ZTD shadow store (MySQLi).
 * Covers capacity checks via COUNT, EXISTS for prerequisite validation,
 * waitlist promotion, course roster queries, and physical isolation.
 * @spec SPEC-10.2.82
 */
class ClassEnrollmentTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_ce_courses (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                max_capacity INT,
                prerequisite_id INT NULL
            )',
            'CREATE TABLE mi_ce_enrollments (
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
        return ['mi_ce_enrollments', 'mi_ce_courses'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Courses: Intro (no prereq, capacity 3), Advanced (prereq=1, capacity 2), Seminar (no prereq, capacity 5)
        $this->mysqli->query("INSERT INTO mi_ce_courses VALUES (1, 'Intro to CS', 3, NULL)");
        $this->mysqli->query("INSERT INTO mi_ce_courses VALUES (2, 'Advanced CS', 2, 1)");
        $this->mysqli->query("INSERT INTO mi_ce_courses VALUES (3, 'Seminar', 5, NULL)");

        // Enrollments: Intro has 2 enrolled, 1 completed
        $this->mysqli->query("INSERT INTO mi_ce_enrollments VALUES (1, 1, 'Alice', 'enrolled', '2026-02-01 09:00:00')");
        $this->mysqli->query("INSERT INTO mi_ce_enrollments VALUES (2, 1, 'Bob', 'enrolled', '2026-02-02 10:00:00')");
        $this->mysqli->query("INSERT INTO mi_ce_enrollments VALUES (3, 1, 'Charlie', 'completed', '2026-02-03 11:00:00')");
        // Advanced has 1 enrolled
        $this->mysqli->query("INSERT INTO mi_ce_enrollments VALUES (4, 2, 'Charlie', 'enrolled', '2026-03-01 09:00:00')");
    }

    /**
     * INSERT an enrollment, verify via JOIN with courses.
     */
    public function testEnrollStudent(): void
    {
        $this->mysqli->query("INSERT INTO mi_ce_enrollments VALUES (5, 3, 'Diana', 'enrolled', '2026-03-05 09:00:00')");

        $rows = $this->ztdQuery(
            "SELECT e.student_name, e.status, c.name AS course_name
             FROM mi_ce_enrollments e
             JOIN mi_ce_courses c ON c.id = e.course_id
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
             FROM mi_ce_courses c
             LEFT JOIN mi_ce_enrollments e ON e.course_id = c.id AND e.status = 'enrolled'
             GROUP BY c.id, c.name, c.max_capacity
             ORDER BY c.id"
        );

        $this->assertCount(3, $rows);
        // Intro: 2 enrolled, capacity 3
        $this->assertEquals(3, (int) $rows[0]['max_capacity']);
        $this->assertEquals(2, (int) $rows[0]['enrolled_count']);
        $this->assertEquals(1, (int) $rows[0]['spots_remaining']);
        // Advanced: 1 enrolled, capacity 2
        $this->assertEquals(2, (int) $rows[1]['max_capacity']);
        $this->assertEquals(1, (int) $rows[1]['enrolled_count']);
        $this->assertEquals(1, (int) $rows[1]['spots_remaining']);
        // Seminar: 0 enrolled, capacity 5
        $this->assertEquals(5, (int) $rows[2]['max_capacity']);
        $this->assertEquals(0, (int) $rows[2]['enrolled_count']);
        $this->assertEquals(5, (int) $rows[2]['spots_remaining']);
    }

    /**
     * EXISTS subquery: check if a student completed the prerequisite course.
     */
    public function testPrerequisiteValidation(): void
    {
        // Charlie completed course 1 (Intro), which is the prerequisite for course 2 (Advanced)
        $rows = $this->ztdPrepareAndExecute(
            "SELECT EXISTS(
                SELECT 1 FROM mi_ce_enrollments
                WHERE course_id = (SELECT prerequisite_id FROM mi_ce_courses WHERE id = ?)
                  AND student_name = ?
                  AND status = 'completed'
             ) AS has_prereq",
            [2, 'Charlie']
        );
        $this->assertEquals(1, (int) $rows[0]['has_prereq']);

        // Alice has NOT completed the prerequisite
        $rows = $this->ztdPrepareAndExecute(
            "SELECT EXISTS(
                SELECT 1 FROM mi_ce_enrollments
                WHERE course_id = (SELECT prerequisite_id FROM mi_ce_courses WHERE id = ?)
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
        // Fill Intro to capacity (add 1 more enrolled to reach 3)
        $this->mysqli->query("INSERT INTO mi_ce_enrollments VALUES (5, 1, 'Diana', 'enrolled', '2026-03-05 09:00:00')");

        // Verify at capacity
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM mi_ce_enrollments WHERE course_id = 1 AND status = 'enrolled'"
        );
        $this->assertEquals(3, (int) $rows[0]['cnt']);

        // Add waitlisted student
        $this->mysqli->query("INSERT INTO mi_ce_enrollments VALUES (6, 1, 'Eve', 'waitlisted', '2026-03-06 09:00:00')");

        $rows = $this->ztdQuery(
            "SELECT student_name, status FROM mi_ce_enrollments WHERE id = 6"
        );
        $this->assertSame('waitlisted', $rows[0]['status']);
    }

    /**
     * When a spot opens, promote the first waitlisted student to enrolled.
     */
    public function testPromoteFromWaitlist(): void
    {
        // Fill course and add two waitlisted students
        $this->mysqli->query("INSERT INTO mi_ce_enrollments VALUES (5, 1, 'Diana', 'enrolled', '2026-03-05 09:00:00')");
        $this->mysqli->query("INSERT INTO mi_ce_enrollments VALUES (6, 1, 'Eve', 'waitlisted', '2026-03-06 09:00:00')");
        $this->mysqli->query("INSERT INTO mi_ce_enrollments VALUES (7, 1, 'Frank', 'waitlisted', '2026-03-07 10:00:00')");

        // Drop Diana to open a spot
        $this->mysqli->query("UPDATE mi_ce_enrollments SET status = 'dropped' WHERE id = 5");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        // Promote first waitlisted (Eve, earliest enrolled_at)
        // First find the earliest waitlisted
        $waitlisted = $this->ztdQuery(
            "SELECT id FROM mi_ce_enrollments
             WHERE course_id = 1 AND status = 'waitlisted'
             ORDER BY enrolled_at
             LIMIT 1"
        );
        $this->assertCount(1, $waitlisted);
        $promoteId = (int) $waitlisted[0]['id'];
        $this->assertEquals(6, $promoteId);

        $this->mysqli->query("UPDATE mi_ce_enrollments SET status = 'enrolled' WHERE id = {$promoteId}");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        // Verify Eve is now enrolled
        $rows = $this->ztdQuery("SELECT status FROM mi_ce_enrollments WHERE id = 6");
        $this->assertSame('enrolled', $rows[0]['status']);

        // Frank is still waitlisted
        $rows = $this->ztdQuery("SELECT status FROM mi_ce_enrollments WHERE id = 7");
        $this->assertSame('waitlisted', $rows[0]['status']);
    }

    /**
     * Course roster: JOIN courses and enrollments, filter enrolled, order by name.
     */
    public function testCourseRoster(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name AS course_name, e.student_name
             FROM mi_ce_enrollments e
             JOIN mi_ce_courses c ON c.id = e.course_id
             WHERE e.status = 'enrolled'
             ORDER BY c.name, e.student_name"
        );

        $this->assertCount(3, $rows);
        // Advanced CS: Charlie
        $this->assertSame('Advanced CS', $rows[0]['course_name']);
        $this->assertSame('Charlie', $rows[0]['student_name']);
        // Intro to CS: Alice, Bob
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
        $this->mysqli->query("INSERT INTO mi_ce_enrollments VALUES (5, 3, 'Eve', 'enrolled', '2026-03-06 09:00:00')");
        $this->mysqli->query("UPDATE mi_ce_enrollments SET status = 'dropped' WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_ce_enrollments");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ce_enrollments');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
