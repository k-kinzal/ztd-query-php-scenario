<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests recursive CTEs and RIGHT JOIN on MySQLi.
 * @spec SPEC-3.3c
 */
class RecursiveCteAndRightJoinTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_rc_categories (id INT PRIMARY KEY, name VARCHAR(255), parent_id INT)',
            'CREATE TABLE mi_rc_students (id INT PRIMARY KEY, name VARCHAR(255))',
            'CREATE TABLE mi_rc_courses (id INT PRIMARY KEY, title VARCHAR(255))',
            'CREATE TABLE mi_rc_enrollments (student_id INT, course_id INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_rc_enrollments', 'mi_rc_categories', 'mi_rc_students', 'mi_rc_courses'];
    }


    public function testRecursiveCteNumberSeries(): void
    {
        $result = $this->mysqli->query("
            WITH RECURSIVE nums(n) AS (
                SELECT 1
                UNION ALL
                SELECT n + 1 FROM nums WHERE n < 5
            )
            SELECT n FROM nums ORDER BY n
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertSame(1, (int) $rows[0]['n']);
    }

    /**
     * WITH RECURSIVE + shadow table fails on MySQL.
     * CTE rewriter prepends its own WITH before RECURSIVE, producing invalid SQL.
     */
    public function testRecursiveCteWithShadowTableFails(): void
    {
        $this->mysqli->query("INSERT INTO mi_rc_categories (id, name, parent_id) VALUES (1, 'Root', NULL)");
        $this->mysqli->query("INSERT INTO mi_rc_categories (id, name, parent_id) VALUES (2, 'Electronics', 1)");

        $this->expectException(\Throwable::class);
        $this->mysqli->query("
            WITH RECURSIVE cat_tree AS (
                SELECT id, name, parent_id, 0 AS depth
                FROM mi_rc_categories
                WHERE parent_id IS NULL
                UNION ALL
                SELECT c.id, c.name, c.parent_id, ct.depth + 1
                FROM mi_rc_categories c
                JOIN cat_tree ct ON c.parent_id = ct.id
            )
            SELECT name, depth FROM cat_tree ORDER BY depth, name
        ");
    }

    public function testRightJoin(): void
    {
        $this->mysqli->query("INSERT INTO mi_rc_students (id, name) VALUES (1, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_rc_students (id, name) VALUES (2, 'Bob')");

        $this->mysqli->query("INSERT INTO mi_rc_courses (id, title) VALUES (1, 'Math')");
        $this->mysqli->query("INSERT INTO mi_rc_courses (id, title) VALUES (2, 'Science')");
        $this->mysqli->query("INSERT INTO mi_rc_courses (id, title) VALUES (3, 'History')");

        $this->mysqli->query("INSERT INTO mi_rc_enrollments (student_id, course_id) VALUES (1, 1)");
        $this->mysqli->query("INSERT INTO mi_rc_enrollments (student_id, course_id) VALUES (1, 2)");
        $this->mysqli->query("INSERT INTO mi_rc_enrollments (student_id, course_id) VALUES (2, 1)");

        $result = $this->mysqli->query("
            SELECT c.title, s.name
            FROM mi_rc_enrollments e
            RIGHT JOIN mi_rc_courses c ON e.course_id = c.id
            LEFT JOIN mi_rc_students s ON e.student_id = s.id
            ORDER BY c.title, s.name
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $history = array_filter($rows, fn($r) => $r['title'] === 'History');
        $this->assertCount(1, $history);
        $historyRow = array_values($history)[0];
        $this->assertNull($historyRow['name']);

        $math = array_filter($rows, fn($r) => $r['title'] === 'Math');
        $this->assertCount(2, $math);
    }
}
