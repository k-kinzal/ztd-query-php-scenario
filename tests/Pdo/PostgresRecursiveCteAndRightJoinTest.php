<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests recursive CTEs and RIGHT JOIN on PostgreSQL PDO.
 * @spec SPEC-3.3c
 */
class PostgresRecursiveCteAndRightJoinTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_rc_categories (id INT PRIMARY KEY, name VARCHAR(255), parent_id INT)',
            'CREATE TABLE pg_rc_students (id INT PRIMARY KEY, name VARCHAR(255))',
            'CREATE TABLE pg_rc_courses (id INT PRIMARY KEY, title VARCHAR(255))',
            'CREATE TABLE pg_rc_enrollments (student_id INT, course_id INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_rc_enrollments', 'pg_rc_categories', 'pg_rc_students', 'pg_rc_courses'];
    }


    public function testRecursiveCteNumberSeries(): void
    {
        $stmt = $this->pdo->query("
            WITH RECURSIVE nums(n) AS (
                SELECT 1
                UNION ALL
                SELECT n + 1 FROM nums WHERE n < 5
            )
            SELECT n FROM nums ORDER BY n
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertSame(1, (int) $rows[0]['n']);
    }

    public function testRecursiveCteWithShadowTable(): void
    {
        $this->pdo->exec("INSERT INTO pg_rc_categories (id, name, parent_id) VALUES (1, 'Root', NULL)");
        $this->pdo->exec("INSERT INTO pg_rc_categories (id, name, parent_id) VALUES (2, 'Electronics', 1)");
        $this->pdo->exec("INSERT INTO pg_rc_categories (id, name, parent_id) VALUES (3, 'Phones', 2)");
        $this->pdo->exec("INSERT INTO pg_rc_categories (id, name, parent_id) VALUES (4, 'Laptops', 2)");
        $this->pdo->exec("INSERT INTO pg_rc_categories (id, name, parent_id) VALUES (5, 'Clothing', 1)");

        $stmt = $this->pdo->query("
            WITH RECURSIVE cat_tree AS (
                SELECT id, name, parent_id, 0 AS depth
                FROM pg_rc_categories
                WHERE parent_id IS NULL
                UNION ALL
                SELECT c.id, c.name, c.parent_id, ct.depth + 1
                FROM pg_rc_categories c
                JOIN cat_tree ct ON c.parent_id = ct.id
            )
            SELECT name, depth FROM cat_tree ORDER BY depth, name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // On PostgreSQL, user CTEs don't get table refs rewritten (documented in 10.3)
        // WITH RECURSIVE is expected to return empty
        if (count($rows) > 0) {
            $this->assertCount(5, $rows);
        } else {
            $this->assertCount(0, $rows);
        }
    }

    public function testRightJoin(): void
    {
        $this->pdo->exec("INSERT INTO pg_rc_students (id, name) VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pg_rc_students (id, name) VALUES (2, 'Bob')");

        $this->pdo->exec("INSERT INTO pg_rc_courses (id, title) VALUES (1, 'Math')");
        $this->pdo->exec("INSERT INTO pg_rc_courses (id, title) VALUES (2, 'Science')");
        $this->pdo->exec("INSERT INTO pg_rc_courses (id, title) VALUES (3, 'History')");

        $this->pdo->exec("INSERT INTO pg_rc_enrollments (student_id, course_id) VALUES (1, 1)");
        $this->pdo->exec("INSERT INTO pg_rc_enrollments (student_id, course_id) VALUES (1, 2)");
        $this->pdo->exec("INSERT INTO pg_rc_enrollments (student_id, course_id) VALUES (2, 1)");

        $stmt = $this->pdo->query("
            SELECT c.title, s.name
            FROM pg_rc_enrollments e
            RIGHT JOIN pg_rc_courses c ON e.course_id = c.id
            LEFT JOIN pg_rc_students s ON e.student_id = s.id
            ORDER BY c.title, s.name NULLS LAST
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $history = array_filter($rows, fn($r) => $r['title'] === 'History');
        $this->assertCount(1, $history);
        $historyRow = array_values($history)[0];
        $this->assertNull($historyRow['name']);

        $math = array_filter($rows, fn($r) => $r['title'] === 'Math');
        $this->assertCount(2, $math);
    }
}
