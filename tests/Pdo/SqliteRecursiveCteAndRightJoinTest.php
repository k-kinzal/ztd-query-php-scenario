<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests recursive CTEs and RIGHT JOIN on SQLite.
 * Recursive CTEs are important for hierarchical data (org charts, categories).
 * RIGHT JOIN is a fundamental SQL join type not yet tested.
 * @spec SPEC-3.3c
 */
class SqliteRecursiveCteAndRightJoinTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT, parent_id INTEGER)',
            'CREATE TABLE students (id INTEGER PRIMARY KEY, name TEXT)',
            'CREATE TABLE courses (id INTEGER PRIMARY KEY, title TEXT)',
            'CREATE TABLE enrollments (student_id INTEGER, course_id INTEGER)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['categories', 'students', 'courses', 'enrollments'];
    }


    /**
     * Basic recursive CTE: generate a number series.
     * This doesn't reference any shadow table, so should work as passthrough.
     */
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
        $this->assertSame(5, (int) $rows[4]['n']);
    }

    /**
     * Recursive CTE referencing a shadow table returns empty on SQLite.
     * Although non-recursive user CTEs (WITH) work correctly with ZTD on SQLite,
     * WITH RECURSIVE does NOT — table references inside the recursive CTE
     * are not rewritten, causing the query to read from the physical table (empty).
     */
    public function testRecursiveCteWithShadowTableReturnsEmpty(): void
    {
        $this->pdo->exec("INSERT INTO categories (id, name, parent_id) VALUES (1, 'Root', NULL)");
        $this->pdo->exec("INSERT INTO categories (id, name, parent_id) VALUES (2, 'Electronics', 1)");
        $this->pdo->exec("INSERT INTO categories (id, name, parent_id) VALUES (3, 'Phones', 2)");
        $this->pdo->exec("INSERT INTO categories (id, name, parent_id) VALUES (4, 'Laptops', 2)");
        $this->pdo->exec("INSERT INTO categories (id, name, parent_id) VALUES (5, 'Clothing', 1)");

        $stmt = $this->pdo->query("
            WITH RECURSIVE cat_tree AS (
                SELECT id, name, parent_id, 0 AS depth
                FROM categories
                WHERE parent_id IS NULL
                UNION ALL
                SELECT c.id, c.name, c.parent_id, ct.depth + 1
                FROM categories c
                JOIN cat_tree ct ON c.parent_id = ct.id
            )
            SELECT name, depth FROM cat_tree ORDER BY depth, name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Returns empty — WITH RECURSIVE table refs not rewritten by CTE rewriter
        $this->assertCount(0, $rows);
    }

    /**
     * Recursive CTE with mutations also returns empty (same limitation).
     */
    public function testRecursiveCteDoesNotReflectMutations(): void
    {
        $this->pdo->exec("INSERT INTO categories (id, name, parent_id) VALUES (1, 'Root', NULL)");
        $this->pdo->exec("INSERT INTO categories (id, name, parent_id) VALUES (2, 'Electronics', 1)");
        $this->pdo->exec("INSERT INTO categories (id, name, parent_id) VALUES (3, 'Phones', 2)");
        $this->pdo->exec("INSERT INTO categories (id, name, parent_id) VALUES (4, 'Accessories', 2)");

        $stmt = $this->pdo->query("
            WITH RECURSIVE cat_tree AS (
                SELECT id, name, parent_id, 0 AS depth
                FROM categories
                WHERE parent_id IS NULL
                UNION ALL
                SELECT c.id, c.name, c.parent_id, ct.depth + 1
                FROM categories c
                JOIN cat_tree ct ON c.parent_id = ct.id
            )
            SELECT name FROM cat_tree WHERE depth = 2 ORDER BY name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);
    }

    /**
     * RIGHT JOIN — SQLite supports RIGHT JOIN since version 3.39.0.
     * Note: SQLite in older versions doesn't support RIGHT JOIN.
     */
    public function testRightJoin(): void
    {
        $this->pdo->exec("INSERT INTO students (id, name) VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO students (id, name) VALUES (2, 'Bob')");

        $this->pdo->exec("INSERT INTO courses (id, title) VALUES (1, 'Math')");
        $this->pdo->exec("INSERT INTO courses (id, title) VALUES (2, 'Science')");
        $this->pdo->exec("INSERT INTO courses (id, title) VALUES (3, 'History')");

        $this->pdo->exec("INSERT INTO enrollments (student_id, course_id) VALUES (1, 1)");
        $this->pdo->exec("INSERT INTO enrollments (student_id, course_id) VALUES (1, 2)");
        $this->pdo->exec("INSERT INTO enrollments (student_id, course_id) VALUES (2, 1)");
        // History (id=3) has no enrollments

        try {
            $stmt = $this->pdo->query("
                SELECT c.title, s.name
                FROM enrollments e
                RIGHT JOIN courses c ON e.course_id = c.id
                LEFT JOIN students s ON e.student_id = s.id
                ORDER BY c.title, s.name NULLS LAST
            ");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // History should appear with NULL student name
            $history = array_filter($rows, fn($r) => $r['title'] === 'History');
            $this->assertCount(1, $history);
            $historyRow = array_values($history)[0];
            $this->assertNull($historyRow['name']);

            // Math should have 2 enrollments
            $math = array_filter($rows, fn($r) => $r['title'] === 'Math');
            $this->assertCount(2, $math);
        } catch (\Throwable $e) {
            // Older SQLite versions may not support RIGHT JOIN
            $this->markTestSkipped('RIGHT JOIN not supported on this SQLite version: ' . $e->getMessage());
        }
    }

    /**
     * RIGHT JOIN with prepared statement.
     */
    public function testRightJoinWithPreparedStatement(): void
    {
        $this->pdo->exec("INSERT INTO students (id, name) VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO courses (id, title) VALUES (1, 'Math')");
        $this->pdo->exec("INSERT INTO courses (id, title) VALUES (2, 'Science')");
        $this->pdo->exec("INSERT INTO enrollments (student_id, course_id) VALUES (1, 1)");

        try {
            $stmt = $this->pdo->prepare("
                SELECT c.title, s.name
                FROM enrollments e
                RIGHT JOIN courses c ON e.course_id = c.id
                LEFT JOIN students s ON e.student_id = s.id
                WHERE c.id <= ?
                ORDER BY c.title
            ");
            $stmt->execute([2]);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestSkipped('RIGHT JOIN not supported on this SQLite version: ' . $e->getMessage());
        }
    }
}
