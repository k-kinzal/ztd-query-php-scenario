<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests recursive CTEs and RIGHT JOIN on MySQL PDO.
 */
class MysqlRecursiveCteAndRightJoinTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_rc_enrollments');
        $raw->exec('DROP TABLE IF EXISTS mysql_rc_categories');
        $raw->exec('DROP TABLE IF EXISTS mysql_rc_students');
        $raw->exec('DROP TABLE IF EXISTS mysql_rc_courses');
        $raw->exec('CREATE TABLE mysql_rc_categories (id INT PRIMARY KEY, name VARCHAR(255), parent_id INT)');
        $raw->exec('CREATE TABLE mysql_rc_students (id INT PRIMARY KEY, name VARCHAR(255))');
        $raw->exec('CREATE TABLE mysql_rc_courses (id INT PRIMARY KEY, title VARCHAR(255))');
        $raw->exec('CREATE TABLE mysql_rc_enrollments (student_id INT, course_id INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
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

    /**
     * WITH RECURSIVE + shadow table fails on MySQL.
     * The CTE rewriter prepends its own WITH clause before the RECURSIVE keyword,
     * producing invalid SQL like: WITH ztd_shadow AS (...), RECURSIVE cat_tree AS (...)
     */
    public function testRecursiveCteWithShadowTableFails(): void
    {
        $this->pdo->exec("INSERT INTO mysql_rc_categories (id, name, parent_id) VALUES (1, 'Root', NULL)");
        $this->pdo->exec("INSERT INTO mysql_rc_categories (id, name, parent_id) VALUES (2, 'Electronics', 1)");

        $this->expectException(\Throwable::class);
        $this->pdo->query("
            WITH RECURSIVE cat_tree AS (
                SELECT id, name, parent_id, 0 AS depth
                FROM mysql_rc_categories
                WHERE parent_id IS NULL
                UNION ALL
                SELECT c.id, c.name, c.parent_id, ct.depth + 1
                FROM mysql_rc_categories c
                JOIN cat_tree ct ON c.parent_id = ct.id
            )
            SELECT name, depth FROM cat_tree ORDER BY depth, name
        ");
    }

    public function testRightJoin(): void
    {
        $this->pdo->exec("INSERT INTO mysql_rc_students (id, name) VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO mysql_rc_students (id, name) VALUES (2, 'Bob')");

        $this->pdo->exec("INSERT INTO mysql_rc_courses (id, title) VALUES (1, 'Math')");
        $this->pdo->exec("INSERT INTO mysql_rc_courses (id, title) VALUES (2, 'Science')");
        $this->pdo->exec("INSERT INTO mysql_rc_courses (id, title) VALUES (3, 'History')");

        $this->pdo->exec("INSERT INTO mysql_rc_enrollments (student_id, course_id) VALUES (1, 1)");
        $this->pdo->exec("INSERT INTO mysql_rc_enrollments (student_id, course_id) VALUES (1, 2)");
        $this->pdo->exec("INSERT INTO mysql_rc_enrollments (student_id, course_id) VALUES (2, 1)");

        $stmt = $this->pdo->query("
            SELECT c.title, s.name
            FROM mysql_rc_enrollments e
            RIGHT JOIN mysql_rc_courses c ON e.course_id = c.id
            LEFT JOIN mysql_rc_students s ON e.student_id = s.id
            ORDER BY c.title, s.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $history = array_filter($rows, fn($r) => $r['title'] === 'History');
        $this->assertCount(1, $history);
        $historyRow = array_values($history)[0];
        $this->assertNull($historyRow['name']);

        $math = array_filter($rows, fn($r) => $r['title'] === 'Math');
        $this->assertCount(2, $math);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_rc_enrollments');
        $raw->exec('DROP TABLE IF EXISTS mysql_rc_categories');
        $raw->exec('DROP TABLE IF EXISTS mysql_rc_students');
        $raw->exec('DROP TABLE IF EXISTS mysql_rc_courses');
    }
}
