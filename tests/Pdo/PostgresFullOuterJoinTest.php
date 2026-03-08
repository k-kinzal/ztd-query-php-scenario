<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests FULL OUTER JOIN on PostgreSQL — a join type not available on MySQL or SQLite.
 * Verifies that CTE rewriter correctly handles FULL OUTER JOIN with shadow tables.
 * @spec SPEC-3.3d
 */
class PostgresFullOuterJoinTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_foj_students (id INT PRIMARY KEY, name VARCHAR(255))',
            'CREATE TABLE pg_foj_scores (id INT PRIMARY KEY, student_id INT, subject VARCHAR(100), score INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_foj_students', 'pg_foj_scores'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_foj_students (id, name) VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pg_foj_students (id, name) VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO pg_foj_students (id, name) VALUES (3, 'Charlie')");
        $this->pdo->exec("INSERT INTO pg_foj_scores (id, student_id, subject, score) VALUES (1, 1, 'Math', 95)");
        $this->pdo->exec("INSERT INTO pg_foj_scores (id, student_id, subject, score) VALUES (2, 1, 'Science', 88)");
        $this->pdo->exec("INSERT INTO pg_foj_scores (id, student_id, subject, score) VALUES (3, 2, 'Math', 72)");
        // Note: Charlie (id=3) has no scores, and there's a score for student_id=4 (no student)
        $this->pdo->exec("INSERT INTO pg_foj_scores (id, student_id, subject, score) VALUES (4, 4, 'Math', 60)");
    }

    public function testFullOuterJoin(): void
    {
        $stmt = $this->pdo->query("
            SELECT s.name, sc.subject, sc.score
            FROM pg_foj_students s
            FULL OUTER JOIN pg_foj_scores sc ON s.id = sc.student_id
            ORDER BY s.name NULLS LAST, sc.subject
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // Alice: Math(95), Science(88); Bob: Math(72); Charlie: NULL; orphan score: NULL, Math(60)
        $this->assertCount(5, $rows);

        // Alice's scores
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Math', $rows[0]['subject']);
        $this->assertSame('Alice', $rows[1]['name']);
        $this->assertSame('Science', $rows[1]['subject']);

        // Bob's score
        $this->assertSame('Bob', $rows[2]['name']);

        // Charlie (no scores — name present, subject/score NULL)
        $this->assertSame('Charlie', $rows[3]['name']);
        $this->assertNull($rows[3]['subject']);

        // Orphan score (no student — name NULL, subject/score present)
        $this->assertNull($rows[4]['name']);
        $this->assertSame(60, (int) $rows[4]['score']);
    }

    public function testFullOuterJoinAfterDelete(): void
    {
        // Delete Alice's scores
        $this->pdo->exec("DELETE FROM pg_foj_scores WHERE student_id = 1");

        $stmt = $this->pdo->query("
            SELECT s.name, sc.subject
            FROM pg_foj_students s
            FULL OUTER JOIN pg_foj_scores sc ON s.id = sc.student_id
            ORDER BY s.name NULLS LAST
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // Alice: NULL (no scores); Bob: Math; Charlie: NULL; orphan: Math
        $this->assertCount(4, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertNull($rows[0]['subject']);
    }

    public function testFullOuterJoinWithPreparedStatement(): void
    {
        $stmt = $this->pdo->prepare("
            SELECT s.name, sc.subject, sc.score
            FROM pg_foj_students s
            FULL OUTER JOIN pg_foj_scores sc ON s.id = sc.student_id
            WHERE sc.score > ? OR sc.score IS NULL
            ORDER BY s.name NULLS LAST
        ");
        $stmt->execute([80]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // Alice: Math(95), Science(88); Charlie: NULL; excluding Bob(72) and orphan(60)
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Charlie', $names);
    }
}
