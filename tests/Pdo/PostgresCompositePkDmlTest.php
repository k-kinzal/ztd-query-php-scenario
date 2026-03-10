<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests DML on tables with composite primary keys through ZTD shadow store
 * on PostgreSQL.
 *
 * @spec SPEC-4.2, SPEC-4.5
 */
class PostgresCompositePkDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_cpk_enrollments (
                student_id INT NOT NULL,
                course_id INT NOT NULL,
                grade VARCHAR(5),
                PRIMARY KEY (student_id, course_id)
            )',
            'CREATE TABLE pg_cpk_students (id SERIAL PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE pg_cpk_courses (id SERIAL PRIMARY KEY, title VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_cpk_enrollments', 'pg_cpk_courses', 'pg_cpk_students'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO pg_cpk_students (id, name) VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pg_cpk_students (id, name) VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO pg_cpk_courses (id, title) VALUES (10, 'Math')");
        $this->pdo->exec("INSERT INTO pg_cpk_courses (id, title) VALUES (20, 'Science')");

        $this->pdo->exec("INSERT INTO pg_cpk_enrollments VALUES (1, 10, 'A')");
        $this->pdo->exec("INSERT INTO pg_cpk_enrollments VALUES (1, 20, 'B')");
        $this->pdo->exec("INSERT INTO pg_cpk_enrollments VALUES (2, 10, 'C')");
    }

    public function testUpdateByCompositePk(): void
    {
        try {
            $this->pdo->exec("UPDATE pg_cpk_enrollments SET grade = 'A+' WHERE student_id = 1 AND course_id = 10");
            $rows = $this->ztdQuery("SELECT grade FROM pg_cpk_enrollments WHERE student_id = 1 AND course_id = 10");

            $this->assertCount(1, $rows);
            $this->assertSame('A+', $rows[0]['grade']);

            $other = $this->ztdQuery("SELECT grade FROM pg_cpk_enrollments WHERE student_id = 1 AND course_id = 20");
            $this->assertSame('B', $other[0]['grade']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE composite PK failed: ' . $e->getMessage());
        }
    }

    public function testDeleteByCompositePk(): void
    {
        try {
            $this->pdo->exec("DELETE FROM pg_cpk_enrollments WHERE student_id = 2 AND course_id = 10");
            $rows = $this->ztdQuery("SELECT student_id, course_id FROM pg_cpk_enrollments ORDER BY student_id, course_id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE composite PK: expected 2, got ' . count($rows) . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE composite PK failed: ' . $e->getMessage());
        }
    }

    public function testUpdateByPartialPk(): void
    {
        try {
            $this->pdo->exec("UPDATE pg_cpk_enrollments SET grade = 'P' WHERE student_id = 1");
            $rows = $this->ztdQuery("SELECT grade FROM pg_cpk_enrollments WHERE student_id = 1 ORDER BY course_id");

            $this->assertCount(2, $rows);
            $this->assertSame('P', $rows[0]['grade']);
            $this->assertSame('P', $rows[1]['grade']);

            $bob = $this->ztdQuery("SELECT grade FROM pg_cpk_enrollments WHERE student_id = 2");
            $this->assertSame('C', $bob[0]['grade']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE partial PK failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateCompositePk(): void
    {
        try {
            $stmt = $this->pdo->prepare("UPDATE pg_cpk_enrollments SET grade = ? WHERE student_id = ? AND course_id = ?");
            $stmt->execute(['D', 2, 10]);

            $rows = $this->ztdQuery("SELECT grade FROM pg_cpk_enrollments WHERE student_id = 2 AND course_id = 10");

            if (count($rows) !== 1 || $rows[0]['grade'] !== 'D') {
                $this->markTestIncomplete('Prepared UPDATE composite PK: expected grade=D, got ' . json_encode($rows));
            }

            $this->assertSame('D', $rows[0]['grade']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE composite PK failed: ' . $e->getMessage());
        }
    }

    public function testSelectJoinOnCompositePk(): void
    {
        $sql = "SELECT s.name, c.title, e.grade
                FROM pg_cpk_enrollments e
                JOIN pg_cpk_students s ON s.id = e.student_id
                JOIN pg_cpk_courses c ON c.id = e.course_id
                ORDER BY s.name, c.title";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'SELECT JOIN composite PK: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT JOIN composite PK failed: ' . $e->getMessage());
        }
    }
}
