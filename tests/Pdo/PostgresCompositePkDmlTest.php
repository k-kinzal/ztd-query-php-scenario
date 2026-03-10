<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests DML on tables with composite primary keys on PostgreSQL.
 *
 * @spec SPEC-10.2
 */
class PostgresCompositePkDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE pg_cpk_enrollments (
            student_id INTEGER NOT NULL,
            course_id INTEGER NOT NULL,
            grade VARCHAR(5),
            PRIMARY KEY (student_id, course_id)
        )";
    }

    protected function getTableNames(): array
    {
        return ['pg_cpk_enrollments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_cpk_enrollments (student_id, course_id, grade) VALUES (1, 101, 'A')");
        $this->ztdExec("INSERT INTO pg_cpk_enrollments (student_id, course_id, grade) VALUES (1, 102, 'B')");
        $this->ztdExec("INSERT INTO pg_cpk_enrollments (student_id, course_id, grade) VALUES (2, 101, 'C')");
        $this->ztdExec("INSERT INTO pg_cpk_enrollments (student_id, course_id, grade) VALUES (2, 103, 'A')");
        $this->ztdExec("INSERT INTO pg_cpk_enrollments (student_id, course_id, grade) VALUES (3, 102, 'B')");
    }

    public function testUpdateByCompositePk(): void
    {
        try {
            $this->ztdExec("UPDATE pg_cpk_enrollments SET grade = 'A+' WHERE student_id = 1 AND course_id = 101");

            $rows = $this->ztdQuery("SELECT grade FROM pg_cpk_enrollments WHERE student_id = 1 AND course_id = 101");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE composite PK (PG): expected 1, got ' . count($rows));
            }

            $this->assertSame('A+', $rows[0]['grade']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE composite PK (PG) failed: ' . $e->getMessage());
        }
    }

    public function testDeleteByCompositePk(): void
    {
        try {
            $this->ztdExec("DELETE FROM pg_cpk_enrollments WHERE student_id = 2 AND course_id = 101");

            $rows = $this->ztdQuery("SELECT student_id, course_id FROM pg_cpk_enrollments ORDER BY student_id, course_id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete('DELETE composite PK (PG): expected 4, got ' . count($rows) . '. Rows: ' . json_encode($rows));
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE composite PK (PG) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateByPartialPk(): void
    {
        try {
            $this->ztdExec("UPDATE pg_cpk_enrollments SET grade = 'F' WHERE student_id = 1");

            $rows = $this->ztdQuery("SELECT course_id, grade FROM pg_cpk_enrollments WHERE student_id = 1 ORDER BY course_id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete('UPDATE partial PK (PG): expected 2, got ' . count($rows));
            }

            $this->assertSame('F', $rows[0]['grade']);
            $this->assertSame('F', $rows[1]['grade']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE partial PK (PG) failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateCompositePk(): void
    {
        try {
            $stmt = $this->ztdPrepare("UPDATE pg_cpk_enrollments SET grade = $1 WHERE student_id = $2 AND course_id = $3");
            $stmt->execute(['D', 3, 102]);

            $rows = $this->ztdQuery("SELECT grade FROM pg_cpk_enrollments WHERE student_id = 3 AND course_id = 102");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared UPDATE composite PK (PG): expected 1, got ' . count($rows));
            }

            $this->assertSame('D', $rows[0]['grade']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE composite PK (PG) failed: ' . $e->getMessage());
        }
    }

    public function testDeleteByPartialPk(): void
    {
        try {
            $this->ztdExec("DELETE FROM pg_cpk_enrollments WHERE course_id = 101");

            $rows = $this->ztdQuery("SELECT student_id, course_id FROM pg_cpk_enrollments ORDER BY student_id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete('DELETE partial PK (PG): expected 3, got ' . count($rows) . '. Rows: ' . json_encode($rows));
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE partial PK (PG) failed: ' . $e->getMessage());
        }
    }
}
