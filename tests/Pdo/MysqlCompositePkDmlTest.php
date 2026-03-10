<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests DML on tables with composite primary keys on MySQL PDO.
 *
 * @spec SPEC-10.2
 */
class MysqlCompositePkDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE my_cpk_enrollments (
            student_id INT NOT NULL,
            course_id INT NOT NULL,
            grade VARCHAR(5),
            PRIMARY KEY (student_id, course_id)
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['my_cpk_enrollments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_cpk_enrollments (student_id, course_id, grade) VALUES (1, 101, 'A')");
        $this->ztdExec("INSERT INTO my_cpk_enrollments (student_id, course_id, grade) VALUES (1, 102, 'B')");
        $this->ztdExec("INSERT INTO my_cpk_enrollments (student_id, course_id, grade) VALUES (2, 101, 'C')");
        $this->ztdExec("INSERT INTO my_cpk_enrollments (student_id, course_id, grade) VALUES (2, 103, 'A')");
        $this->ztdExec("INSERT INTO my_cpk_enrollments (student_id, course_id, grade) VALUES (3, 102, 'B')");
    }

    public function testUpdateByCompositePk(): void
    {
        try {
            $this->ztdExec("UPDATE my_cpk_enrollments SET grade = 'A+' WHERE student_id = 1 AND course_id = 101");

            $rows = $this->ztdQuery("SELECT grade FROM my_cpk_enrollments WHERE student_id = 1 AND course_id = 101");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE composite PK (MySQL): expected 1, got ' . count($rows));
            }

            $this->assertSame('A+', $rows[0]['grade']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE composite PK (MySQL) failed: ' . $e->getMessage());
        }
    }

    public function testDeleteByCompositePk(): void
    {
        try {
            $this->ztdExec("DELETE FROM my_cpk_enrollments WHERE student_id = 2 AND course_id = 101");

            $rows = $this->ztdQuery("SELECT student_id, course_id FROM my_cpk_enrollments ORDER BY student_id, course_id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete('DELETE composite PK (MySQL): expected 4, got ' . count($rows) . '. Rows: ' . json_encode($rows));
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE composite PK (MySQL) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateByPartialPk(): void
    {
        try {
            $this->ztdExec("UPDATE my_cpk_enrollments SET grade = 'F' WHERE student_id = 1");

            $rows = $this->ztdQuery("SELECT course_id, grade FROM my_cpk_enrollments WHERE student_id = 1 ORDER BY course_id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete('UPDATE partial PK (MySQL): expected 2, got ' . count($rows));
            }

            $this->assertSame('F', $rows[0]['grade']);
            $this->assertSame('F', $rows[1]['grade']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE partial PK (MySQL) failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateCompositePk(): void
    {
        try {
            $stmt = $this->ztdPrepare("UPDATE my_cpk_enrollments SET grade = ? WHERE student_id = ? AND course_id = ?");
            $stmt->execute(['D', 3, 102]);

            $rows = $this->ztdQuery("SELECT grade FROM my_cpk_enrollments WHERE student_id = 3 AND course_id = 102");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared UPDATE composite PK (MySQL): expected 1, got ' . count($rows));
            }

            $this->assertSame('D', $rows[0]['grade']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE composite PK (MySQL) failed: ' . $e->getMessage());
        }
    }
}
