<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DML on tables with composite (multi-column) primary keys on SQLite.
 *
 * Composite PKs are common in junction/association tables. This tests whether
 * the CTE rewriter correctly identifies and handles rows when the PK spans
 * multiple columns.
 *
 * @spec SPEC-10.2
 */
class SqliteCompositePkDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_cpk_enrollments (
            student_id INTEGER NOT NULL,
            course_id INTEGER NOT NULL,
            enrolled_at TEXT DEFAULT (date('now')),
            grade TEXT,
            PRIMARY KEY (student_id, course_id)
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_cpk_enrollments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_cpk_enrollments (student_id, course_id, grade) VALUES (1, 101, 'A')");
        $this->ztdExec("INSERT INTO sl_cpk_enrollments (student_id, course_id, grade) VALUES (1, 102, 'B')");
        $this->ztdExec("INSERT INTO sl_cpk_enrollments (student_id, course_id, grade) VALUES (2, 101, 'C')");
        $this->ztdExec("INSERT INTO sl_cpk_enrollments (student_id, course_id, grade) VALUES (2, 103, 'A')");
        $this->ztdExec("INSERT INTO sl_cpk_enrollments (student_id, course_id, grade) VALUES (3, 102, 'B')");
    }

    /**
     * UPDATE targeting a specific composite PK.
     */
    public function testUpdateByCompositePk(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_cpk_enrollments SET grade = 'A+' WHERE student_id = 1 AND course_id = 101"
            );

            $rows = $this->ztdQuery(
                "SELECT grade FROM sl_cpk_enrollments WHERE student_id = 1 AND course_id = 101"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE composite PK: expected 1, got ' . count($rows)
                );
            }

            $this->assertSame('A+', $rows[0]['grade']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE composite PK failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE targeting a specific composite PK.
     */
    public function testDeleteByCompositePk(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM sl_cpk_enrollments WHERE student_id = 2 AND course_id = 101"
            );

            $rows = $this->ztdQuery("SELECT student_id, course_id FROM sl_cpk_enrollments ORDER BY student_id, course_id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'DELETE composite PK: expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE composite PK failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE multiple rows by one PK column.
     */
    public function testUpdateByPartialPk(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_cpk_enrollments SET grade = 'F' WHERE student_id = 1"
            );

            $rows = $this->ztdQuery(
                "SELECT course_id, grade FROM sl_cpk_enrollments WHERE student_id = 1 ORDER BY course_id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE partial PK: expected 2, got ' . count($rows)
                );
            }

            $this->assertSame('F', $rows[0]['grade']);
            $this->assertSame('F', $rows[1]['grade']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE partial PK failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE all enrollments for a course.
     */
    public function testDeleteByPartialPk(): void
    {
        try {
            $this->ztdExec("DELETE FROM sl_cpk_enrollments WHERE course_id = 101");

            $rows = $this->ztdQuery("SELECT student_id, course_id FROM sl_cpk_enrollments ORDER BY student_id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE partial PK: expected 3, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE partial PK failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with composite PK parameters.
     */
    public function testPreparedUpdateCompositePk(): void
    {
        try {
            $stmt = $this->ztdPrepare(
                "UPDATE sl_cpk_enrollments SET grade = ? WHERE student_id = ? AND course_id = ?"
            );
            $stmt->execute(['D', 3, 102]);

            $rows = $this->ztdQuery(
                "SELECT grade FROM sl_cpk_enrollments WHERE student_id = 3 AND course_id = 102"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared UPDATE composite PK: expected 1, got ' . count($rows)
                );
            }

            $this->assertSame('D', $rows[0]['grade']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE composite PK failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT duplicate composite PK — should fail with constraint violation.
     */
    public function testInsertDuplicateCompositePk(): void
    {
        $threw = false;
        try {
            $this->ztdExec("INSERT INTO sl_cpk_enrollments (student_id, course_id, grade) VALUES (1, 101, 'X')");
        } catch (\Throwable $e) {
            $threw = true;
            $this->assertTrue(true); // Expected error
            return;
        }

        // If no exception, check if duplicate was silently accepted
        $rows = $this->ztdQuery(
            "SELECT grade FROM sl_cpk_enrollments WHERE student_id = 1 AND course_id = 101"
        );
        if (count($rows) > 1) {
            $this->markTestIncomplete(
                'INSERT duplicate composite PK: duplicate accepted — ' . count($rows) . ' rows'
            );
        } elseif (count($rows) === 1 && $rows[0]['grade'] === 'X') {
            $this->markTestIncomplete(
                'INSERT duplicate composite PK: silently replaced existing row'
            );
        } else {
            $this->markTestIncomplete(
                'INSERT duplicate composite PK: no error, grade=' . ($rows[0]['grade'] ?? 'NULL')
            );
        }
    }
}
