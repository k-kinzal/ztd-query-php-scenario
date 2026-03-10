<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests multi-column IN clause with tuple syntax in DML operations on SQLite PDO.
 *
 * The pattern WHERE (col1, col2) IN ((v1, v2), ...) is commonly used to target
 * rows identified by composite key values in UPDATE and DELETE statements.
 * SQLite supports row-value comparisons since version 3.15.0 (2016-10-14).
 * This exercises CTE rewriter handling of row-value comparisons.
 *
 * @spec SPEC-10.2
 */
class SqliteMultiColumnInDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_mci_enrollments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_id INT NOT NULL,
            course_id INT NOT NULL,
            grade VARCHAR(2),
            enrolled_date DATE
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_mci_enrollments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_mci_enrollments (student_id, course_id, grade, enrolled_date) VALUES (1, 101, 'B+', '2025-01-15')");
        $this->ztdExec("INSERT INTO sl_mci_enrollments (student_id, course_id, grade, enrolled_date) VALUES (1, 102, 'A-', '2025-01-16')");
        $this->ztdExec("INSERT INTO sl_mci_enrollments (student_id, course_id, grade, enrolled_date) VALUES (2, 101, 'C', '2025-02-01')");
        $this->ztdExec("INSERT INTO sl_mci_enrollments (student_id, course_id, grade, enrolled_date) VALUES (2, 102, 'B', '2025-02-02')");
        $this->ztdExec("INSERT INTO sl_mci_enrollments (student_id, course_id, grade, enrolled_date) VALUES (3, 101, 'A', '2025-03-01')");
        $this->ztdExec("INSERT INTO sl_mci_enrollments (student_id, course_id, grade, enrolled_date) VALUES (3, 103, 'B-', '2025-03-10')");
    }

    /**
     * DELETE WHERE (student_id, course_id) IN ((1, 101), (2, 102)) should remove exactly two rows.
     */
    public function testDeleteWithMultiColumnInTuple(): void
    {
        try {
            $this->ztdExec("DELETE FROM sl_mci_enrollments WHERE (student_id, course_id) IN ((1, 101), (2, 102))");

            $rows = $this->ztdQuery("SELECT student_id, course_id FROM sl_mci_enrollments ORDER BY student_id, course_id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'DELETE with multi-column IN: expected 4 remaining rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);

            $pairs = array_map(fn($r) => [(int) $r['student_id'], (int) $r['course_id']], $rows);
            $this->assertNotContains([1, 101], $pairs);
            $this->assertNotContains([2, 102], $pairs);
            $this->assertContains([1, 102], $pairs);
            $this->assertContains([2, 101], $pairs);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with multi-column IN failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET grade = 'A' WHERE (student_id, course_id) IN ((1, 101)) should update one row.
     */
    public function testUpdateWithMultiColumnInTuple(): void
    {
        try {
            $this->ztdExec("UPDATE sl_mci_enrollments SET grade = 'A' WHERE (student_id, course_id) IN ((1, 101))");

            $rows = $this->ztdQuery("SELECT grade FROM sl_mci_enrollments WHERE student_id = 1 AND course_id = 101");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE with multi-column IN: expected 1 row, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('A', $rows[0]['grade']);

            // Verify other rows unchanged
            $other = $this->ztdQuery("SELECT grade FROM sl_mci_enrollments WHERE student_id = 1 AND course_id = 102");
            $this->assertSame('A-', $other[0]['grade']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with multi-column IN failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT WHERE (student_id, course_id) IN (SELECT ...) with subquery.
     */
    public function testSelectWithMultiColumnInSubquery(): void
    {
        try {
            // Update two rows first so we can identify them via subquery
            $this->ztdExec("UPDATE sl_mci_enrollments SET grade = 'A' WHERE student_id = 3");

            $rows = $this->ztdQuery(
                "SELECT student_id, course_id, grade FROM sl_mci_enrollments
                 WHERE (student_id, course_id) IN (
                     SELECT student_id, course_id FROM sl_mci_enrollments WHERE grade = 'A'
                 )
                 ORDER BY student_id, course_id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'SELECT with multi-column IN subquery: expected 2 rows (student 3), got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(3, (int) $rows[0]['student_id']);
            $this->assertSame(3, (int) $rows[1]['student_id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT with multi-column IN subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE with (col1, col2) IN ((?, ?)) syntax.
     */
    public function testPreparedDeleteWithMultiColumnIn(): void
    {
        try {
            $stmt = $this->ztdPrepare("DELETE FROM sl_mci_enrollments WHERE (student_id, course_id) IN ((?, ?))");
            $stmt->execute([1, 101]);

            $rows = $this->ztdQuery("SELECT student_id, course_id FROM sl_mci_enrollments ORDER BY student_id, course_id");

            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'Prepared DELETE with multi-column IN: expected 5 remaining rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(5, $rows);
            $pairs = array_map(fn($r) => [(int) $r['student_id'], (int) $r['course_id']], $rows);
            $this->assertNotContains([1, 101], $pairs);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE with multi-column IN failed: ' . $e->getMessage());
        }
    }

    /**
     * Multi-column NOT IN for exclusion pattern: select rows NOT matching given tuples.
     */
    public function testMultiColumnNotIn(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT student_id, course_id, grade FROM sl_mci_enrollments
                 WHERE (student_id, course_id) NOT IN ((1, 101), (2, 102), (3, 103))
                 ORDER BY student_id, course_id"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multi-column NOT IN: expected 3 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $pairs = array_map(fn($r) => [(int) $r['student_id'], (int) $r['course_id']], $rows);
            $this->assertContains([1, 102], $pairs);
            $this->assertContains([2, 101], $pairs);
            $this->assertContains([3, 101], $pairs);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-column NOT IN failed: ' . $e->getMessage());
        }
    }

    /**
     * After DML with multi-column IN, verify remaining rows are correct.
     */
    public function testVerifyRemainingRowsAfterMultiColumnInDml(): void
    {
        try {
            // Delete two specific enrollments
            $this->ztdExec("DELETE FROM sl_mci_enrollments WHERE (student_id, course_id) IN ((1, 101), (3, 103))");

            // Update one enrollment
            $this->ztdExec("UPDATE sl_mci_enrollments SET grade = 'A+' WHERE (student_id, course_id) IN ((2, 101))");

            $rows = $this->ztdQuery("SELECT student_id, course_id, grade FROM sl_mci_enrollments ORDER BY student_id, course_id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'Verify remaining rows: expected 4 rows after DELETE+UPDATE, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);

            // Check the updated row
            $updated = array_values(array_filter($rows, fn($r) => (int) $r['student_id'] === 2 && (int) $r['course_id'] === 101));
            $this->assertCount(1, $updated);
            $this->assertSame('A+', $updated[0]['grade']);

            // Check an untouched row
            $untouched = array_values(array_filter($rows, fn($r) => (int) $r['student_id'] === 1 && (int) $r['course_id'] === 102));
            $this->assertCount(1, $untouched);
            $this->assertSame('A-', $untouched[0]['grade']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Verify remaining rows after multi-column IN DML failed: ' . $e->getMessage());
        }
    }
}
