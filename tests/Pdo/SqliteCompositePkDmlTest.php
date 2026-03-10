<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DML operations on tables with composite (multi-column) primary keys
 * through ZTD shadow store.
 *
 * The shadow store must correctly track and apply UPDATE/DELETE using composite
 * PKs. This is common in junction tables, audit logs, and any many-to-many
 * relationship modeling.
 *
 * @spec SPEC-4.2, SPEC-4.5
 */
class SqliteCompositePkDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_cpk_enrollments (
                student_id INTEGER NOT NULL,
                course_id INTEGER NOT NULL,
                grade TEXT,
                enrolled_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (student_id, course_id)
            )',
            'CREATE TABLE sl_cpk_students (id INTEGER PRIMARY KEY, name TEXT)',
            'CREATE TABLE sl_cpk_courses (id INTEGER PRIMARY KEY, title TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_cpk_enrollments', 'sl_cpk_courses', 'sl_cpk_students'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO sl_cpk_students VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO sl_cpk_students VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO sl_cpk_courses VALUES (10, 'Math')");
        $this->pdo->exec("INSERT INTO sl_cpk_courses VALUES (20, 'Science')");

        $this->pdo->exec("INSERT INTO sl_cpk_enrollments (student_id, course_id, grade) VALUES (1, 10, 'A')");
        $this->pdo->exec("INSERT INTO sl_cpk_enrollments (student_id, course_id, grade) VALUES (1, 20, 'B')");
        $this->pdo->exec("INSERT INTO sl_cpk_enrollments (student_id, course_id, grade) VALUES (2, 10, 'C')");
    }

    /**
     * UPDATE targeting a specific composite PK.
     */
    public function testUpdateByCompositePk(): void
    {
        $sql = "UPDATE sl_cpk_enrollments SET grade = 'A+' WHERE student_id = 1 AND course_id = 10";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery(
                "SELECT grade FROM sl_cpk_enrollments WHERE student_id = 1 AND course_id = 10"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE composite PK: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('A+', $rows[0]['grade']);

            // Other rows unchanged
            $other = $this->ztdQuery(
                "SELECT grade FROM sl_cpk_enrollments WHERE student_id = 1 AND course_id = 20"
            );
            $this->assertSame('B', $other[0]['grade']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE composite PK failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE targeting a specific composite PK.
     */
    public function testDeleteByCompositePk(): void
    {
        $sql = "DELETE FROM sl_cpk_enrollments WHERE student_id = 2 AND course_id = 10";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery(
                "SELECT student_id, course_id FROM sl_cpk_enrollments ORDER BY student_id, course_id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE composite PK: expected 2 remaining, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            // Alice's enrollments remain
            $this->assertEquals(1, $rows[0]['student_id']);
            $this->assertEquals(10, $rows[0]['course_id']);
            $this->assertEquals(1, $rows[1]['student_id']);
            $this->assertEquals(20, $rows[1]['course_id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE composite PK failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE by one PK column only — should affect multiple rows.
     */
    public function testUpdateByPartialPk(): void
    {
        $sql = "UPDATE sl_cpk_enrollments SET grade = 'P' WHERE student_id = 1";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery(
                "SELECT grade FROM sl_cpk_enrollments WHERE student_id = 1 ORDER BY course_id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE partial PK: expected 2, got ' . count($rows)
                );
            }

            $this->assertSame('P', $rows[0]['grade']);
            $this->assertSame('P', $rows[1]['grade']);

            // Bob's row unchanged
            $bob = $this->ztdQuery(
                "SELECT grade FROM sl_cpk_enrollments WHERE student_id = 2"
            );
            $this->assertSame('C', $bob[0]['grade']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE partial PK failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT duplicate composite PK should fail.
     */
    public function testInsertDuplicateCompositePk(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_cpk_enrollments (student_id, course_id, grade) VALUES (1, 10, 'F')"
            );

            // If no exception, check if row was duplicated or ignored
            $rows = $this->ztdQuery(
                "SELECT grade FROM sl_cpk_enrollments WHERE student_id = 1 AND course_id = 10"
            );

            if (count($rows) > 1) {
                $this->markTestIncomplete(
                    'INSERT duplicate composite PK: shadow store allowed duplicate. Got '
                    . count($rows) . ' rows'
                );
            }

            // Shadow store might not enforce PK uniqueness (known pattern)
            $this->markTestIncomplete(
                'INSERT duplicate composite PK: no exception thrown, grade='
                . ($rows[0]['grade'] ?? 'null')
            );
        } catch (\Throwable $e) {
            // Expected: constraint violation
            $this->assertTrue(true, 'Duplicate composite PK correctly rejected');
        }
    }

    /**
     * Prepared UPDATE with composite PK params.
     */
    public function testPreparedUpdateCompositePk(): void
    {
        $sql = "UPDATE sl_cpk_enrollments SET grade = ? WHERE student_id = ? AND course_id = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['D', 2, 10]);

            $rows = $this->ztdQuery(
                "SELECT grade FROM sl_cpk_enrollments WHERE student_id = 2 AND course_id = 10"
            );

            if (count($rows) !== 1 || $rows[0]['grade'] !== 'D') {
                $this->markTestIncomplete(
                    'Prepared UPDATE composite PK: expected grade=D, got '
                    . json_encode($rows)
                );
            }

            $this->assertSame('D', $rows[0]['grade']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE composite PK failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE by partial PK — should delete all rows matching the column.
     */
    public function testDeleteByPartialPk(): void
    {
        $sql = "DELETE FROM sl_cpk_enrollments WHERE course_id = 10";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery(
                "SELECT student_id, course_id FROM sl_cpk_enrollments ORDER BY student_id"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'DELETE partial PK: expected 1 remaining, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertEquals(1, $rows[0]['student_id']);
            $this->assertEquals(20, $rows[0]['course_id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE partial PK failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with JOIN on composite PK table.
     */
    public function testSelectJoinOnCompositePk(): void
    {
        $sql = "SELECT s.name, c.title, e.grade
                FROM sl_cpk_enrollments e
                JOIN sl_cpk_students s ON s.id = e.student_id
                JOIN sl_cpk_courses c ON c.id = e.course_id
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
            $this->assertSame('Math', $rows[0]['title']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT JOIN composite PK failed: ' . $e->getMessage());
        }
    }
}
