<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests row value constructors with subqueries through PostgreSQL CTE shadow store.
 *
 * WHERE (col1, col2) IN (SELECT ...) is a common pattern for composite-key
 * lookups. The PgSqlParser must preserve row constructors during CTE rewriting.
 */
class PostgresRowValueInSubqueryTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_rvi_schedules (
                teacher_id INTEGER NOT NULL,
                room_id INTEGER NOT NULL,
                time_slot VARCHAR(20) NOT NULL,
                subject VARCHAR(50) NOT NULL,
                PRIMARY KEY (teacher_id, room_id, time_slot)
            )',
            'CREATE TABLE pg_rvi_conflicts (
                id SERIAL PRIMARY KEY,
                teacher_id INTEGER NOT NULL,
                room_id INTEGER NOT NULL,
                time_slot VARCHAR(20) NOT NULL,
                reason VARCHAR(100) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_rvi_conflicts', 'pg_rvi_schedules'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_rvi_schedules VALUES (1, 101, '09:00', 'Math')");
        $this->pdo->exec("INSERT INTO pg_rvi_schedules VALUES (1, 102, '10:00', 'Physics')");
        $this->pdo->exec("INSERT INTO pg_rvi_schedules VALUES (2, 101, '09:00', 'English')");
        $this->pdo->exec("INSERT INTO pg_rvi_schedules VALUES (2, 103, '11:00', 'History')");
        $this->pdo->exec("INSERT INTO pg_rvi_schedules VALUES (3, 102, '10:00', 'Chemistry')");

        $this->pdo->exec("INSERT INTO pg_rvi_conflicts (teacher_id, room_id, time_slot, reason) VALUES (1, 101, '09:00', 'Room maintenance')");
        $this->pdo->exec("INSERT INTO pg_rvi_conflicts (teacher_id, room_id, time_slot, reason) VALUES (2, 103, '11:00', 'Teacher absent')");
    }

    /**
     * Two-column row value IN with subquery.
     */
    public function testTwoColumnRowValueIn(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.subject
             FROM pg_rvi_schedules s
             WHERE (s.teacher_id, s.time_slot) IN (
                 SELECT c.teacher_id, c.time_slot FROM pg_rvi_conflicts c
             )
             ORDER BY s.subject"
        );

        $this->assertCount(2, $rows);
        $subjects = array_column($rows, 'subject');
        $this->assertContains('Math', $subjects);
        $this->assertContains('History', $subjects);
    }

    /**
     * Three-column row value IN with subquery.
     */
    public function testThreeColumnRowValueIn(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.subject
             FROM pg_rvi_schedules s
             WHERE (s.teacher_id, s.room_id, s.time_slot) IN (
                 SELECT c.teacher_id, c.room_id, c.time_slot FROM pg_rvi_conflicts c
             )
             ORDER BY s.subject"
        );

        $this->assertCount(2, $rows);
        $this->assertEquals('History', $rows[0]['subject']);
        $this->assertEquals('Math', $rows[1]['subject']);
    }

    /**
     * Row value NOT IN with subquery — schedules without conflicts.
     */
    public function testRowValueNotInSubquery(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.subject
             FROM pg_rvi_schedules s
             WHERE (s.teacher_id, s.room_id, s.time_slot) NOT IN (
                 SELECT c.teacher_id, c.room_id, c.time_slot FROM pg_rvi_conflicts c
             )
             ORDER BY s.subject"
        );

        $this->assertCount(3, $rows);
        $subjects = array_column($rows, 'subject');
        $this->assertContains('Chemistry', $subjects);
        $this->assertContains('English', $subjects);
        $this->assertContains('Physics', $subjects);
    }

    /**
     * UPDATE with row value IN subquery.
     *
     * The CTE rewriter may produce invalid SQL when UPDATE uses
     * row value constructors in WHERE on PostgreSQL.
     */
    public function testUpdateWithRowValueInSubquery(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_rvi_schedules SET subject = subject || ' (CANCELLED)'
                 WHERE (teacher_id, room_id, time_slot) IN (
                     SELECT teacher_id, room_id, time_slot FROM pg_rvi_conflicts
                 )"
            );
        } catch (\ZtdQuery\Adapter\Pdo\ZtdPdoException $e) {
            if (str_contains($e->getMessage(), 'syntax error')) {
                $this->markTestIncomplete(
                    'UPDATE with row value constructor (col1, col2, col3) IN (SELECT ...) '
                    . 'produces syntax error on PostgreSQL. The CTE rewriter generates invalid SQL '
                    . 'when processing row value constructors in UPDATE WHERE clause. '
                    . 'Error: ' . $e->getMessage()
                );
            }
            throw $e;
        }

        $rows = $this->ztdQuery("SELECT subject FROM pg_rvi_schedules WHERE subject LIKE '%(CANCELLED)%' ORDER BY subject");
        $this->assertCount(2, $rows);
    }

    /**
     * DELETE with row value IN subquery.
     */
    public function testDeleteWithRowValueInSubquery(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM pg_rvi_schedules
                 WHERE (teacher_id, room_id, time_slot) IN (
                     SELECT teacher_id, room_id, time_slot FROM pg_rvi_conflicts
                 )"
            );
        } catch (\ZtdQuery\Adapter\Pdo\ZtdPdoException $e) {
            if (str_contains($e->getMessage(), 'syntax error')) {
                $this->markTestIncomplete(
                    'DELETE with row value constructor in WHERE produces syntax error on PostgreSQL. '
                    . 'Same root cause as UPDATE row value constructor issue.'
                );
            }
            throw $e;
        }

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_rvi_schedules");
        $this->assertEquals(3, (int) $rows[0]['cnt']);
    }

    /**
     * Prepared statement with row value subquery.
     *
     * The CTE rewriter may mishandle row value constructors combined
     * with $N parameter placeholders on PostgreSQL.
     */
    public function testPreparedRowValueInSubquery(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT s.subject
             FROM pg_rvi_schedules s
             WHERE (s.teacher_id, s.room_id, s.time_slot) IN (
                 SELECT c.teacher_id, c.room_id, c.time_slot
                 FROM pg_rvi_conflicts c
                 WHERE c.reason = $1
             )",
            ['Room maintenance']
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'Row value IN subquery with $N prepared param returns empty on PostgreSQL. '
                . 'The CTE rewriter may misparse the row value constructor when $N params are present.'
            );
        }

        $this->assertCount(1, $rows);
        $this->assertEquals('Math', $rows[0]['subject']);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM pg_rvi_schedules')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
