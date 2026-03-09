<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests row value constructors with subqueries through MySQL-PDO CTE shadow store.
 *
 * Control test for PostgresRowValueInSubqueryTest. Verifies whether
 * UPDATE/DELETE with row value constructors in WHERE work on MySQL.
 */
class MysqlRowValueInSubqueryTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_rvi_schedules (
                teacher_id INT NOT NULL,
                room_id INT NOT NULL,
                time_slot VARCHAR(20) NOT NULL,
                subject VARCHAR(50) NOT NULL,
                PRIMARY KEY (teacher_id, room_id, time_slot)
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_rvi_conflicts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                teacher_id INT NOT NULL,
                room_id INT NOT NULL,
                time_slot VARCHAR(20) NOT NULL,
                reason VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_rvi_conflicts', 'mp_rvi_schedules'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_rvi_schedules VALUES (1, 101, '09:00', 'Math')");
        $this->pdo->exec("INSERT INTO mp_rvi_schedules VALUES (1, 102, '10:00', 'Physics')");
        $this->pdo->exec("INSERT INTO mp_rvi_schedules VALUES (2, 101, '09:00', 'English')");
        $this->pdo->exec("INSERT INTO mp_rvi_schedules VALUES (2, 103, '11:00', 'History')");
        $this->pdo->exec("INSERT INTO mp_rvi_schedules VALUES (3, 102, '10:00', 'Chemistry')");

        $this->pdo->exec("INSERT INTO mp_rvi_conflicts (teacher_id, room_id, time_slot, reason) VALUES (1, 101, '09:00', 'Room maintenance')");
        $this->pdo->exec("INSERT INTO mp_rvi_conflicts (teacher_id, room_id, time_slot, reason) VALUES (2, 103, '11:00', 'Teacher absent')");
    }

    /**
     * SELECT with row value IN subquery.
     */
    public function testRowValueInSubquery(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.subject
             FROM mp_rvi_schedules s
             WHERE (s.teacher_id, s.room_id, s.time_slot) IN (
                 SELECT c.teacher_id, c.room_id, c.time_slot FROM mp_rvi_conflicts c
             )
             ORDER BY s.subject"
        );

        $this->assertCount(2, $rows);
        $this->assertEquals('History', $rows[0]['subject']);
        $this->assertEquals('Math', $rows[1]['subject']);
    }

    /**
     * UPDATE with row value IN subquery.
     */
    public function testUpdateWithRowValueInSubquery(): void
    {
        $this->pdo->exec(
            "UPDATE mp_rvi_schedules SET subject = CONCAT(subject, ' (CANCELLED)')
             WHERE (teacher_id, room_id, time_slot) IN (
                 SELECT teacher_id, room_id, time_slot FROM mp_rvi_conflicts
             )"
        );

        $rows = $this->ztdQuery("SELECT subject FROM mp_rvi_schedules WHERE subject LIKE '%(CANCELLED)%' ORDER BY subject");
        $this->assertCount(2, $rows);
    }

    /**
     * DELETE with row value IN subquery.
     */
    public function testDeleteWithRowValueInSubquery(): void
    {
        $this->pdo->exec(
            "DELETE FROM mp_rvi_schedules
             WHERE (teacher_id, room_id, time_slot) IN (
                 SELECT teacher_id, room_id, time_slot FROM mp_rvi_conflicts
             )"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_rvi_schedules");
        $this->assertEquals(3, (int) $rows[0]['cnt']);
    }

    /**
     * Prepared statement with row value IN subquery.
     */
    public function testPreparedRowValueInSubquery(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT s.subject
             FROM mp_rvi_schedules s
             WHERE (s.teacher_id, s.room_id, s.time_slot) IN (
                 SELECT c.teacher_id, c.room_id, c.time_slot
                 FROM mp_rvi_conflicts c
                 WHERE c.reason = ?
             )",
            ['Room maintenance']
        );

        $this->assertCount(1, $rows);
        $this->assertEquals('Math', $rows[0]['subject']);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM mp_rvi_schedules')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
