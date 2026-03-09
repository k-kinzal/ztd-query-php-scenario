<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests a waitlist queue management workflow through ZTD shadow store (MySQL PDO).
 * Covers priority ordering, promotion, capacity limits, position calculations
 * via window functions, and physical isolation.
 * @spec SPEC-10.2.68
 */
class MysqlWaitlistQueueTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_wq_events (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                capacity INT,
                enrolled_count INT
            )',
            'CREATE TABLE mp_wq_waitlist (
                id INT PRIMARY KEY,
                event_id INT,
                person_name VARCHAR(255),
                priority INT,
                status VARCHAR(20),
                joined_at DATETIME
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_wq_waitlist', 'mp_wq_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 2 events
        $this->pdo->exec("INSERT INTO mp_wq_events VALUES (1, 'Workshop A', 3, 2)");
        $this->pdo->exec("INSERT INTO mp_wq_events VALUES (2, 'Workshop B', 5, 0)");

        // 6 waitlist entries for event 1
        $this->pdo->exec("INSERT INTO mp_wq_waitlist VALUES (1, 1, 'Alice',   2, 'waiting',   '2026-03-10 10:00:00')");
        $this->pdo->exec("INSERT INTO mp_wq_waitlist VALUES (2, 1, 'Bob',     1, 'waiting',   '2026-03-10 10:30:00')");
        $this->pdo->exec("INSERT INTO mp_wq_waitlist VALUES (3, 1, 'Charlie', 2, 'waiting',   '2026-03-10 11:00:00')");
        $this->pdo->exec("INSERT INTO mp_wq_waitlist VALUES (4, 1, 'Diana',   3, 'waiting',   '2026-03-10 09:00:00')");
        $this->pdo->exec("INSERT INTO mp_wq_waitlist VALUES (5, 1, 'Eve',     1, 'waiting',   '2026-03-10 11:30:00')");
        $this->pdo->exec("INSERT INTO mp_wq_waitlist VALUES (6, 1, 'Frank',   2, 'cancelled', '2026-03-10 10:15:00')");
    }

    /**
     * ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY priority, joined_at)
     * assigns queue positions among waiting entries.
     */
    public function testWaitlistPositionByPriority(): void
    {
        $rows = $this->ztdQuery(
            "SELECT w.person_name, w.priority, w.joined_at,
                    ROW_NUMBER() OVER (PARTITION BY w.event_id ORDER BY w.priority, w.joined_at) AS position
             FROM mp_wq_waitlist w
             WHERE w.event_id = 1 AND w.status = 'waiting'
             ORDER BY position"
        );

        // Expected order: Bob (1, 10:30), Eve (1, 11:30), Alice (2, 10:00), Charlie (2, 11:00), Diana (3, 09:00)
        $this->assertCount(5, $rows);
        $this->assertSame('Bob',     $rows[0]['person_name']);
        $this->assertEquals(1, (int) $rows[0]['position']);
        $this->assertSame('Eve',     $rows[1]['person_name']);
        $this->assertEquals(2, (int) $rows[1]['position']);
        $this->assertSame('Alice',   $rows[2]['person_name']);
        $this->assertEquals(3, (int) $rows[2]['position']);
        $this->assertSame('Charlie', $rows[3]['person_name']);
        $this->assertEquals(4, (int) $rows[3]['position']);
        $this->assertSame('Diana',   $rows[4]['person_name']);
        $this->assertEquals(5, (int) $rows[4]['position']);
    }

    /**
     * INSERT a new entry and verify its position in the queue via window function.
     */
    public function testJoinWaitlist(): void
    {
        // Grace joins with normal priority
        $this->pdo->exec("INSERT INTO mp_wq_waitlist VALUES (7, 1, 'Grace', 2, 'waiting', '2026-03-10 12:00:00')");

        $rows = $this->ztdQuery(
            "SELECT w.person_name,
                    ROW_NUMBER() OVER (PARTITION BY w.event_id ORDER BY w.priority, w.joined_at) AS position
             FROM mp_wq_waitlist w
             WHERE w.event_id = 1 AND w.status = 'waiting'
             ORDER BY position"
        );

        // 6 waiting entries now (5 original waiting + Grace)
        $this->assertCount(6, $rows);

        // Grace (priority 2, 12:00) should be after Charlie (priority 2, 11:00) => position 5
        $graceRow = array_values(array_filter($rows, fn($r) => $r['person_name'] === 'Grace'));
        $this->assertCount(1, $graceRow);
        $this->assertEquals(5, (int) $graceRow[0]['position']);
    }

    /**
     * Promote the top waiting entry: UPDATE status to 'promoted', increment enrolled_count.
     */
    public function testPromoteFromWaitlist(): void
    {
        // Find the top waiting entry (Bob, position 1)
        $top = $this->ztdQuery(
            "SELECT w.id, w.person_name
             FROM mp_wq_waitlist w
             WHERE w.event_id = 1 AND w.status = 'waiting'
             ORDER BY w.priority, w.joined_at
             LIMIT 1"
        );
        $this->assertSame('Bob', $top[0]['person_name']);

        // Promote Bob
        $affected = $this->pdo->exec("UPDATE mp_wq_waitlist SET status = 'promoted' WHERE id = " . (int) $top[0]['id']);
        $this->assertSame(1, $affected);

        // Increment enrolled count
        $affected = $this->pdo->exec("UPDATE mp_wq_events SET enrolled_count = enrolled_count + 1 WHERE id = 1");
        $this->assertSame(1, $affected);

        // Verify enrolled count is now 3 (was 2)
        $event = $this->ztdQuery("SELECT enrolled_count, capacity FROM mp_wq_events WHERE id = 1");
        $this->assertEquals(3, (int) $event[0]['enrolled_count']);
        $this->assertEquals(3, (int) $event[0]['capacity']);

        // Verify Bob is promoted
        $rows = $this->ztdQuery("SELECT status FROM mp_wq_waitlist WHERE id = 2");
        $this->assertSame('promoted', $rows[0]['status']);

        // Remaining waiting list should not include Bob
        $waiting = $this->ztdQuery(
            "SELECT person_name FROM mp_wq_waitlist WHERE event_id = 1 AND status = 'waiting' ORDER BY priority, joined_at"
        );
        $this->assertCount(4, $waiting);
        $this->assertSame('Eve', $waiting[0]['person_name']);
    }

    /**
     * Cancel an entry and verify remaining positions shift correctly.
     */
    public function testCancelAndShiftPositions(): void
    {
        // Cancel Alice (id=1, was position 3)
        $affected = $this->pdo->exec("UPDATE mp_wq_waitlist SET status = 'cancelled' WHERE id = 1");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery(
            "SELECT w.person_name,
                    ROW_NUMBER() OVER (PARTITION BY w.event_id ORDER BY w.priority, w.joined_at) AS position
             FROM mp_wq_waitlist w
             WHERE w.event_id = 1 AND w.status = 'waiting'
             ORDER BY position"
        );

        // 4 waiting entries remain: Bob(1), Eve(1), Charlie(2), Diana(3)
        $this->assertCount(4, $rows);
        $this->assertSame('Bob',     $rows[0]['person_name']);
        $this->assertEquals(1, (int) $rows[0]['position']);
        $this->assertSame('Eve',     $rows[1]['person_name']);
        $this->assertEquals(2, (int) $rows[1]['position']);
        $this->assertSame('Charlie', $rows[2]['person_name']);
        $this->assertEquals(3, (int) $rows[2]['position']);
        $this->assertSame('Diana',   $rows[3]['person_name']);
        $this->assertEquals(4, (int) $rows[3]['position']);
    }

    /**
     * No promotion allowed when enrolled_count equals capacity.
     */
    public function testCapacityGuard(): void
    {
        // Fill event 1 to capacity (capacity=3, enrolled=2 -> set to 3)
        $this->pdo->exec("UPDATE mp_wq_events SET enrolled_count = 3 WHERE id = 1");

        // Verify event is at capacity
        $event = $this->ztdQuery(
            "SELECT e.capacity, e.enrolled_count,
                    (e.capacity - e.enrolled_count) AS spots_available
             FROM mp_wq_events e
             WHERE e.id = 1"
        );
        $this->assertEquals(0, (int) $event[0]['spots_available']);

        // Count waiting entries that could be promoted
        $waiting = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM mp_wq_waitlist WHERE event_id = 1 AND status = 'waiting'"
        );
        $this->assertEquals(5, (int) $waiting[0]['cnt']);

        // Verify no spots: enrolled_count >= capacity
        $promotable = $this->ztdQuery(
            "SELECT w.person_name
             FROM mp_wq_waitlist w
             JOIN mp_wq_events e ON e.id = w.event_id
             WHERE w.event_id = 1
               AND w.status = 'waiting'
               AND e.enrolled_count < e.capacity
             ORDER BY w.priority, w.joined_at
             LIMIT 1"
        );
        $this->assertCount(0, $promotable);
    }

    /**
     * Verify full priority queue ordering: priority ASC, then joined_at ASC.
     */
    public function testPriorityQueueOrdering(): void
    {
        $rows = $this->ztdQuery(
            "SELECT w.person_name, w.priority, w.joined_at
             FROM mp_wq_waitlist w
             WHERE w.event_id = 1 AND w.status = 'waiting'
             ORDER BY w.priority ASC, w.joined_at ASC"
        );

        $this->assertCount(5, $rows);

        // Priority 1: Bob (10:30), Eve (11:30)
        $this->assertSame('Bob', $rows[0]['person_name']);
        $this->assertEquals(1, (int) $rows[0]['priority']);
        $this->assertSame('Eve', $rows[1]['person_name']);
        $this->assertEquals(1, (int) $rows[1]['priority']);

        // Priority 2: Alice (10:00), Charlie (11:00)
        $this->assertSame('Alice', $rows[2]['person_name']);
        $this->assertEquals(2, (int) $rows[2]['priority']);
        $this->assertSame('Charlie', $rows[3]['person_name']);
        $this->assertEquals(2, (int) $rows[3]['priority']);

        // Priority 3: Diana (09:00)
        $this->assertSame('Diana', $rows[4]['person_name']);
        $this->assertEquals(3, (int) $rows[4]['priority']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Mutate through ZTD
        $this->pdo->exec("INSERT INTO mp_wq_waitlist VALUES (7, 1, 'Grace', 2, 'waiting', '2026-03-10 12:00:00')");
        $this->pdo->exec("UPDATE mp_wq_waitlist SET status = 'promoted' WHERE id = 2");
        $this->pdo->exec("UPDATE mp_wq_events SET enrolled_count = enrolled_count + 1 WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_wq_waitlist");
        $this->assertSame(7, (int) $rows[0]['cnt']);

        $event = $this->ztdQuery("SELECT enrolled_count FROM mp_wq_events WHERE id = 1");
        $this->assertEquals(3, (int) $event[0]['enrolled_count']);

        // Physical tables untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM mp_wq_waitlist')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);

        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM mp_wq_events')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
