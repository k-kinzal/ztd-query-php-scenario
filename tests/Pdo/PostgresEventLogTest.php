<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests audit trail / event log patterns through ZTD shadow store.
 * Simulates append-only event tables with time-range queries and filtering.
 * @spec SPEC-3.1, SPEC-3.2, SPEC-4.1
 */
class PostgresEventLogTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_el_events (
                id INTEGER PRIMARY KEY,
                event_type TEXT,
                entity_type TEXT,
                entity_id INTEGER,
                user_id INTEGER,
                payload TEXT,
                created_at TIMESTAMP
            )',
            'CREATE TABLE pg_el_users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                role TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_el_events', 'pg_el_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_el_users VALUES (1, 'Alice', 'admin')");
        $this->pdo->exec("INSERT INTO pg_el_users VALUES (2, 'Bob', 'editor')");
        $this->pdo->exec("INSERT INTO pg_el_users VALUES (3, 'Charlie', 'viewer')");

        // Event log entries
        $this->pdo->exec("INSERT INTO pg_el_events VALUES (1, 'create', 'article', 100, 1, '{\"title\":\"Hello\"}', '2024-03-01 09:00:00')");
        $this->pdo->exec("INSERT INTO pg_el_events VALUES (2, 'update', 'article', 100, 2, '{\"field\":\"body\"}', '2024-03-01 10:30:00')");
        $this->pdo->exec("INSERT INTO pg_el_events VALUES (3, 'create', 'article', 101, 1, '{\"title\":\"World\"}', '2024-03-02 08:15:00')");
        $this->pdo->exec("INSERT INTO pg_el_events VALUES (4, 'delete', 'comment', 200, 2, '{\"reason\":\"spam\"}', '2024-03-02 14:00:00')");
        $this->pdo->exec("INSERT INTO pg_el_events VALUES (5, 'update', 'article', 100, 1, '{\"field\":\"title\"}', '2024-03-03 11:00:00')");
        $this->pdo->exec("INSERT INTO pg_el_events VALUES (6, 'create', 'comment', 201, 3, '{\"body\":\"Nice!\"}', '2024-03-03 15:30:00')");
    }

    /**
     * Query events by time range (BETWEEN).
     */
    public function testTimeRangeQuery(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, event_type, created_at FROM pg_el_events
             WHERE created_at BETWEEN '2024-03-01 00:00:00' AND '2024-03-01 23:59:59'
             ORDER BY created_at"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('create', $rows[0]['event_type']);
        $this->assertSame('update', $rows[1]['event_type']);
    }

    /**
     * Prepared time-range query with parameters.
     */
    public function testPreparedTimeRange(): void
    {
        $stmt = $this->pdo->prepare(
            'SELECT COUNT(*) AS cnt FROM pg_el_events
             WHERE created_at >= ? AND created_at < ?'
        );

        $stmt->execute(['2024-03-02 00:00:00', '2024-03-03 00:00:00']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(2, (int) $rows[0]['cnt']); // Events 3 and 4

        // Reuse with different range
        $stmt->execute(['2024-03-03 00:00:00', '2024-03-04 00:00:00']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(2, (int) $rows[0]['cnt']); // Events 5 and 6
    }

    /**
     * Filter events by type and entity.
     */
    public function testFilterByTypeAndEntity(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, created_at FROM pg_el_events
             WHERE event_type = 'update' AND entity_type = 'article' AND entity_id = 100
             ORDER BY created_at"
        );

        $this->assertCount(2, $rows);
        $this->assertEquals(2, (int) $rows[0]['id']);
        $this->assertEquals(5, (int) $rows[1]['id']);
    }

    /**
     * Append new events and verify they appear in time-ordered query.
     */
    public function testAppendAndQuery(): void
    {
        // Append new events
        $this->pdo->exec("INSERT INTO pg_el_events VALUES (7, 'update', 'article', 101, 2, '{\"field\":\"body\"}', '2024-03-04 09:00:00')");
        $this->pdo->exec("INSERT INTO pg_el_events VALUES (8, 'delete', 'article', 100, 1, '{\"reason\":\"archived\"}', '2024-03-04 10:00:00')");

        $rows = $this->ztdQuery(
            "SELECT id, event_type, entity_id FROM pg_el_events
             WHERE created_at >= '2024-03-04 00:00:00'
             ORDER BY created_at"
        );

        $this->assertCount(2, $rows);
        $this->assertEquals(7, (int) $rows[0]['id']);
        $this->assertSame('update', $rows[0]['event_type']);
        $this->assertEquals(8, (int) $rows[1]['id']);
        $this->assertSame('delete', $rows[1]['event_type']);
    }

    /**
     * Event count by type (activity summary).
     */
    public function testEventCountByType(): void
    {
        $rows = $this->ztdQuery(
            "SELECT event_type, COUNT(*) AS cnt
             FROM pg_el_events
             GROUP BY event_type
             ORDER BY cnt DESC, event_type"
        );

        $this->assertCount(3, $rows);
        $creates = array_values(array_filter($rows, fn($r) => $r['event_type'] === 'create'));
        $this->assertEquals(3, (int) $creates[0]['cnt']);
        $updates = array_values(array_filter($rows, fn($r) => $r['event_type'] === 'update'));
        $this->assertEquals(2, (int) $updates[0]['cnt']);
    }

    /**
     * Event log with user JOIN (who did what).
     */
    public function testEventLogWithUserJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.event_type, e.entity_type, e.entity_id,
                    u.name AS user_name, u.role AS user_role
             FROM pg_el_events e
             JOIN pg_el_users u ON e.user_id = u.id
             WHERE e.entity_type = 'article'
             ORDER BY e.created_at"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Alice', $rows[0]['user_name']);
        $this->assertSame('create', $rows[0]['event_type']);
        $this->assertSame('Bob', $rows[1]['user_name']);
        $this->assertSame('update', $rows[1]['event_type']);
    }

    /**
     * Activity per user (who is most active).
     */
    public function testActivityPerUser(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name, COUNT(e.id) AS event_count
             FROM pg_el_users u
             LEFT JOIN pg_el_events e ON u.id = e.user_id
             GROUP BY u.id, u.name
             ORDER BY event_count DESC"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(3, (int) $rows[0]['event_count']);
    }

    /**
     * Entity history: all events for a specific entity in chronological order.
     */
    public function testEntityHistory(): void
    {
        $stmt = $this->pdo->prepare(
            'SELECT event_type, created_at FROM pg_el_events
             WHERE entity_type = ? AND entity_id = ?
             ORDER BY created_at'
        );

        $stmt->execute(['article', 100]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('create', $rows[0]['event_type']);
        $this->assertSame('update', $rows[1]['event_type']);
        $this->assertSame('update', $rows[2]['event_type']);
    }

    /**
     * Multiple COUNT DISTINCT in single query.
     */
    public function testMultipleCountDistinct(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COUNT(DISTINCT user_id) AS unique_users,
                    COUNT(DISTINCT entity_type) AS unique_entity_types,
                    COUNT(DISTINCT event_type) AS unique_event_types,
                    COUNT(*) AS total_events
             FROM pg_el_events"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(3, (int) $rows[0]['unique_users']);
        $this->assertEquals(2, (int) $rows[0]['unique_entity_types']);
        $this->assertEquals(3, (int) $rows[0]['unique_event_types']);
        $this->assertEquals(6, (int) $rows[0]['total_events']);
    }

    /**
     * Multiple COUNT DISTINCT with GROUP BY.
     */
    public function testMultipleCountDistinctGrouped(): void
    {
        $rows = $this->ztdQuery(
            "SELECT entity_type,
                    COUNT(DISTINCT user_id) AS unique_users,
                    COUNT(DISTINCT event_type) AS unique_actions,
                    COUNT(*) AS total
             FROM pg_el_events
             GROUP BY entity_type
             ORDER BY entity_type"
        );

        $this->assertCount(2, $rows);
        // article: users 1,2 (Alice, Bob), actions: create, update
        $this->assertSame('article', $rows[0]['entity_type']);
        $this->assertEquals(2, (int) $rows[0]['unique_users']);
        $this->assertEquals(2, (int) $rows[0]['unique_actions']);
        $this->assertEquals(4, (int) $rows[0]['total']);
        // comment: users 2,3 (Bob, Charlie), actions: delete, create
        $this->assertSame('comment', $rows[1]['entity_type']);
        $this->assertEquals(2, (int) $rows[1]['unique_users']);
        $this->assertEquals(2, (int) $rows[1]['unique_actions']);
        $this->assertEquals(2, (int) $rows[1]['total']);
    }

    /**
     * Physical isolation: events added in ZTD do not reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // All events exist in ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_el_events");
        $this->assertEquals(6, (int) $rows[0]['cnt']);

        // Nothing in physical table
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_el_events');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
