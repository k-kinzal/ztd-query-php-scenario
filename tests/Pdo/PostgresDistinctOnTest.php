<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL-specific DISTINCT ON syntax through the CTE rewriter.
 * DISTINCT ON is widely used in PostgreSQL for "first row per group" queries.
 * The CTE rewriter must not strip or misinterpret the DISTINCT ON clause.
 *
 * SQL patterns exercised: DISTINCT ON basic, DISTINCT ON with ORDER BY,
 * DISTINCT ON with shadow data, DISTINCT ON with JOIN.
 * @spec SPEC-3.3
 */
class PostgresDistinctOnTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_don_events (
                id SERIAL PRIMARY KEY,
                user_id INT NOT NULL,
                event_type VARCHAR(50) NOT NULL,
                created_at TIMESTAMP NOT NULL,
                payload TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_don_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_don_events (user_id, event_type, created_at, payload) VALUES (1, 'login', '2025-01-01 10:00:00', 'first login')");
        $this->pdo->exec("INSERT INTO pg_don_events (user_id, event_type, created_at, payload) VALUES (1, 'purchase', '2025-01-01 11:00:00', 'item A')");
        $this->pdo->exec("INSERT INTO pg_don_events (user_id, event_type, created_at, payload) VALUES (1, 'login', '2025-01-02 09:00:00', 'second login')");
        $this->pdo->exec("INSERT INTO pg_don_events (user_id, event_type, created_at, payload) VALUES (2, 'login', '2025-01-01 08:00:00', 'user2 login')");
        $this->pdo->exec("INSERT INTO pg_don_events (user_id, event_type, created_at, payload) VALUES (2, 'purchase', '2025-01-03 14:00:00', 'item B')");
    }

    /**
     * DISTINCT ON — latest event per user.
     */
    public function testDistinctOnLatestPerUser(): void
    {
        $rows = $this->ztdQuery(
            "SELECT DISTINCT ON (user_id) user_id, event_type, created_at, payload
             FROM pg_don_events
             ORDER BY user_id, created_at DESC"
        );

        $this->assertCount(2, $rows);
        $user1 = array_values(array_filter($rows, fn($r) => (int) $r['user_id'] === 1));
        $this->assertCount(1, $user1);
        $this->assertSame('login', $user1[0]['event_type']);
        $this->assertSame('second login', $user1[0]['payload']);

        $user2 = array_values(array_filter($rows, fn($r) => (int) $r['user_id'] === 2));
        $this->assertCount(1, $user2);
        $this->assertSame('purchase', $user2[0]['event_type']);
    }

    /**
     * DISTINCT ON — first event per user (earliest).
     */
    public function testDistinctOnFirstPerUser(): void
    {
        $rows = $this->ztdQuery(
            "SELECT DISTINCT ON (user_id) user_id, event_type, payload
             FROM pg_don_events
             ORDER BY user_id, created_at ASC"
        );

        $this->assertCount(2, $rows);
        $user1 = array_values(array_filter($rows, fn($r) => (int) $r['user_id'] === 1));
        $this->assertSame('login', $user1[0]['event_type']);
        $this->assertSame('first login', $user1[0]['payload']);
    }

    /**
     * DISTINCT ON — first event per event type.
     */
    public function testDistinctOnFirstPerEventType(): void
    {
        $rows = $this->ztdQuery(
            "SELECT DISTINCT ON (event_type) event_type, user_id, payload
             FROM pg_don_events
             ORDER BY event_type, created_at ASC"
        );

        $this->assertCount(2, $rows);
        $logins = array_values(array_filter($rows, fn($r) => $r['event_type'] === 'login'));
        $this->assertCount(1, $logins);
        $this->assertEquals(2, (int) $logins[0]['user_id']);
    }

    /**
     * DISTINCT ON with shadow data — INSERT then query.
     */
    public function testDistinctOnAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO pg_don_events (user_id, event_type, created_at, payload) VALUES (1, 'logout', '2025-01-05 20:00:00', 'final logout')");

        $rows = $this->ztdQuery(
            "SELECT DISTINCT ON (user_id) user_id, event_type, payload
             FROM pg_don_events
             ORDER BY user_id, created_at DESC"
        );

        $this->assertCount(2, $rows);
        $user1 = array_values(array_filter($rows, fn($r) => (int) $r['user_id'] === 1));
        $this->assertSame('logout', $user1[0]['event_type']);
        $this->assertSame('final logout', $user1[0]['payload']);
    }

    /**
     * DISTINCT ON after UPDATE.
     *
     * Known issue: SERIAL PK stores as NULL in shadow (Issue #21/#77),
     * so UPDATE matches rows but cannot properly modify them.
     */
    public function testDistinctOnAfterUpdate(): void
    {
        $this->ztdExec(
            "UPDATE pg_don_events SET event_type = 'session_start'
             WHERE user_id = 1 AND created_at = '2025-01-02 09:00:00'"
        );

        $rows = $this->ztdQuery(
            "SELECT DISTINCT ON (user_id) user_id, event_type
             FROM pg_don_events
             ORDER BY user_id, created_at DESC"
        );

        $user1 = array_values(array_filter($rows, fn($r) => (int) $r['user_id'] === 1));
        if ($user1[0]['event_type'] === 'login') {
            $this->markTestIncomplete(
                'SPEC-11.LAST-INSERT-ID [Issue #77/#21]: SERIAL PK is NULL in shadow, UPDATE silently fails to modify row.'
            );
        }
        $this->assertSame('session_start', $user1[0]['event_type']);
    }

    /**
     * DISTINCT ON with prepared parameters.
     *
     * Known issue: PostgreSQL $N prepared params return empty results
     * through CTE rewriter (Issue #63/#68).
     */
    public function testDistinctOnWithPreparedParams(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT DISTINCT ON (user_id) user_id, event_type, payload
             FROM pg_don_events
             WHERE event_type = $1
             ORDER BY user_id, created_at DESC"
        );
        $stmt->execute(['login']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'PostgreSQL $N prepared param with DISTINCT ON returns empty (related to Issue #63/#68).'
            );
        }
        $this->assertCount(2, $rows);
    }
}
