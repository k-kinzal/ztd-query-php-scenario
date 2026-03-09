<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests event-sourcing-style patterns: appending events to a log and querying
 * derived projections — a common pattern in CQRS applications (PostgreSQL PDO).
 * @spec SPEC-10.2.120
 */
class PostgresEventSourcingProjectionTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_es_accounts (
                id SERIAL PRIMARY KEY,
                owner_name TEXT
            )',
            'CREATE TABLE pg_es_events (
                id SERIAL PRIMARY KEY,
                aggregate_id INT,
                event_type TEXT,
                payload_amount NUMERIC(12,2),
                occurred_at TEXT,
                version INT
            )',
            'CREATE TABLE pg_es_snapshots (
                id SERIAL PRIMARY KEY,
                account_id INT,
                balance NUMERIC(12,2),
                snapshot_version INT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_es_snapshots', 'pg_es_events', 'pg_es_accounts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Accounts (2)
        $this->pdo->exec("INSERT INTO pg_es_accounts VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pg_es_accounts VALUES (2, 'Bob')");

        // Events (8: credits and debits across both accounts)
        $this->pdo->exec("INSERT INTO pg_es_events VALUES (1, 1, 'credit', 1000.00, '2026-03-09 09:00:00', 1)");
        $this->pdo->exec("INSERT INTO pg_es_events VALUES (2, 1, 'debit',   200.00, '2026-03-09 09:15:00', 2)");
        $this->pdo->exec("INSERT INTO pg_es_events VALUES (3, 1, 'credit',  500.00, '2026-03-09 09:30:00', 3)");
        $this->pdo->exec("INSERT INTO pg_es_events VALUES (4, 1, 'debit',   100.00, '2026-03-09 10:00:00', 4)");
        $this->pdo->exec("INSERT INTO pg_es_events VALUES (5, 2, 'credit', 2000.00, '2026-03-09 09:00:00', 1)");
        $this->pdo->exec("INSERT INTO pg_es_events VALUES (6, 2, 'debit',   750.00, '2026-03-09 09:30:00', 2)");
        $this->pdo->exec("INSERT INTO pg_es_events VALUES (7, 2, 'credit',  300.00, '2026-03-09 10:00:00', 3)");
        $this->pdo->exec("INSERT INTO pg_es_events VALUES (8, 2, 'debit',   100.00, '2026-03-09 10:30:00', 4)");

        // Snapshots (2: balance after version 2 for each account)
        $this->pdo->exec("INSERT INTO pg_es_snapshots VALUES (1, 1, 800.00, 2)");
        $this->pdo->exec("INSERT INTO pg_es_snapshots VALUES (2, 2, 1250.00, 2)");
    }

    /**
     * Current balance from events: SUM credits minus debits, joined with accounts.
     */
    public function testCurrentBalanceFromEvents(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.id, a.owner_name,
                    SUM(CASE WHEN e.event_type = 'credit' THEN e.payload_amount ELSE -e.payload_amount END) AS balance
             FROM pg_es_accounts a
             JOIN pg_es_events e ON e.aggregate_id = a.id
             GROUP BY a.id, a.owner_name
             ORDER BY a.id"
        );

        $this->assertCount(2, $rows);

        // Alice: 1000 - 200 + 500 - 100 = 1200
        $this->assertSame('Alice', $rows[0]['owner_name']);
        $this->assertEqualsWithDelta(1200.0, (float) $rows[0]['balance'], 0.01);

        // Bob: 2000 - 750 + 300 - 100 = 1450
        $this->assertSame('Bob', $rows[1]['owner_name']);
        $this->assertEqualsWithDelta(1450.0, (float) $rows[1]['balance'], 0.01);
    }

    /**
     * Event count by type: GROUP BY aggregate_id, event_type with COUNT.
     */
    public function testEventCountByType(): void
    {
        $rows = $this->ztdQuery(
            "SELECT aggregate_id, event_type, COUNT(*) AS cnt
             FROM pg_es_events
             GROUP BY aggregate_id, event_type
             ORDER BY aggregate_id, event_type"
        );

        $this->assertCount(4, $rows);

        // Alice: credit=2, debit=2
        $this->assertEquals(1, (int) $rows[0]['aggregate_id']);
        $this->assertSame('credit', $rows[0]['event_type']);
        $this->assertEquals(2, (int) $rows[0]['cnt']);
        $this->assertEquals(1, (int) $rows[1]['aggregate_id']);
        $this->assertSame('debit', $rows[1]['event_type']);
        $this->assertEquals(2, (int) $rows[1]['cnt']);

        // Bob: credit=2, debit=2
        $this->assertEquals(2, (int) $rows[2]['aggregate_id']);
        $this->assertSame('credit', $rows[2]['event_type']);
        $this->assertEquals(2, (int) $rows[2]['cnt']);
        $this->assertEquals(2, (int) $rows[3]['aggregate_id']);
        $this->assertSame('debit', $rows[3]['event_type']);
        $this->assertEquals(2, (int) $rows[3]['cnt']);
    }

    /**
     * Latest event per account: JOIN with MAX(version) to get the most recent event.
     */
    public function testLatestEventPerAccount(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.aggregate_id, e.event_type, e.payload_amount, e.version
             FROM pg_es_events e
             JOIN (
                 SELECT aggregate_id, MAX(version) AS max_ver
                 FROM pg_es_events
                 GROUP BY aggregate_id
             ) latest ON latest.aggregate_id = e.aggregate_id AND latest.max_ver = e.version
             ORDER BY e.aggregate_id"
        );

        $this->assertCount(2, $rows);

        // Alice latest: version 4, debit 100
        $this->assertEquals(1, (int) $rows[0]['aggregate_id']);
        $this->assertSame('debit', $rows[0]['event_type']);
        $this->assertEqualsWithDelta(100.0, (float) $rows[0]['payload_amount'], 0.01);
        $this->assertEquals(4, (int) $rows[0]['version']);

        // Bob latest: version 4, debit 100
        $this->assertEquals(2, (int) $rows[1]['aggregate_id']);
        $this->assertSame('debit', $rows[1]['event_type']);
        $this->assertEqualsWithDelta(100.0, (float) $rows[1]['payload_amount'], 0.01);
        $this->assertEquals(4, (int) $rows[1]['version']);
    }

    /**
     * Running balance with window function: cumulative SUM partitioned by account.
     */
    public function testRunningBalanceWithWindow(): void
    {
        $rows = $this->ztdQuery(
            "SELECT aggregate_id, version,
                    SUM(CASE WHEN event_type = 'credit' THEN payload_amount ELSE -payload_amount END)
                        OVER (PARTITION BY aggregate_id ORDER BY version) AS running_balance
             FROM pg_es_events
             ORDER BY aggregate_id, version"
        );

        $this->assertCount(8, $rows);

        // Alice running balance: v1=1000, v2=800, v3=1300, v4=1200
        $this->assertEqualsWithDelta(1000.0, (float) $rows[0]['running_balance'], 0.01);
        $this->assertEqualsWithDelta(800.0, (float) $rows[1]['running_balance'], 0.01);
        $this->assertEqualsWithDelta(1300.0, (float) $rows[2]['running_balance'], 0.01);
        $this->assertEqualsWithDelta(1200.0, (float) $rows[3]['running_balance'], 0.01);

        // Bob running balance: v1=2000, v2=1250, v3=1550, v4=1450
        $this->assertEqualsWithDelta(2000.0, (float) $rows[4]['running_balance'], 0.01);
        $this->assertEqualsWithDelta(1250.0, (float) $rows[5]['running_balance'], 0.01);
        $this->assertEqualsWithDelta(1550.0, (float) $rows[6]['running_balance'], 0.01);
        $this->assertEqualsWithDelta(1450.0, (float) $rows[7]['running_balance'], 0.01);
    }

    /**
     * Snapshot plus delta: combine snapshot balance with events after snapshot version.
     */
    public function testSnapshotPlusDelta(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.account_id,
                    s.balance + COALESCE(SUM(
                        CASE WHEN e.event_type = 'credit' THEN e.payload_amount ELSE -e.payload_amount END
                    ), 0) AS current_balance
             FROM pg_es_snapshots s
             LEFT JOIN pg_es_events e ON e.aggregate_id = s.account_id AND e.version > s.snapshot_version
             GROUP BY s.account_id, s.balance
             ORDER BY s.account_id"
        );

        $this->assertCount(2, $rows);

        // Alice: snapshot 800 + (credit 500 - debit 100) = 1200
        $this->assertEquals(1, (int) $rows[0]['account_id']);
        $this->assertEqualsWithDelta(1200.0, (float) $rows[0]['current_balance'], 0.01);

        // Bob: snapshot 1250 + (credit 300 - debit 100) = 1450
        $this->assertEquals(2, (int) $rows[1]['account_id']);
        $this->assertEqualsWithDelta(1450.0, (float) $rows[1]['current_balance'], 0.01);
    }

    /**
     * Events between versions: filter for replay range.
     */
    public function testEventsBetweenVersions(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, aggregate_id, event_type, version
             FROM pg_es_events
             WHERE aggregate_id = 1 AND version BETWEEN 2 AND 3
             ORDER BY version"
        );

        $this->assertCount(2, $rows);

        $this->assertEquals(2, (int) $rows[0]['version']);
        $this->assertSame('debit', $rows[0]['event_type']);

        $this->assertEquals(3, (int) $rows[1]['version']);
        $this->assertSame('credit', $rows[1]['event_type']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_es_events VALUES (9, 1, 'credit', 999.00, '2026-03-09 11:00:00', 5)");
        $this->pdo->exec("UPDATE pg_es_accounts SET owner_name = 'Alice Updated' WHERE id = 1");

        // ZTD sees changes
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_es_events");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT owner_name FROM pg_es_accounts WHERE id = 1");
        $this->assertSame('Alice Updated', $rows[0]['owner_name']);

        // Physical tables untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_es_events")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
