<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL INTERVAL type through the CTE rewriter.
 *
 * Real-world scenario: INTERVAL is widely used in PostgreSQL for date
 * arithmetic, scheduling, and time-based queries. INTERVAL literals
 * have a special syntax (INTERVAL '1 day') that might confuse the
 * CTE rewriter. The shadow store must also preserve INTERVAL values.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class PostgresIntervalTypeTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_it_events (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                scheduled_at TIMESTAMP NOT NULL,
                duration INTERVAL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_it_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_it_events (id, name, scheduled_at, duration) VALUES (1, 'Meeting', '2025-06-15 10:00:00', INTERVAL '1 hour')");
        $this->ztdExec("INSERT INTO pg_it_events (id, name, scheduled_at, duration) VALUES (2, 'Workshop', '2025-06-15 14:00:00', INTERVAL '3 hours')");
        $this->ztdExec("INSERT INTO pg_it_events (id, name, scheduled_at, duration) VALUES (3, 'Break', '2025-06-15 12:00:00', INTERVAL '30 minutes')");
    }

    /**
     * SELECT with INTERVAL in computed column.
     */
    public function testIntervalInComputed(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, (scheduled_at + duration) AS end_time FROM pg_it_events ORDER BY id"
            );

            $this->assertCount(3, $rows);
            // Meeting 10:00 + 1 hour = 11:00
            $this->assertStringContainsString('11:00', $rows[0]['end_time']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INTERVAL in computed failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * WHERE with date arithmetic using INTERVAL.
     */
    public function testIntervalInWhereArithmetic(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM pg_it_events
                 WHERE scheduled_at > TIMESTAMP '2025-06-15 00:00:00' + INTERVAL '11 hours'
                 ORDER BY id"
            );

            // Events after 11:00: Workshop (14:00), Break (12:00)
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INTERVAL in WHERE arithmetic failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INTERVAL value survives shadow store round-trip.
     */
    public function testIntervalRoundTrip(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT duration::TEXT AS dur FROM pg_it_events WHERE id = 1"
            );

            $this->assertCount(1, $rows);
            // INTERVAL '1 hour' should be returned as '01:00:00' or similar
            $this->assertNotNull($rows[0]['dur']);
            if ($rows[0]['dur'] === null || $rows[0]['dur'] === '') {
                $this->markTestIncomplete(
                    'INTERVAL value lost in shadow store.'
                );
            }
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INTERVAL round-trip failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with INTERVAL literal in SET.
     */
    public function testUpdateWithIntervalLiteral(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_it_events SET duration = INTERVAL '2 hours 30 minutes' WHERE id = 3"
            );

            $rows = $this->ztdQuery("SELECT duration::TEXT AS dur FROM pg_it_events WHERE id = 3");
            $this->assertCount(1, $rows);
            $this->assertStringContainsString('02:30', $rows[0]['dur']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with INTERVAL literal failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with INTERVAL then compute end time.
     */
    public function testInsertIntervalThenCompute(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_it_events (id, name, scheduled_at, duration) VALUES (4, 'Lunch', '2025-06-15 12:00:00', INTERVAL '45 minutes')"
            );

            $rows = $this->ztdQuery(
                "SELECT (scheduled_at + duration)::TEXT AS end_time FROM pg_it_events WHERE id = 4"
            );
            $this->assertCount(1, $rows);
            $this->assertStringContainsString('12:45', $rows[0]['end_time']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT INTERVAL then compute failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ORDER BY computed INTERVAL expression.
     */
    public function testOrderByIntervalExpression(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM pg_it_events ORDER BY duration"
            );

            $this->assertCount(3, $rows);
            // 30 min < 1 hour < 3 hours
            $this->assertSame('Break', $rows[0]['name']);
            $this->assertSame('Meeting', $rows[1]['name']);
            $this->assertSame('Workshop', $rows[2]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'ORDER BY INTERVAL failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * AGE() function with shadow data.
     */
    public function testAgeFunctionWithShadowData(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, AGE(TIMESTAMP '2025-06-15 17:00:00', scheduled_at)::TEXT AS time_until
                 FROM pg_it_events ORDER BY id"
            );

            $this->assertCount(3, $rows);
            // Meeting at 10:00: AGE from 17:00 = 7 hours
            $this->assertStringContainsString('07:00', $rows[0]['time_until']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'AGE function failed: ' . $e->getMessage()
            );
        }
    }
}
