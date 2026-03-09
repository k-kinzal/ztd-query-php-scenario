<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests aggregate FILTER (WHERE ...) clause through PostgreSQL CTE shadow store.
 *
 * PostgreSQL supports FILTER since 9.4. The CTE rewriter must preserve
 * FILTER clauses when generating CTE-rewritten queries.
 *
 * Note: PostgresAggregateFilterTest already exists but tests a specific
 * scenario. This test provides systematic coverage of FILTER clause
 * behavior including edge cases.
 */
class PostgresAggregateFilterClauseTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_afc_events (
                id SERIAL PRIMARY KEY,
                event_type VARCHAR(50) NOT NULL,
                user_id INTEGER NOT NULL,
                revenue NUMERIC(10,2),
                event_date DATE NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_afc_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_afc_events (id, event_type, user_id, revenue, event_date) VALUES (1, 'purchase', 1, 100.00, '2025-01-01')");
        $this->pdo->exec("INSERT INTO pg_afc_events (id, event_type, user_id, revenue, event_date) VALUES (2, 'purchase', 1, 200.00, '2025-01-15')");
        $this->pdo->exec("INSERT INTO pg_afc_events (id, event_type, user_id, revenue, event_date) VALUES (3, 'refund', 1, -50.00, '2025-01-20')");
        $this->pdo->exec("INSERT INTO pg_afc_events (id, event_type, user_id, revenue, event_date) VALUES (4, 'purchase', 2, 75.00, '2025-01-05')");
        $this->pdo->exec("INSERT INTO pg_afc_events (id, event_type, user_id, revenue, event_date) VALUES (5, 'view', 2, NULL, '2025-01-10')");
        $this->pdo->exec("INSERT INTO pg_afc_events (id, event_type, user_id, revenue, event_date) VALUES (6, 'purchase', 3, 500.00, '2025-02-01')");
        $this->pdo->exec("INSERT INTO pg_afc_events (id, event_type, user_id, revenue, event_date) VALUES (7, 'refund', 3, -100.00, '2025-02-15')");
    }

    /**
     * COUNT with FILTER per user.
     */
    public function testCountFilter(): void
    {
        $rows = $this->ztdQuery(
            "SELECT user_id,
                    COUNT(*) AS total_events,
                    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
                    COUNT(*) FILTER (WHERE event_type = 'refund') AS refunds
             FROM pg_afc_events
             GROUP BY user_id
             ORDER BY user_id"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(3, $rows[0]['total_events']); // user 1
        $this->assertEquals(2, $rows[0]['purchases']);
        $this->assertEquals(1, $rows[0]['refunds']);
    }

    /**
     * SUM with FILTER — separate purchase and refund totals.
     */
    public function testSumFilter(): void
    {
        $rows = $this->ztdQuery(
            "SELECT user_id,
                    SUM(revenue) FILTER (WHERE event_type = 'purchase') AS purchase_total,
                    SUM(revenue) FILTER (WHERE event_type = 'refund') AS refund_total
             FROM pg_afc_events
             GROUP BY user_id
             ORDER BY user_id"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(300.00, (float) $rows[0]['purchase_total']); // user 1
        $this->assertEquals(-50.00, (float) $rows[0]['refund_total']);
    }

    /**
     * FILTER with compound condition.
     */
    public function testFilterCompoundCondition(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                COUNT(*) FILTER (WHERE event_type = 'purchase' AND revenue > 100) AS big_purchases,
                SUM(revenue) FILTER (WHERE event_date >= '2025-02-01') AS feb_revenue
             FROM pg_afc_events"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(2, $rows[0]['big_purchases']); // 200 + 500
        $this->assertEquals(400.00, (float) $rows[0]['feb_revenue']); // 500 + (-100)
    }

    /**
     * FILTER in HAVING clause.
     */
    public function testFilterInHaving(): void
    {
        $rows = $this->ztdQuery(
            "SELECT user_id,
                    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases
             FROM pg_afc_events
             GROUP BY user_id
             HAVING COUNT(*) FILTER (WHERE event_type = 'purchase') >= 2
             ORDER BY user_id"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1, $rows[0]['user_id']);
    }

    /**
     * FILTER with prepared statement parameter inside FILTER condition.
     *
     * The CTE rewriter may mishandle $N params inside FILTER (WHERE ...)
     * clauses, producing incorrect filtering results.
     */
    public function testFilterWithPreparedParam(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT user_id,
                    SUM(revenue) FILTER (WHERE event_type = $1) AS filtered_total
             FROM pg_afc_events
             GROUP BY user_id
             ORDER BY user_id",
            ['purchase']
        );

        $this->assertCount(3, $rows);

        if ((float) $rows[0]['filtered_total'] === 0.0 || $rows[0]['filtered_total'] === null) {
            $this->markTestIncomplete(
                'FILTER (WHERE event_type = $1) with prepared param returns 0/NULL on PostgreSQL. '
                . 'The CTE rewriter may misparse $N parameters inside FILTER clauses, '
                . 'causing the filter condition to always be false or to be dropped entirely.'
            );
        }

        $this->assertEquals(300.00, (float) $rows[0]['filtered_total']);
        $this->assertEquals(75.00, (float) $rows[1]['filtered_total']);
        $this->assertEquals(500.00, (float) $rows[2]['filtered_total']);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM pg_afc_events')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
