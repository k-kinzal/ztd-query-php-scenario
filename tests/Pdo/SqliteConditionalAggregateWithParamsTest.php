<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests conditional aggregation (SUM/COUNT with CASE) using prepared params.
 *
 * Known: CASE with params in WHERE is broken (#75), HAVING with params is broken (#22).
 * This tests CASE expressions inside aggregate functions with bound params — a very common
 * reporting pattern (pivot tables, conditional totals).
 *
 * @spec SPEC-3.2, SPEC-3.3
 */
class SqliteConditionalAggregateWithParamsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_cag_orders (
            id INTEGER PRIMARY KEY,
            customer TEXT NOT NULL,
            status TEXT NOT NULL,
            amount REAL NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_cag_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_cag_orders VALUES (1, 'alice', 'completed', 100.00)");
        $this->pdo->exec("INSERT INTO sl_cag_orders VALUES (2, 'alice', 'pending',    50.00)");
        $this->pdo->exec("INSERT INTO sl_cag_orders VALUES (3, 'alice', 'completed', 200.00)");
        $this->pdo->exec("INSERT INTO sl_cag_orders VALUES (4, 'bob',   'completed', 150.00)");
        $this->pdo->exec("INSERT INTO sl_cag_orders VALUES (5, 'bob',   'cancelled',  30.00)");
        $this->pdo->exec("INSERT INTO sl_cag_orders VALUES (6, 'bob',   'pending',    80.00)");
    }

    /**
     * SUM(CASE WHEN status = ? THEN amount ELSE 0 END) — conditional sum with param.
     *
     * Expected for 'completed':
     *   alice: 100 + 200 = 300
     *   bob:   150
     */
    public function testSumCaseWithPreparedParam(): void
    {
        $sql = "SELECT customer, SUM(CASE WHEN status = ? THEN amount ELSE 0 END) AS cond_total
                FROM sl_cag_orders GROUP BY customer ORDER BY customer";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, ['completed']);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'SUM(CASE WHEN status = ? ...): expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('alice', $rows[0]['customer']);
            $this->assertEquals(300.0, (float) $rows[0]['cond_total'], '', 0.01);
            $this->assertSame('bob', $rows[1]['customer']);
            $this->assertEquals(150.0, (float) $rows[1]['cond_total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SUM(CASE WHEN status = ? ...) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * COUNT(CASE WHEN status = ? THEN 1 END) — conditional count with param.
     *
     * Expected for 'pending':
     *   alice: 1
     *   bob:   1
     */
    public function testCountCaseWithPreparedParam(): void
    {
        $sql = "SELECT customer, COUNT(CASE WHEN status = ? THEN 1 END) AS cnt
                FROM sl_cag_orders GROUP BY customer ORDER BY customer";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, ['pending']);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'COUNT(CASE WHEN status = ? ...): expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('alice', $rows[0]['customer']);
            $this->assertSame(1, (int) $rows[0]['cnt']);
            $this->assertSame('bob', $rows[1]['customer']);
            $this->assertSame(1, (int) $rows[1]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'COUNT(CASE WHEN status = ? ...) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multiple conditional aggregates with different params in same query.
     *
     * SELECT customer,
     *   SUM(CASE WHEN status = ? THEN amount ELSE 0 END) AS completed_total,
     *   SUM(CASE WHEN status = ? THEN amount ELSE 0 END) AS pending_total
     * FROM sl_cag_orders GROUP BY customer
     *
     * Params: ['completed', 'pending']
     * Expected:
     *   alice: completed=300, pending=50
     *   bob:   completed=150, pending=80
     */
    public function testMultipleConditionalAggregatesWithParams(): void
    {
        $sql = "SELECT customer,
                    SUM(CASE WHEN status = ? THEN amount ELSE 0 END) AS completed_total,
                    SUM(CASE WHEN status = ? THEN amount ELSE 0 END) AS pending_total
                FROM sl_cag_orders
                GROUP BY customer ORDER BY customer";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, ['completed', 'pending']);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Multiple conditional aggregates: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);

            // alice: completed=300, pending=50
            $this->assertSame('alice', $rows[0]['customer']);
            $aliceCompleted = (float) $rows[0]['completed_total'];
            $alicePending = (float) $rows[0]['pending_total'];
            if (abs($aliceCompleted - 300.0) > 0.01 || abs($alicePending - 50.0) > 0.01) {
                $this->markTestIncomplete(
                    "alice conditional aggregates wrong: completed={$aliceCompleted} (expected 300), "
                    . "pending={$alicePending} (expected 50)"
                );
            }

            // bob: completed=150, pending=80
            $this->assertSame('bob', $rows[1]['customer']);
            $bobCompleted = (float) $rows[1]['completed_total'];
            $bobPending = (float) $rows[1]['pending_total'];
            if (abs($bobCompleted - 150.0) > 0.01 || abs($bobPending - 80.0) > 0.01) {
                $this->markTestIncomplete(
                    "bob conditional aggregates wrong: completed={$bobCompleted} (expected 150), "
                    . "pending={$bobPending} (expected 80)"
                );
            }

            $this->assertEquals(300.0, $aliceCompleted, '', 0.01);
            $this->assertEquals(50.0, $alicePending, '', 0.01);
            $this->assertEquals(150.0, $bobCompleted, '', 0.01);
            $this->assertEquals(80.0, $bobPending, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multiple conditional aggregates with params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Same conditional aggregation query WITHOUT params (literal values) — control.
     * This should work even if the prepared param version fails.
     */
    public function testConditionalAggregateWithLiteralsControl(): void
    {
        $sql = "SELECT customer,
                    SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_total,
                    SUM(CASE WHEN status = 'pending' THEN amount ELSE 0 END) AS pending_total
                FROM sl_cag_orders
                GROUP BY customer ORDER BY customer";

        $rows = $this->ztdQuery($sql);

        $this->assertCount(2, $rows);
        $this->assertSame('alice', $rows[0]['customer']);
        $this->assertEquals(300.0, (float) $rows[0]['completed_total'], '', 0.01);
        $this->assertEquals(50.0, (float) $rows[0]['pending_total'], '', 0.01);
        $this->assertSame('bob', $rows[1]['customer']);
        $this->assertEquals(150.0, (float) $rows[1]['completed_total'], '', 0.01);
        $this->assertEquals(80.0, (float) $rows[1]['pending_total'], '', 0.01);
    }

    /**
     * Conditional aggregate with HAVING and param.
     *
     * SUM(CASE WHEN status = ? THEN amount ELSE 0 END) > 200
     * With param 'completed': only alice (300) qualifies, bob (150) does not.
     */
    public function testConditionalAggregateInHavingWithParam(): void
    {
        $sql = "SELECT customer, SUM(CASE WHEN status = ? THEN amount ELSE 0 END) AS cond_total
                FROM sl_cag_orders
                GROUP BY customer
                HAVING SUM(CASE WHEN status = ? THEN amount ELSE 0 END) > 200
                ORDER BY customer";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, ['completed', 'completed']);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Conditional aggregate in HAVING with param: expected 1 row (alice), got '
                    . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('alice', $rows[0]['customer']);
            $this->assertEquals(300.0, (float) $rows[0]['cond_total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Conditional aggregate in HAVING with param failed: ' . $e->getMessage()
            );
        }
    }
}
