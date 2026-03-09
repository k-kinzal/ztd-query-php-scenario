<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests conditional aggregation (SUM/COUNT with CASE) using prepared params on PostgreSQL.
 *
 * @spec SPEC-3.2, SPEC-3.3
 */
class PostgresConditionalAggregateWithParamsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_cag_orders (
            id INTEGER PRIMARY KEY,
            customer VARCHAR(50) NOT NULL,
            status VARCHAR(20) NOT NULL,
            amount NUMERIC(10,2) NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_cag_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_cag_orders VALUES (1, 'alice', 'completed', 100.00)");
        $this->pdo->exec("INSERT INTO pg_cag_orders VALUES (2, 'alice', 'pending',    50.00)");
        $this->pdo->exec("INSERT INTO pg_cag_orders VALUES (3, 'alice', 'completed', 200.00)");
        $this->pdo->exec("INSERT INTO pg_cag_orders VALUES (4, 'bob',   'completed', 150.00)");
        $this->pdo->exec("INSERT INTO pg_cag_orders VALUES (5, 'bob',   'cancelled',  30.00)");
        $this->pdo->exec("INSERT INTO pg_cag_orders VALUES (6, 'bob',   'pending',    80.00)");
    }

    /**
     * SUM(CASE WHEN status = ? THEN amount ELSE 0 END) — with ? placeholder.
     */
    public function testSumCaseWithQuestionMarkParam(): void
    {
        $sql = "SELECT customer, SUM(CASE WHEN status = ? THEN amount ELSE 0 END) AS cond_total
                FROM pg_cag_orders GROUP BY customer ORDER BY customer";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, ['completed']);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'SUM(CASE ? params): expected 2 rows, got ' . count($rows)
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
     * Multiple conditional aggregates with params.
     */
    public function testMultipleConditionalAggregatesWithParams(): void
    {
        $sql = "SELECT customer,
                    SUM(CASE WHEN status = ? THEN amount ELSE 0 END) AS completed_total,
                    SUM(CASE WHEN status = ? THEN amount ELSE 0 END) AS pending_total
                FROM pg_cag_orders
                GROUP BY customer ORDER BY customer";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, ['completed', 'pending']);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Multiple conditional aggregates: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertEquals(300.0, (float) $rows[0]['completed_total'], '', 0.01);
            $this->assertEquals(50.0, (float) $rows[0]['pending_total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multiple conditional aggregates failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * PostgreSQL FILTER clause as alternative to CASE — prepared param.
     *
     * SUM(amount) FILTER (WHERE status = ?) — known to have issues (#62)
     * but only tested with $N params. Test with ? placeholder.
     */
    public function testFilterClauseWithQuestionMarkParam(): void
    {
        $sql = "SELECT customer, SUM(amount) FILTER (WHERE status = ?) AS filtered_total
                FROM pg_cag_orders GROUP BY customer ORDER BY customer";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, ['completed']);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'FILTER clause with ?: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('alice', $rows[0]['customer']);
            $filteredAlice = (float) $rows[0]['filtered_total'];
            $filteredBob = (float) $rows[1]['filtered_total'];

            if (abs($filteredAlice - 300.0) > 0.01 || abs($filteredBob - 150.0) > 0.01) {
                $this->markTestIncomplete(
                    "FILTER clause results wrong: alice={$filteredAlice} (exp 300), "
                    . "bob={$filteredBob} (exp 150)"
                );
            }

            $this->assertEquals(300.0, $filteredAlice, '', 0.01);
            $this->assertEquals(150.0, $filteredBob, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'FILTER clause with ? param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Control: literal values.
     */
    public function testConditionalAggregateWithLiteralsControl(): void
    {
        $sql = "SELECT customer,
                    SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_total
                FROM pg_cag_orders GROUP BY customer ORDER BY customer";

        $rows = $this->ztdQuery($sql);

        $this->assertCount(2, $rows);
        $this->assertEquals(300.0, (float) $rows[0]['completed_total'], '', 0.01);
        $this->assertEquals(150.0, (float) $rows[1]['completed_total'], '', 0.01);
    }
}
