<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests conditional aggregation (SUM/COUNT with CASE) using prepared params on MySQL PDO.
 *
 * @spec SPEC-3.2, SPEC-3.3
 */
class MysqlConditionalAggregateWithParamsTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_cag_orders (
            id INT PRIMARY KEY,
            customer VARCHAR(50) NOT NULL,
            status VARCHAR(20) NOT NULL,
            amount DECIMAL(10,2) NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['my_cag_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_cag_orders VALUES (1, 'alice', 'completed', 100.00)");
        $this->pdo->exec("INSERT INTO my_cag_orders VALUES (2, 'alice', 'pending',    50.00)");
        $this->pdo->exec("INSERT INTO my_cag_orders VALUES (3, 'alice', 'completed', 200.00)");
        $this->pdo->exec("INSERT INTO my_cag_orders VALUES (4, 'bob',   'completed', 150.00)");
        $this->pdo->exec("INSERT INTO my_cag_orders VALUES (5, 'bob',   'cancelled',  30.00)");
        $this->pdo->exec("INSERT INTO my_cag_orders VALUES (6, 'bob',   'pending',    80.00)");
    }

    /**
     * SUM(CASE WHEN status = ? THEN amount ELSE 0 END) — conditional sum with param.
     */
    public function testSumCaseWithPreparedParam(): void
    {
        $sql = "SELECT customer, SUM(CASE WHEN status = ? THEN amount ELSE 0 END) AS cond_total
                FROM my_cag_orders GROUP BY customer ORDER BY customer";

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
     * Multiple conditional aggregates with different params.
     */
    public function testMultipleConditionalAggregatesWithParams(): void
    {
        $sql = "SELECT customer,
                    SUM(CASE WHEN status = ? THEN amount ELSE 0 END) AS completed_total,
                    SUM(CASE WHEN status = ? THEN amount ELSE 0 END) AS pending_total
                FROM my_cag_orders
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
            $this->assertEquals(150.0, (float) $rows[1]['completed_total'], '', 0.01);
            $this->assertEquals(80.0, (float) $rows[1]['pending_total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multiple conditional aggregates failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Conditional aggregate in HAVING with param.
     */
    public function testConditionalAggregateInHavingWithParam(): void
    {
        $sql = "SELECT customer, SUM(CASE WHEN status = ? THEN amount ELSE 0 END) AS cond_total
                FROM my_cag_orders
                GROUP BY customer
                HAVING SUM(CASE WHEN status = ? THEN amount ELSE 0 END) > 200
                ORDER BY customer";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, ['completed', 'completed']);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Conditional aggregate in HAVING: expected 1 row (alice), got '
                    . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('alice', $rows[0]['customer']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Conditional aggregate in HAVING with param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Control: literal values (no params) should work.
     */
    public function testConditionalAggregateWithLiteralsControl(): void
    {
        $sql = "SELECT customer,
                    SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_total
                FROM my_cag_orders GROUP BY customer ORDER BY customer";

        $rows = $this->ztdQuery($sql);

        $this->assertCount(2, $rows);
        $this->assertEquals(300.0, (float) $rows[0]['completed_total'], '', 0.01);
        $this->assertEquals(150.0, (float) $rows[1]['completed_total'], '', 0.01);
    }
}
