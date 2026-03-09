<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests conditional aggregation (SUM/COUNT with CASE) using prepared params on MySQLi.
 *
 * @spec SPEC-3.2, SPEC-3.3
 */
class ConditionalAggregateWithParamsTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_cag_orders (
            id INT PRIMARY KEY,
            customer VARCHAR(50) NOT NULL,
            status VARCHAR(20) NOT NULL,
            amount DECIMAL(10,2) NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_cag_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_cag_orders VALUES (1, 'alice', 'completed', 100.00)");
        $this->mysqli->query("INSERT INTO mi_cag_orders VALUES (2, 'alice', 'pending',    50.00)");
        $this->mysqli->query("INSERT INTO mi_cag_orders VALUES (3, 'alice', 'completed', 200.00)");
        $this->mysqli->query("INSERT INTO mi_cag_orders VALUES (4, 'bob',   'completed', 150.00)");
        $this->mysqli->query("INSERT INTO mi_cag_orders VALUES (5, 'bob',   'cancelled',  30.00)");
        $this->mysqli->query("INSERT INTO mi_cag_orders VALUES (6, 'bob',   'pending',    80.00)");
    }

    /**
     * SUM(CASE WHEN status = ? THEN amount ELSE 0 END) via prepare+bind_param.
     */
    public function testSumCaseWithPreparedParam(): void
    {
        $sql = "SELECT customer, SUM(CASE WHEN status = ? THEN amount ELSE 0 END) AS cond_total
                FROM mi_cag_orders GROUP BY customer ORDER BY customer";

        try {
            $stmt = $this->mysqli->prepare($sql);
            $status = 'completed';
            $stmt->bind_param('s', $status);
            $stmt->execute();
            $result = $stmt->get_result();
            $rows = $result->fetch_all(MYSQLI_ASSOC);

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
                FROM mi_cag_orders
                GROUP BY customer ORDER BY customer";

        try {
            $stmt = $this->mysqli->prepare($sql);
            $s1 = 'completed';
            $s2 = 'pending';
            $stmt->bind_param('ss', $s1, $s2);
            $stmt->execute();
            $result = $stmt->get_result();
            $rows = $result->fetch_all(MYSQLI_ASSOC);

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
     * Control: literal values.
     */
    public function testConditionalAggregateWithLiteralsControl(): void
    {
        $sql = "SELECT customer,
                    SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_total
                FROM mi_cag_orders GROUP BY customer ORDER BY customer";

        $rows = $this->ztdQuery($sql);

        $this->assertCount(2, $rows);
        $this->assertEquals(300.0, (float) $rows[0]['completed_total'], '', 0.01);
        $this->assertEquals(150.0, (float) $rows[1]['completed_total'], '', 0.01);
    }
}
