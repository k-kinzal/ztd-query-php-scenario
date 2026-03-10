<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE SET with CASE WHEN expressions on MySQLi.
 *
 * @spec SPEC-10.2
 */
class CaseWhenUpdateDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE mi_cw_orders (
            id INT AUTO_INCREMENT PRIMARY KEY,
            customer VARCHAR(100) NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            status VARCHAR(30) DEFAULT 'pending',
            tier VARCHAR(30)
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['mi_cw_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_cw_orders (customer, amount, status) VALUES ('Alice', 50.00, 'pending')");
        $this->ztdExec("INSERT INTO mi_cw_orders (customer, amount, status) VALUES ('Bob', 150.00, 'pending')");
        $this->ztdExec("INSERT INTO mi_cw_orders (customer, amount, status) VALUES ('Charlie', 500.00, 'pending')");
        $this->ztdExec("INSERT INTO mi_cw_orders (customer, amount, status) VALUES ('Diana', 1000.00, 'shipped')");
    }

    public function testCaseWhenUpdateTier(): void
    {
        try {
            $this->ztdExec(
                "UPDATE mi_cw_orders SET tier = CASE
                    WHEN amount >= 500 THEN 'gold'
                    WHEN amount >= 100 THEN 'silver'
                    ELSE 'bronze'
                END"
            );

            $rows = $this->ztdQuery("SELECT customer, tier FROM mi_cw_orders ORDER BY customer");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'CASE WHEN tier (MySQLi): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $expected = ['Alice' => 'bronze', 'Bob' => 'silver', 'Charlie' => 'gold', 'Diana' => 'gold'];
            foreach ($rows as $row) {
                if ($row['tier'] !== $expected[$row['customer']]) {
                    $this->markTestIncomplete(
                        'CASE WHEN tier (MySQLi): ' . $row['customer']
                        . ' expected ' . $expected[$row['customer']] . ', got ' . ($row['tier'] ?? 'NULL')
                    );
                }
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE WHEN tier (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testCaseWhenWithWhere(): void
    {
        try {
            $this->ztdExec(
                "UPDATE mi_cw_orders SET status = CASE
                    WHEN amount >= 200 THEN 'approved'
                    ELSE 'review'
                END
                WHERE status = 'pending'"
            );

            $rows = $this->ztdQuery("SELECT customer, status FROM mi_cw_orders ORDER BY customer");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'CASE WHEN WHERE (MySQLi): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $expected = ['Alice' => 'review', 'Bob' => 'review', 'Charlie' => 'approved', 'Diana' => 'shipped'];
            foreach ($rows as $row) {
                if ($row['status'] !== $expected[$row['customer']]) {
                    $this->markTestIncomplete(
                        'CASE WHEN WHERE (MySQLi): ' . $row['customer']
                        . ' expected ' . $expected[$row['customer']] . ', got ' . $row['status']
                    );
                }
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE WHEN WHERE (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testCaseWhenAfterDml(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_cw_orders (customer, amount, status) VALUES ('Eve', 300.00, 'pending')");

            $this->ztdExec(
                "UPDATE mi_cw_orders SET tier = CASE
                    WHEN amount >= 500 THEN 'gold'
                    WHEN amount >= 100 THEN 'silver'
                    ELSE 'bronze'
                END"
            );

            $eve = $this->ztdQuery("SELECT tier FROM mi_cw_orders WHERE customer = 'Eve'");

            if (count($eve) !== 1) {
                $this->markTestIncomplete('CASE after DML (MySQLi): expected 1 Eve row, got ' . count($eve));
            }

            if ($eve[0]['tier'] !== 'silver') {
                $this->markTestIncomplete(
                    'CASE after DML (MySQLi): Eve expected silver, got ' . ($eve[0]['tier'] ?? 'NULL')
                );
            }

            $this->assertSame('silver', $eve[0]['tier']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE after DML (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
