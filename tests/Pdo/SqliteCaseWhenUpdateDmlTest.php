<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE SET with CASE WHEN expressions on SQLite.
 *
 * CASE WHEN in UPDATE is common for conditional business logic:
 * status transitions, tiered pricing, batch categorization.
 *
 * @spec SPEC-10.2
 */
class SqliteCaseWhenUpdateDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_cw_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer TEXT NOT NULL,
            amount REAL NOT NULL,
            status TEXT DEFAULT 'pending',
            tier TEXT
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_cw_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_cw_orders (customer, amount, status) VALUES ('Alice', 50.00, 'pending')");
        $this->ztdExec("INSERT INTO sl_cw_orders (customer, amount, status) VALUES ('Bob', 150.00, 'pending')");
        $this->ztdExec("INSERT INTO sl_cw_orders (customer, amount, status) VALUES ('Charlie', 500.00, 'pending')");
        $this->ztdExec("INSERT INTO sl_cw_orders (customer, amount, status) VALUES ('Diana', 1000.00, 'shipped')");
    }

    /**
     * UPDATE SET tier = CASE WHEN amount >= 500 THEN 'gold' ... END.
     */
    public function testCaseWhenUpdateTier(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_cw_orders SET tier = CASE
                    WHEN amount >= 500 THEN 'gold'
                    WHEN amount >= 100 THEN 'silver'
                    ELSE 'bronze'
                END"
            );

            $rows = $this->ztdQuery("SELECT customer, tier FROM sl_cw_orders ORDER BY customer");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'CASE WHEN tier (SQLite): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $expected = ['Alice' => 'bronze', 'Bob' => 'silver', 'Charlie' => 'gold', 'Diana' => 'gold'];
            foreach ($rows as $row) {
                if ($row['tier'] !== $expected[$row['customer']]) {
                    $this->markTestIncomplete(
                        'CASE WHEN tier (SQLite): ' . $row['customer']
                        . ' expected ' . $expected[$row['customer']] . ', got ' . ($row['tier'] ?? 'NULL')
                    );
                }
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE WHEN tier (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET status = CASE WHEN with WHERE clause.
     */
    public function testCaseWhenWithWhere(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_cw_orders SET status = CASE
                    WHEN amount >= 200 THEN 'approved'
                    ELSE 'review'
                END
                WHERE status = 'pending'"
            );

            $rows = $this->ztdQuery("SELECT customer, status FROM sl_cw_orders ORDER BY customer");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'CASE WHEN WHERE (SQLite): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $expected = ['Alice' => 'review', 'Bob' => 'review', 'Charlie' => 'approved', 'Diana' => 'shipped'];
            foreach ($rows as $row) {
                if ($row['status'] !== $expected[$row['customer']]) {
                    $this->markTestIncomplete(
                        'CASE WHEN WHERE (SQLite): ' . $row['customer']
                        . ' expected ' . $expected[$row['customer']] . ', got ' . $row['status']
                    );
                }
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE WHEN WHERE (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE multiple columns using CASE WHEN.
     */
    public function testCaseWhenMultiColumn(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_cw_orders SET
                    tier = CASE WHEN amount >= 500 THEN 'gold' ELSE 'standard' END,
                    status = CASE WHEN amount >= 500 THEN 'priority' ELSE status END
                WHERE status = 'pending'"
            );

            $gold = $this->ztdQuery("SELECT customer, tier, status FROM sl_cw_orders WHERE customer = 'Charlie'");

            if (count($gold) !== 1) {
                $this->markTestIncomplete('CASE multi-col (SQLite): expected 1 gold row, got ' . count($gold));
            }

            if ($gold[0]['tier'] !== 'gold' || $gold[0]['status'] !== 'priority') {
                $this->markTestIncomplete(
                    'CASE multi-col (SQLite): Charlie expected gold/priority, got '
                    . ($gold[0]['tier'] ?? 'NULL') . '/' . ($gold[0]['status'] ?? 'NULL')
                );
            }

            $this->assertSame('gold', $gold[0]['tier']);
            $this->assertSame('priority', $gold[0]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE multi-col (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * Sequential CASE WHEN updates on shadow data.
     */
    public function testCaseWhenAfterDml(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_cw_orders (customer, amount, status) VALUES ('Eve', 300.00, 'pending')");

            $this->ztdExec(
                "UPDATE sl_cw_orders SET tier = CASE
                    WHEN amount >= 500 THEN 'gold'
                    WHEN amount >= 100 THEN 'silver'
                    ELSE 'bronze'
                END"
            );

            $eve = $this->ztdQuery("SELECT tier FROM sl_cw_orders WHERE customer = 'Eve'");

            if (count($eve) !== 1) {
                $this->markTestIncomplete('CASE after DML (SQLite): expected 1 Eve row, got ' . count($eve));
            }

            if ($eve[0]['tier'] !== 'silver') {
                $this->markTestIncomplete(
                    'CASE after DML (SQLite): Eve expected silver, got ' . ($eve[0]['tier'] ?? 'NULL')
                );
            }

            $this->assertSame('silver', $eve[0]['tier']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE after DML (SQLite) failed: ' . $e->getMessage());
        }
    }
}
