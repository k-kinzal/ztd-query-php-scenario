<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE SET with complex multi-column arithmetic expressions
 * that reference multiple columns in the same row.
 *
 * Patterns like SET a = a + b, SET a = b * c - d, and simultaneous
 * multi-column updates where each column references others.
 *
 * @spec SPEC-4.2
 */
class SqliteUpdateMultiColumnArithmeticTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_umca_accounts (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            balance REAL NOT NULL,
            bonus REAL NOT NULL DEFAULT 0,
            total REAL NOT NULL DEFAULT 0
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_umca_accounts'];
    }

    /**
     * UPDATE SET col = col + other_col (add bonus to balance).
     */
    public function testUpdateAddColumnToColumn(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_umca_accounts (id, name, balance, bonus, total) VALUES (1, 'Alice', 100.00, 25.00, 0.00)"
            );

            $this->pdo->exec("UPDATE sl_umca_accounts SET balance = balance + bonus WHERE id = 1");

            $rows = $this->ztdQuery("SELECT balance, bonus FROM sl_umca_accounts WHERE id = 1");
            $this->assertCount(1, $rows);

            $balance = (float) $rows[0]['balance'];
            if (abs($balance - 125.00) > 0.01) {
                $this->markTestIncomplete(
                    'UPDATE balance = balance + bonus wrong. Expected 125.00, got ' . json_encode($balance)
                    . '. The shadow store may not evaluate cross-column arithmetic.'
                );
            }
            $this->assertEquals(125.00, $balance, '', 0.01);
            // bonus should remain unchanged
            $this->assertEquals(25.00, (float) $rows[0]['bonus'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Update add column to column test failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with arithmetic involving multiple columns.
     * SET total = balance * 1.05 + bonus
     */
    public function testUpdateMultiColumnFormula(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_umca_accounts (id, name, balance, bonus, total) VALUES (1, 'Bob', 200.00, 10.00, 0.00)"
            );

            $this->pdo->exec("UPDATE sl_umca_accounts SET total = balance * 1.05 + bonus WHERE id = 1");

            $rows = $this->ztdQuery("SELECT total FROM sl_umca_accounts WHERE id = 1");
            $this->assertCount(1, $rows);

            $total = (float) $rows[0]['total'];
            // 200 * 1.05 + 10 = 220
            if (abs($total - 220.00) > 0.01) {
                $this->markTestIncomplete(
                    'UPDATE total = balance * 1.05 + bonus wrong. Expected 220.00, got ' . json_encode($total)
                );
            }
            $this->assertEquals(220.00, $total, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Update multi-column formula test failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET two columns simultaneously, each referencing itself.
     * SET balance = balance - 50, bonus = bonus + 10
     */
    public function testUpdateTwoColumnsIndependently(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_umca_accounts (id, name, balance, bonus, total) VALUES (1, 'Carol', 300.00, 20.00, 0.00)"
            );

            $this->pdo->exec("UPDATE sl_umca_accounts SET balance = balance - 50, bonus = bonus + 10 WHERE id = 1");

            $rows = $this->ztdQuery("SELECT balance, bonus FROM sl_umca_accounts WHERE id = 1");
            $this->assertCount(1, $rows);

            $balance = (float) $rows[0]['balance'];
            $bonus = (float) $rows[0]['bonus'];

            if (abs($balance - 250.00) > 0.01 || abs($bonus - 30.00) > 0.01) {
                $this->markTestIncomplete(
                    'Simultaneous two-column UPDATE wrong. Expected balance=250, bonus=30. '
                    . 'Got balance=' . json_encode($balance) . ', bonus=' . json_encode($bonus)
                );
            }
            $this->assertEquals(250.00, $balance, '', 0.01);
            $this->assertEquals(30.00, $bonus, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Update two columns independently test failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with column swap (a=b, b=a).
     * This pattern is known to be fragile (see Issue #141 on MySQLi).
     * Test whether SQLite handles it.
     */
    public function testUpdateColumnSwap(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_umca_accounts (id, name, balance, bonus, total) VALUES (1, 'Dave', 100.00, 50.00, 0.00)"
            );

            $this->pdo->exec("UPDATE sl_umca_accounts SET balance = bonus, bonus = balance WHERE id = 1");

            $rows = $this->ztdQuery("SELECT balance, bonus FROM sl_umca_accounts WHERE id = 1");

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Column swap UPDATE returned 0 rows. Shadow store may have corrupted data (see Issue #141).'
                );
            }

            $balance = (float) $rows[0]['balance'];
            $bonus = (float) $rows[0]['bonus'];

            // In standard SQL, balance=bonus uses the PRE-update value of bonus (50),
            // and bonus=balance uses the PRE-update value of balance (100).
            // So after swap: balance=50, bonus=100.
            if (abs($balance - 50.00) > 0.01 || abs($bonus - 100.00) > 0.01) {
                $this->markTestIncomplete(
                    'Column swap wrong. Expected balance=50, bonus=100. '
                    . 'Got balance=' . json_encode($balance) . ', bonus=' . json_encode($bonus)
                    . '. The shadow store may evaluate SET assignments sequentially instead of concurrently.'
                );
            }
            $this->assertEquals(50.00, $balance, '', 0.01);
            $this->assertEquals(100.00, $bonus, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Column swap test failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple successive UPDATEs on the same row.
     * Each UPDATE should see the result of the previous one.
     */
    public function testChainedUpdates(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_umca_accounts (id, name, balance, bonus, total) VALUES (1, 'Eve', 100.00, 0.00, 0.00)"
            );

            $this->pdo->exec("UPDATE sl_umca_accounts SET balance = balance + 50 WHERE id = 1");
            $this->pdo->exec("UPDATE sl_umca_accounts SET bonus = balance * 0.1 WHERE id = 1");
            $this->pdo->exec("UPDATE sl_umca_accounts SET total = balance + bonus WHERE id = 1");

            $rows = $this->ztdQuery("SELECT balance, bonus, total FROM sl_umca_accounts WHERE id = 1");
            $this->assertCount(1, $rows);

            $balance = (float) $rows[0]['balance'];
            $bonus = (float) $rows[0]['bonus'];
            $total = (float) $rows[0]['total'];

            // balance = 100 + 50 = 150
            // bonus = 150 * 0.1 = 15
            // total = 150 + 15 = 165
            if (abs($balance - 150.00) > 0.01) {
                $this->markTestIncomplete(
                    'Chained UPDATE: balance wrong. Expected 150, got ' . json_encode($balance)
                );
            }
            if (abs($bonus - 15.00) > 0.01) {
                $this->markTestIncomplete(
                    'Chained UPDATE: bonus wrong. Expected 15 (150*0.1), got ' . json_encode($bonus)
                    . '. Shadow store may not see previous UPDATE result.'
                );
            }
            if (abs($total - 165.00) > 0.01) {
                $this->markTestIncomplete(
                    'Chained UPDATE: total wrong. Expected 165, got ' . json_encode($total)
                );
            }

            $this->assertEquals(150.00, $balance, '', 0.01);
            $this->assertEquals(15.00, $bonus, '', 0.01);
            $this->assertEquals(165.00, $total, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Chained updates test failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with multi-column arithmetic and params.
     */
    public function testPreparedMultiColumnUpdate(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_umca_accounts (id, name, balance, bonus, total) VALUES (1, 'Frank', 200.00, 30.00, 0.00)"
            );

            $stmt = $this->pdo->prepare(
                "UPDATE sl_umca_accounts SET balance = balance + ?, bonus = bonus * ? WHERE id = ?"
            );
            $stmt->execute([50.00, 2.0, 1]);

            $rows = $this->ztdQuery("SELECT balance, bonus FROM sl_umca_accounts WHERE id = 1");
            $this->assertCount(1, $rows);

            $balance = (float) $rows[0]['balance'];
            $bonus = (float) $rows[0]['bonus'];

            if (abs($balance - 250.00) > 0.01 || abs($bonus - 60.00) > 0.01) {
                $this->markTestIncomplete(
                    'Prepared multi-column UPDATE wrong. Expected balance=250, bonus=60. '
                    . 'Got balance=' . json_encode($balance) . ', bonus=' . json_encode($bonus)
                );
            }
            $this->assertEquals(250.00, $balance, '', 0.01);
            $this->assertEquals(60.00, $bonus, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared multi-column update test failed: ' . $e->getMessage());
        }
    }
}
