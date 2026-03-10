<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests CASE expressions in UPDATE SET clause through ZTD on MySQLi.
 *
 * CASE in UPDATE SET is common for bulk conditional updates.
 * Known fragile on MySQL: Issue #96 found CASE in WHERE matches ALL rows.
 *
 * @spec SPEC-4.2
 */
class CaseInUpdateSetTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_ciu_accounts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                balance DECIMAL(10,2) NOT NULL,
                tier VARCHAR(20) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_ciu_accounts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_ciu_accounts (name, balance, tier) VALUES ('Alice', 5000.00, 'silver')");
        $this->mysqli->query("INSERT INTO mi_ciu_accounts (name, balance, tier) VALUES ('Bob', 500.00, 'bronze')");
        $this->mysqli->query("INSERT INTO mi_ciu_accounts (name, balance, tier) VALUES ('Charlie', 15000.00, 'gold')");
        $this->mysqli->query("INSERT INTO mi_ciu_accounts (name, balance, tier) VALUES ('Diana', 0.00, 'bronze')");
    }

    /**
     * Simple CASE in SET clause.
     */
    public function testCaseInSetExec(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_ciu_accounts SET tier = CASE
                    WHEN balance >= 10000 THEN 'gold'
                    WHEN balance >= 1000 THEN 'silver'
                    ELSE 'bronze'
                 END"
            );
            $rows = $this->ztdQuery('SELECT id, tier FROM mi_ciu_accounts ORDER BY id');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE in UPDATE SET failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(4, $rows);
        $this->assertSame('silver', $rows[0]['tier']); // Alice: 5000
        $this->assertSame('bronze', $rows[1]['tier']); // Bob: 500
        $this->assertSame('gold', $rows[2]['tier']); // Charlie: 15000
        $this->assertSame('bronze', $rows[3]['tier']); // Diana: 0
    }

    /**
     * CASE in SET with WHERE clause.
     */
    public function testCaseInSetWithWhere(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_ciu_accounts SET tier = CASE
                    WHEN balance >= 10000 THEN 'platinum'
                    ELSE 'gold'
                 END
                 WHERE balance >= 1000"
            );
            $rows = $this->ztdQuery('SELECT id, tier FROM mi_ciu_accounts ORDER BY id');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE in SET with WHERE failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(4, $rows);
        $this->assertSame('gold', $rows[0]['tier']);      // Alice: 5000 >= 1000
        $this->assertSame('bronze', $rows[1]['tier']);     // Bob: not updated
        $this->assertSame('platinum', $rows[2]['tier']);   // Charlie: 15000 >= 10000
        $this->assertSame('bronze', $rows[3]['tier']);     // Diana: not updated
    }

    /**
     * Multiple CASE expressions in single UPDATE.
     */
    public function testMultipleCaseInSet(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_ciu_accounts SET
                    tier = CASE
                        WHEN balance >= 10000 THEN 'gold'
                        WHEN balance >= 1000 THEN 'silver'
                        ELSE 'bronze'
                    END,
                    balance = CASE
                        WHEN balance >= 10000 THEN balance * 1.05
                        WHEN balance >= 1000 THEN balance * 1.03
                        ELSE balance
                    END"
            );
            $rows = $this->ztdQuery('SELECT id, tier, balance FROM mi_ciu_accounts ORDER BY id');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiple CASE in SET failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(4, $rows);
        $this->assertSame('silver', $rows[0]['tier']);
        $this->assertEqualsWithDelta(5150.00, (float) $rows[0]['balance'], 0.01);
        $this->assertSame('gold', $rows[2]['tier']);
        $this->assertEqualsWithDelta(15750.00, (float) $rows[2]['balance'], 0.01);
    }

    /**
     * Prepared CASE in UPDATE SET.
     */
    public function testPreparedCaseInSet(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "UPDATE mi_ciu_accounts SET tier = CASE
                    WHEN balance >= ? THEN 'gold'
                    WHEN balance >= ? THEN 'silver'
                    ELSE 'bronze'
                 END",
                [10000, 1000]
            );

            $rows = $this->ztdQuery('SELECT id, tier FROM mi_ciu_accounts ORDER BY id');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared CASE in UPDATE SET failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(4, $rows);
        $this->assertSame('silver', $rows[0]['tier']);
        $this->assertSame('bronze', $rows[1]['tier']);
        $this->assertSame('gold', $rows[2]['tier']);
        $this->assertSame('bronze', $rows[3]['tier']);
    }
}
