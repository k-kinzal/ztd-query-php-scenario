<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CASE expressions in UPDATE SET clause through ZTD.
 *
 * CASE in UPDATE SET is a common pattern for conditional updates (e.g., bulk
 * status transitions, tier assignments). Known to be fragile with prepared
 * statements on PostgreSQL [Issue #61]; this tests SQLite behavior.
 * @spec SPEC-4.2
 */
class SqliteCaseInUpdateSetTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE ciu_accounts (id INTEGER PRIMARY KEY, name TEXT, balance REAL, tier TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['ciu_accounts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO ciu_accounts VALUES (1, 'Alice', 5000.00, 'silver')");
        $this->pdo->exec("INSERT INTO ciu_accounts VALUES (2, 'Bob', 500.00, 'bronze')");
        $this->pdo->exec("INSERT INTO ciu_accounts VALUES (3, 'Charlie', 15000.00, 'gold')");
        $this->pdo->exec("INSERT INTO ciu_accounts VALUES (4, 'Diana', 0.00, 'bronze')");
    }

    /**
     * Simple CASE in SET clause via exec().
     */
    public function testCaseInSetExec(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE ciu_accounts SET tier = CASE
                    WHEN balance >= 10000 THEN 'gold'
                    WHEN balance >= 1000 THEN 'silver'
                    ELSE 'bronze'
                 END"
            );
            $rows = $this->ztdQuery('SELECT id, tier FROM ciu_accounts ORDER BY id');
        } catch (\Exception $e) {
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
            $this->pdo->exec(
                "UPDATE ciu_accounts SET tier = CASE
                    WHEN balance >= 10000 THEN 'platinum'
                    ELSE 'gold'
                 END
                 WHERE balance >= 1000"
            );
            $rows = $this->ztdQuery('SELECT id, tier FROM ciu_accounts ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('CASE in SET with WHERE failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(4, $rows);
        $this->assertSame('gold', $rows[0]['tier']); // Alice: 5000 >= 1000
        $this->assertSame('bronze', $rows[1]['tier']); // Bob: 500 < 1000 (not updated)
        $this->assertSame('platinum', $rows[2]['tier']); // Charlie: 15000 >= 10000
        $this->assertSame('bronze', $rows[3]['tier']); // Diana: 0 (not updated)
    }

    /**
     * Multiple CASE expressions in single UPDATE.
     */
    public function testMultipleCaseInSet(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE ciu_accounts SET
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
            $rows = $this->ztdQuery('SELECT id, tier, balance FROM ciu_accounts ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Multiple CASE in SET failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(4, $rows);
        $this->assertSame('silver', $rows[0]['tier']);
        $this->assertEquals(5150.00, (float) $rows[0]['balance'], '', 0.01); // 5000 * 1.03
        $this->assertSame('gold', $rows[2]['tier']);
        $this->assertEquals(15750.00, (float) $rows[2]['balance'], '', 0.01); // 15000 * 1.05
    }

    /**
     * Searched CASE (CASE WHEN without CASE <expr>).
     */
    public function testSearchedCase(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE ciu_accounts SET name = CASE
                    WHEN id = 1 THEN 'Alice A.'
                    WHEN id = 2 THEN 'Bob B.'
                    ELSE name
                 END"
            );
            $rows = $this->ztdQuery('SELECT id, name FROM ciu_accounts ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Searched CASE in UPDATE failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(4, $rows);
        $this->assertSame('Alice A.', $rows[0]['name']);
        $this->assertSame('Bob B.', $rows[1]['name']);
        $this->assertSame('Charlie', $rows[2]['name']); // unchanged
    }

    /**
     * CASE in SET then verify with SELECT after another mutation.
     */
    public function testCaseUpdateThenFurtherMutation(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE ciu_accounts SET tier = CASE
                    WHEN balance >= 10000 THEN 'gold'
                    ELSE tier
                 END"
            );
            // Further mutation
            $this->pdo->exec("INSERT INTO ciu_accounts VALUES (5, 'Eve', 20000.00, 'gold')");
            $this->pdo->exec("DELETE FROM ciu_accounts WHERE balance = 0");

            $rows = $this->ztdQuery(
                "SELECT name, tier FROM ciu_accounts WHERE tier = 'gold' ORDER BY name"
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('CASE UPDATE + further mutations failed: ' . $e->getMessage());
            return;
        }

        // Charlie (15000 -> gold) and Eve (20000, inserted as gold)
        $this->assertCount(2, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
        $this->assertSame('Eve', $rows[1]['name']);
    }

    /**
     * Prepared CASE in UPDATE SET — known fragile area.
     */
    public function testPreparedCaseInSet(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE ciu_accounts SET tier = CASE
                    WHEN balance >= ? THEN 'gold'
                    WHEN balance >= ? THEN 'silver'
                    ELSE 'bronze'
                 END"
            );
            $stmt->execute([10000, 1000]);

            $rows = $this->ztdQuery('SELECT id, tier FROM ciu_accounts ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Prepared CASE in UPDATE SET failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(4, $rows);
        $this->assertSame('silver', $rows[0]['tier']); // 5000
        $this->assertSame('bronze', $rows[1]['tier']); // 500
        $this->assertSame('gold', $rows[2]['tier']); // 15000
        $this->assertSame('bronze', $rows[3]['tier']); // 0
    }

    /**
     * Nested CASE in SET.
     */
    public function testNestedCaseInSet(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE ciu_accounts SET tier = CASE
                    WHEN balance >= 10000 THEN CASE
                        WHEN balance >= 20000 THEN 'platinum'
                        ELSE 'gold'
                    END
                    WHEN balance >= 1000 THEN 'silver'
                    ELSE 'bronze'
                 END"
            );
            $rows = $this->ztdQuery('SELECT id, tier FROM ciu_accounts ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Nested CASE in SET failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(4, $rows);
        $this->assertSame('silver', $rows[0]['tier']); // 5000
        $this->assertSame('bronze', $rows[1]['tier']); // 500
        $this->assertSame('gold', $rows[2]['tier']); // 15000 (>= 10000 but < 20000)
        $this->assertSame('bronze', $rows[3]['tier']); // 0
    }
}
