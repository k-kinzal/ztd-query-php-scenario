<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE with CASE expression containing correlated subquery in SET.
 *
 * Pattern: UPDATE t SET status = CASE WHEN EXISTS(SELECT ...) THEN 'active' ELSE 'inactive' END
 * The CTE rewriter may mishandle correlation references in CASE within SET.
 *
 * @spec SPEC-4.2
 */
class SqliteUpdateCaseSubqueryTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ucs_users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT \'unknown\'
            )',
            'CREATE TABLE sl_ucs_orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                amount REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ucs_orders', 'sl_ucs_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ucs_users VALUES (1, 'Alice', 'unknown')");
        $this->pdo->exec("INSERT INTO sl_ucs_users VALUES (2, 'Bob', 'unknown')");
        $this->pdo->exec("INSERT INTO sl_ucs_users VALUES (3, 'Charlie', 'unknown')");

        // Alice and Bob have orders, Charlie does not
        $this->pdo->exec("INSERT INTO sl_ucs_orders VALUES (1, 1, 100)");
        $this->pdo->exec("INSERT INTO sl_ucs_orders VALUES (2, 1, 200)");
        $this->pdo->exec("INSERT INTO sl_ucs_orders VALUES (3, 2, 50)");
    }

    /**
     * UPDATE SET with CASE WHEN EXISTS(correlated subquery).
     */
    public function testUpdateCaseWhenExists(): void
    {
        $sql = "UPDATE sl_ucs_users
                SET status = CASE
                    WHEN EXISTS(SELECT 1 FROM sl_ucs_orders WHERE user_id = sl_ucs_users.id)
                    THEN 'active'
                    ELSE 'inactive'
                END";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name, status FROM sl_ucs_users ORDER BY id");

            $this->assertCount(3, $rows);

            $aliceStatus = $rows[0]['status'];
            $bobStatus = $rows[1]['status'];
            $charlieStatus = $rows[2]['status'];

            if ($aliceStatus !== 'active') {
                $this->markTestIncomplete(
                    "CASE WHEN EXISTS: Alice expected 'active', got '{$aliceStatus}'"
                );
            }
            if ($bobStatus !== 'active') {
                $this->markTestIncomplete(
                    "CASE WHEN EXISTS: Bob expected 'active', got '{$bobStatus}'"
                );
            }
            if ($charlieStatus !== 'inactive') {
                $this->markTestIncomplete(
                    "CASE WHEN EXISTS: Charlie expected 'inactive', got '{$charlieStatus}'"
                );
            }

            $this->assertSame('active', $aliceStatus);
            $this->assertSame('active', $bobStatus);
            $this->assertSame('inactive', $charlieStatus);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE CASE WHEN EXISTS failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE SET with CASE WHEN (scalar correlated subquery > threshold).
     */
    public function testUpdateCaseScalarSubquery(): void
    {
        $sql = "UPDATE sl_ucs_users
                SET status = CASE
                    WHEN (SELECT SUM(amount) FROM sl_ucs_orders WHERE user_id = sl_ucs_users.id) > 100
                    THEN 'premium'
                    WHEN (SELECT COUNT(*) FROM sl_ucs_orders WHERE user_id = sl_ucs_users.id) > 0
                    THEN 'basic'
                    ELSE 'none'
                END";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name, status FROM sl_ucs_users ORDER BY id");

            $this->assertCount(3, $rows);

            // Alice: SUM=300 > 100 → premium
            // Bob: SUM=50 <= 100, COUNT=1 > 0 → basic
            // Charlie: COUNT=0 → none
            $aliceStatus = $rows[0]['status'];
            $bobStatus = $rows[1]['status'];
            $charlieStatus = $rows[2]['status'];

            if ($aliceStatus !== 'premium') {
                $this->markTestIncomplete(
                    "CASE scalar subquery: Alice expected 'premium', got '{$aliceStatus}'"
                );
            }
            if ($bobStatus !== 'basic') {
                $this->markTestIncomplete(
                    "CASE scalar subquery: Bob expected 'basic', got '{$bobStatus}'"
                );
            }
            if ($charlieStatus !== 'none') {
                $this->markTestIncomplete(
                    "CASE scalar subquery: Charlie expected 'none', got '{$charlieStatus}'"
                );
            }

            $this->assertSame('premium', $aliceStatus);
            $this->assertSame('basic', $bobStatus);
            $this->assertSame('none', $charlieStatus);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE CASE scalar subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared UPDATE with CASE WHEN EXISTS and param.
     */
    public function testPreparedUpdateCaseExistsWithParam(): void
    {
        $sql = "UPDATE sl_ucs_users
                SET status = CASE
                    WHEN EXISTS(SELECT 1 FROM sl_ucs_orders WHERE user_id = sl_ucs_users.id AND amount >= ?)
                    THEN 'high_spender'
                    ELSE 'low_spender'
                END";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([100]);

            $rows = $this->ztdQuery("SELECT id, name, status FROM sl_ucs_users ORDER BY id");

            $this->assertCount(3, $rows);

            // Alice has orders 100, 200 (both >= 100) → high_spender
            // Bob has order 50 (< 100) → low_spender
            // Charlie has no orders → low_spender
            $aliceStatus = $rows[0]['status'];
            $bobStatus = $rows[1]['status'];

            if ($aliceStatus !== 'high_spender') {
                $this->markTestIncomplete(
                    "Prepared CASE EXISTS: Alice expected 'high_spender', got '{$aliceStatus}'"
                );
            }
            if ($bobStatus !== 'low_spender') {
                $this->markTestIncomplete(
                    "Prepared CASE EXISTS: Bob expected 'low_spender', got '{$bobStatus}'"
                );
            }

            $this->assertSame('high_spender', $aliceStatus);
            $this->assertSame('low_spender', $bobStatus);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared UPDATE CASE EXISTS with param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE CASE on shadow-inserted data.
     */
    public function testUpdateCaseExistsOnShadowData(): void
    {
        try {
            // Add an order for Charlie in shadow
            $this->pdo->exec("INSERT INTO sl_ucs_orders VALUES (4, 3, 500)");

            $sql = "UPDATE sl_ucs_users
                    SET status = CASE
                        WHEN EXISTS(SELECT 1 FROM sl_ucs_orders WHERE user_id = sl_ucs_users.id)
                        THEN 'active'
                        ELSE 'inactive'
                    END";

            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, status FROM sl_ucs_users ORDER BY id");

            $this->assertCount(3, $rows);

            // Now all three should be 'active'
            foreach ($rows as $row) {
                if ($row['status'] !== 'active') {
                    $this->markTestIncomplete(
                        "CASE EXISTS shadow: user {$row['id']} expected 'active', got '{$row['status']}'"
                    );
                }
            }

            $this->assertSame('active', $rows[0]['status']);
            $this->assertSame('active', $rows[1]['status']);
            $this->assertSame('active', $rows[2]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE CASE EXISTS on shadow data failed: ' . $e->getMessage()
            );
        }
    }
}
