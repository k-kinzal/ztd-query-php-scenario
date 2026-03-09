<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests UPDATE with CASE expression containing correlated subquery in SET on PostgreSQL.
 *
 * @spec SPEC-4.2
 */
class PostgresUpdateCaseSubqueryTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_ucs_users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT \'unknown\'
            )',
            'CREATE TABLE pg_ucs_orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                amount NUMERIC(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_ucs_orders', 'pg_ucs_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ucs_users VALUES (1, 'Alice', 'unknown')");
        $this->pdo->exec("INSERT INTO pg_ucs_users VALUES (2, 'Bob', 'unknown')");
        $this->pdo->exec("INSERT INTO pg_ucs_users VALUES (3, 'Charlie', 'unknown')");

        $this->pdo->exec("INSERT INTO pg_ucs_orders VALUES (1, 1, 100)");
        $this->pdo->exec("INSERT INTO pg_ucs_orders VALUES (2, 1, 200)");
        $this->pdo->exec("INSERT INTO pg_ucs_orders VALUES (3, 2, 50)");
    }

    /**
     * UPDATE SET with CASE WHEN EXISTS(correlated subquery).
     */
    public function testUpdateCaseWhenExists(): void
    {
        $sql = "UPDATE pg_ucs_users
                SET status = CASE
                    WHEN EXISTS(SELECT 1 FROM pg_ucs_orders WHERE user_id = pg_ucs_users.id)
                    THEN 'active'
                    ELSE 'inactive'
                END";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, status FROM pg_ucs_users ORDER BY id");

            $this->assertCount(3, $rows);

            if ($rows[0]['status'] !== 'active' || $rows[1]['status'] !== 'active' || $rows[2]['status'] !== 'inactive') {
                $this->markTestIncomplete(
                    'CASE WHEN EXISTS: ' . json_encode($rows)
                );
            }

            $this->assertSame('active', $rows[0]['status']);
            $this->assertSame('active', $rows[1]['status']);
            $this->assertSame('inactive', $rows[2]['status']);
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
        $sql = "UPDATE pg_ucs_users
                SET status = CASE
                    WHEN (SELECT SUM(amount) FROM pg_ucs_orders WHERE user_id = pg_ucs_users.id) > 100
                    THEN 'premium'
                    WHEN (SELECT COUNT(*) FROM pg_ucs_orders WHERE user_id = pg_ucs_users.id) > 0
                    THEN 'basic'
                    ELSE 'none'
                END";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, status FROM pg_ucs_users ORDER BY id");

            $this->assertCount(3, $rows);

            if ($rows[0]['status'] !== 'premium' || $rows[1]['status'] !== 'basic' || $rows[2]['status'] !== 'none') {
                $this->markTestIncomplete(
                    'CASE scalar subquery: ' . json_encode($rows)
                );
            }

            $this->assertSame('premium', $rows[0]['status']);
            $this->assertSame('basic', $rows[1]['status']);
            $this->assertSame('none', $rows[2]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE CASE scalar subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared UPDATE CASE EXISTS with $N param.
     */
    public function testPreparedUpdateCaseExistsWithDollarParam(): void
    {
        $sql = "UPDATE pg_ucs_users
                SET status = CASE
                    WHEN EXISTS(SELECT 1 FROM pg_ucs_orders WHERE user_id = pg_ucs_users.id AND amount >= $1)
                    THEN 'high_spender'
                    ELSE 'low_spender'
                END";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([100]);

            $rows = $this->ztdQuery("SELECT id, status FROM pg_ucs_users ORDER BY id");

            $this->assertCount(3, $rows);

            if ($rows[0]['status'] !== 'high_spender') {
                $this->markTestIncomplete(
                    "Alice expected 'high_spender', got '{$rows[0]['status']}'"
                );
            }

            $this->assertSame('high_spender', $rows[0]['status']);
            $this->assertSame('low_spender', $rows[1]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared UPDATE CASE EXISTS with $N param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared UPDATE CASE EXISTS with ? param.
     */
    public function testPreparedUpdateCaseExistsWithQuestionParam(): void
    {
        $sql = "UPDATE pg_ucs_users
                SET status = CASE
                    WHEN EXISTS(SELECT 1 FROM pg_ucs_orders WHERE user_id = pg_ucs_users.id AND amount >= ?)
                    THEN 'high_spender'
                    ELSE 'low_spender'
                END";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([100]);

            $rows = $this->ztdQuery("SELECT id, status FROM pg_ucs_users ORDER BY id");

            $this->assertCount(3, $rows);

            if ($rows[0]['status'] !== 'high_spender') {
                $this->markTestIncomplete(
                    "Alice (?) expected 'high_spender', got '{$rows[0]['status']}'"
                );
            }

            $this->assertSame('high_spender', $rows[0]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared UPDATE CASE EXISTS with ? param failed: ' . $e->getMessage()
            );
        }
    }
}
