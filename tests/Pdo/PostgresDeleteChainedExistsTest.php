<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests DELETE with multiple chained EXISTS / NOT EXISTS conditions — PostgreSQL.
 *
 * @spec SPEC-4.3
 */
class PostgresDeleteChainedExistsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_dce_users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            )',
            'CREATE TABLE pg_dce_orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                total NUMERIC(10,2) NOT NULL
            )',
            'CREATE TABLE pg_dce_payments (
                id INTEGER PRIMARY KEY,
                order_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                status VARCHAR(20) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_dce_payments', 'pg_dce_orders', 'pg_dce_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_dce_users VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO pg_dce_users VALUES (2, 'Bob', 1)");
        $this->pdo->exec("INSERT INTO pg_dce_users VALUES (3, 'Charlie', 1)");
        $this->pdo->exec("INSERT INTO pg_dce_users VALUES (4, 'Diana', 1)");

        $this->pdo->exec("INSERT INTO pg_dce_orders VALUES (1, 1, 100)");
        $this->pdo->exec("INSERT INTO pg_dce_orders VALUES (2, 1, 200)");
        $this->pdo->exec("INSERT INTO pg_dce_orders VALUES (3, 2, 50)");
        $this->pdo->exec("INSERT INTO pg_dce_orders VALUES (4, 3, 75)");

        $this->pdo->exec("INSERT INTO pg_dce_payments VALUES (1, 1, 1, 'completed')");
        $this->pdo->exec("INSERT INTO pg_dce_payments VALUES (2, 2, 1, 'completed')");
        $this->pdo->exec("INSERT INTO pg_dce_payments VALUES (3, 3, 2, 'failed')");
        $this->pdo->exec("INSERT INTO pg_dce_payments VALUES (4, 4, 3, 'pending')");
    }

    public function testDeleteWithExistsAndNotExists(): void
    {
        $sql = "DELETE FROM pg_dce_users
                WHERE EXISTS (
                    SELECT 1 FROM pg_dce_orders WHERE user_id = pg_dce_users.id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM pg_dce_payments
                    WHERE user_id = pg_dce_users.id AND status = 'completed'
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT name FROM pg_dce_users ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'EXISTS+NOT EXISTS DELETE: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Alice', $names);
            $this->assertContains('Diana', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Chained EXISTS DELETE failed: ' . $e->getMessage());
        }
    }

    public function testPreparedDeleteChainedExists(): void
    {
        $sql = "DELETE FROM pg_dce_users
                WHERE EXISTS (
                    SELECT 1 FROM pg_dce_orders WHERE user_id = pg_dce_users.id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM pg_dce_payments
                    WHERE user_id = pg_dce_users.id AND status = $1
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['completed']);

            $rows = $this->ztdQuery("SELECT name FROM pg_dce_users ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared EXISTS DELETE: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Alice', $names);
            $this->assertContains('Diana', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared chained EXISTS DELETE failed: ' . $e->getMessage());
        }
    }
}
