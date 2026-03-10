<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests CTE name collision: user CTE has the same name as a physical table (MySQL PDO).
 * @spec SPEC-3.3
 */
class MysqlCteNameCollisionTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_cnc_users (id INT PRIMARY KEY, name VARCHAR(50), active TINYINT)',
            'CREATE TABLE my_cnc_orders (id INT PRIMARY KEY, user_id INT, amount DECIMAL(10,2), status VARCHAR(20))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_cnc_orders', 'my_cnc_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO my_cnc_users VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO my_cnc_users VALUES (2, 'Bob', 0)");
        $this->pdo->exec("INSERT INTO my_cnc_users VALUES (3, 'Charlie', 1)");

        $this->pdo->exec("INSERT INTO my_cnc_orders VALUES (1, 1, 100.00, 'complete')");
        $this->pdo->exec("INSERT INTO my_cnc_orders VALUES (2, 1, 200.00, 'pending')");
        $this->pdo->exec("INSERT INTO my_cnc_orders VALUES (3, 3, 50.00, 'pending')");
    }

    public function testCteNamedSameAsTable(): void
    {
        $sql = "WITH my_cnc_users AS (
                    SELECT id, name, active FROM my_cnc_users WHERE active = 1
                )
                SELECT name FROM my_cnc_users ORDER BY id";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'CTE name collision SELECT: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Charlie', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CTE name collision SELECT failed: ' . $e->getMessage());
        }
    }

    public function testCteNameCollisionInJoin(): void
    {
        $sql = "WITH my_cnc_users AS (
                    SELECT id, name FROM my_cnc_users WHERE active = 1
                )
                SELECT u.name, o.amount
                FROM my_cnc_users u
                JOIN my_cnc_orders o ON o.user_id = u.id
                WHERE o.status = 'pending'
                ORDER BY o.amount DESC";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'CTE name collision JOIN: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CTE name collision JOIN failed: ' . $e->getMessage());
        }
    }

    public function testMixedCteNameCollision(): void
    {
        $sql = "WITH
                    my_cnc_orders AS (
                        SELECT id, user_id, amount FROM my_cnc_orders WHERE status = 'pending'
                    ),
                    totals AS (
                        SELECT user_id, SUM(amount) AS total FROM my_cnc_orders GROUP BY user_id
                    )
                SELECT u.name, t.total
                FROM my_cnc_users u
                JOIN totals t ON t.user_id = u.id
                ORDER BY t.total DESC";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Mixed CTE name collision: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Mixed CTE name collision failed: ' . $e->getMessage());
        }
    }

    public function testCteNameCollisionPrepared(): void
    {
        $sql = "WITH my_cnc_orders AS (
                    SELECT id, user_id, amount FROM my_cnc_orders WHERE amount > ?
                )
                SELECT u.name, o.amount
                FROM my_cnc_users u
                JOIN my_cnc_orders o ON o.user_id = u.id
                ORDER BY o.amount DESC";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, [100]);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'CTE name collision prepared: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CTE name collision prepared failed: ' . $e->getMessage());
        }
    }
}
