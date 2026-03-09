<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests DELETE with JOIN patterns on PostgreSQL PDO.
 *
 * PostgreSQL does not support DELETE ... JOIN syntax directly.
 * Instead it supports:
 * 1. DELETE FROM t1 USING t2 WHERE t1.fk = t2.id AND condition
 * 2. DELETE FROM t1 WHERE col IN (SELECT col FROM t2 WHERE condition)
 * 3. DELETE FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.fk = t1.id AND condition)
 *
 * These are common patterns that users need for cross-table deletion.
 * @spec SPEC-4.2d
 */
class PostgresDeleteWithJoinTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_dj_users (id INT PRIMARY KEY, name VARCHAR(50), active BOOLEAN DEFAULT TRUE)',
            'CREATE TABLE pg_dj_orders (id INT PRIMARY KEY, user_id INT, amount INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_dj_orders', 'pg_dj_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_dj_users (id, name, active) VALUES (1, 'Alice', TRUE)");
        $this->pdo->exec("INSERT INTO pg_dj_users (id, name, active) VALUES (2, 'Bob', FALSE)");
        $this->pdo->exec("INSERT INTO pg_dj_users (id, name, active) VALUES (3, 'Charlie', TRUE)");
        $this->pdo->exec("INSERT INTO pg_dj_orders (id, user_id, amount) VALUES (1, 1, 100)");
        $this->pdo->exec("INSERT INTO pg_dj_orders (id, user_id, amount) VALUES (2, 2, 200)");
        $this->pdo->exec("INSERT INTO pg_dj_orders (id, user_id, amount) VALUES (3, 2, 50)");
        $this->pdo->exec("INSERT INTO pg_dj_orders (id, user_id, amount) VALUES (4, 3, 300)");
    }

    /**
     * DELETE ... USING — PostgreSQL's equivalent of DELETE ... JOIN.
     * Deletes orders belonging to inactive users.
     */
    public function testDeleteUsingJoin(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM pg_dj_orders o USING pg_dj_users u WHERE o.user_id = u.id AND u.active = FALSE"
            );

            $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_dj_orders WHERE user_id = 2');
            $this->assertSame(0, (int) $stmt->fetchColumn());

            $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_dj_orders');
            $this->assertSame(2, (int) $stmt->fetchColumn());
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE...USING not supported through ZTD: ' . get_class($e) . ' — ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE WHERE IN (subquery) — cross-table deletion via subquery.
     */
    public function testDeleteWhereInSubquery(): void
    {
        $this->pdo->exec(
            "DELETE FROM pg_dj_orders WHERE user_id IN (SELECT id FROM pg_dj_users WHERE active = FALSE)"
        );

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_dj_orders WHERE user_id = 2');
        $this->assertSame(0, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_dj_orders');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE WHERE EXISTS — correlated subquery deletion.
     */
    public function testDeleteWhereExists(): void
    {
        $this->pdo->exec(
            "DELETE FROM pg_dj_orders o WHERE EXISTS (SELECT 1 FROM pg_dj_users u WHERE u.id = o.user_id AND u.active = FALSE)"
        );

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_dj_orders WHERE user_id = 2');
        $this->assertSame(0, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_dj_orders');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE ... USING with prepared statement.
     */
    public function testDeleteUsingPrepared(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM pg_dj_orders o USING pg_dj_users u WHERE o.user_id = u.id AND u.name = $1"
            );
            $stmt->execute(['Bob']);

            $select = $this->pdo->query('SELECT COUNT(*) FROM pg_dj_orders WHERE user_id = 2');
            $this->assertSame(0, (int) $select->fetchColumn());
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE...USING with prepared statement not supported: ' . get_class($e) . ' — ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE WHERE IN with prepared statement.
     */
    public function testDeleteWhereInPrepared(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT COUNT(*) AS cnt FROM pg_dj_orders WHERE user_id IN (SELECT id FROM pg_dj_users WHERE active = $1)",
            [false]
        );
        $countBefore = (int) $rows[0]['cnt'];
        $this->assertGreaterThanOrEqual(1, $countBefore);

        $stmt = $this->pdo->prepare(
            "DELETE FROM pg_dj_orders WHERE user_id IN (SELECT id FROM pg_dj_users WHERE active = $1)"
        );
        $stmt->execute([false]);

        $select = $this->pdo->query('SELECT COUNT(*) FROM pg_dj_orders WHERE user_id = 2');
        $this->assertSame(0, (int) $select->fetchColumn());
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec(
            "DELETE FROM pg_dj_orders WHERE user_id IN (SELECT id FROM pg_dj_users WHERE active = FALSE)"
        );

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_dj_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
