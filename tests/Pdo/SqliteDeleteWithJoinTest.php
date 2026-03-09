<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DELETE with JOIN patterns on SQLite PDO.
 *
 * SQLite does not support DELETE ... JOIN or DELETE ... USING.
 * Cross-table deletion is done via:
 * 1. DELETE FROM t1 WHERE col IN (SELECT col FROM t2 WHERE condition)
 * 2. DELETE FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE ...)
 * 3. DELETE FROM t1 WHERE col = (SELECT col FROM t2 WHERE ...)
 *
 * These are the natural patterns SQLite users rely on for cross-table deletion.
 * @spec SPEC-4.2d
 */
class SqliteDeleteWithJoinTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE dj_users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER DEFAULT 1)',
            'CREATE TABLE dj_orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['dj_orders', 'dj_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO dj_users (id, name, active) VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO dj_users (id, name, active) VALUES (2, 'Bob', 0)");
        $this->pdo->exec("INSERT INTO dj_users (id, name, active) VALUES (3, 'Charlie', 1)");
        $this->pdo->exec("INSERT INTO dj_orders (id, user_id, amount) VALUES (1, 1, 100)");
        $this->pdo->exec("INSERT INTO dj_orders (id, user_id, amount) VALUES (2, 2, 200)");
        $this->pdo->exec("INSERT INTO dj_orders (id, user_id, amount) VALUES (3, 2, 50)");
        $this->pdo->exec("INSERT INTO dj_orders (id, user_id, amount) VALUES (4, 3, 300)");
    }

    /**
     * DELETE WHERE IN (subquery) — cross-table deletion.
     */
    public function testDeleteWhereInSubquery(): void
    {
        $this->pdo->exec(
            "DELETE FROM dj_orders WHERE user_id IN (SELECT id FROM dj_users WHERE active = 0)"
        );

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM dj_orders WHERE user_id = 2');
        $this->assertSame(0, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM dj_orders');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE WHERE EXISTS — correlated subquery deletion.
     */
    public function testDeleteWhereExists(): void
    {
        $this->pdo->exec(
            "DELETE FROM dj_orders WHERE EXISTS (SELECT 1 FROM dj_users WHERE dj_users.id = dj_orders.user_id AND dj_users.active = 0)"
        );

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM dj_orders WHERE user_id = 2');
        $this->assertSame(0, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM dj_orders');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE WHERE NOT EXISTS — keep only orders for existing active users.
     */
    public function testDeleteWhereNotExists(): void
    {
        $this->pdo->exec(
            "DELETE FROM dj_orders WHERE NOT EXISTS (SELECT 1 FROM dj_users WHERE dj_users.id = dj_orders.user_id AND dj_users.active = 1)"
        );

        // Bob (inactive) orders should be deleted
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM dj_orders WHERE user_id = 2');
        $this->assertSame(0, (int) $stmt->fetchColumn());

        // Alice and Charlie orders remain
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM dj_orders');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE WHERE IN with prepared statement.
     */
    public function testDeleteWhereInPrepared(): void
    {
        $stmt = $this->pdo->prepare(
            "DELETE FROM dj_orders WHERE user_id IN (SELECT id FROM dj_users WHERE name = ?)"
        );
        $stmt->execute(['Bob']);

        $select = $this->pdo->query('SELECT COUNT(*) FROM dj_orders WHERE user_id = 2');
        $this->assertSame(0, (int) $select->fetchColumn());
    }

    /**
     * DELETE WHERE EXISTS with prepared statement.
     */
    public function testDeleteWhereExistsPrepared(): void
    {
        $stmt = $this->pdo->prepare(
            "DELETE FROM dj_orders WHERE EXISTS (SELECT 1 FROM dj_users WHERE dj_users.id = dj_orders.user_id AND dj_users.active = ?)"
        );
        $stmt->execute([0]);

        $select = $this->pdo->query('SELECT COUNT(*) FROM dj_orders WHERE user_id = 2');
        $this->assertSame(0, (int) $select->fetchColumn());
    }

    /**
     * Verify row counts after chained DELETE operations.
     */
    public function testChainedDeleteOperations(): void
    {
        // Delete Bob's orders via subquery
        $this->pdo->exec(
            "DELETE FROM dj_orders WHERE user_id IN (SELECT id FROM dj_users WHERE active = 0)"
        );
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM dj_orders');
        $this->assertSame(2, (int) $stmt->fetchColumn());

        // Now delete Bob himself
        $this->pdo->exec("DELETE FROM dj_users WHERE active = 0");
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM dj_users');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec(
            "DELETE FROM dj_orders WHERE user_id IN (SELECT id FROM dj_users WHERE active = 0)"
        );

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM dj_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
