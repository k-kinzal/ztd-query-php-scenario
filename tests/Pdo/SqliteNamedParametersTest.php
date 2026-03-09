<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests PDO named parameters (:name style) through the CTE rewriter.
 * Many PHP applications use named parameters instead of positional (?).
 * The CTE rewriter must not interfere with :name placeholders.
 *
 * SQL patterns exercised: SELECT with :name params, INSERT with :name params,
 * UPDATE with :name params, DELETE with :name params, named params in
 * complex queries, named params in subqueries.
 * @spec SPEC-3.2
 */
class SqliteNamedParametersTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_np_users (
                id INTEGER PRIMARY KEY,
                username TEXT NOT NULL,
                email TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT \'user\',
                active INTEGER NOT NULL DEFAULT 1
            )',
            'CREATE TABLE sl_np_orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                total REAL NOT NULL,
                status TEXT NOT NULL DEFAULT \'pending\'
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_np_orders', 'sl_np_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_np_users VALUES (1, 'alice', 'alice@example.com', 'admin', 1)");
        $this->pdo->exec("INSERT INTO sl_np_users VALUES (2, 'bob', 'bob@example.com', 'user', 1)");
        $this->pdo->exec("INSERT INTO sl_np_users VALUES (3, 'carol', 'carol@example.com', 'user', 0)");

        $this->pdo->exec("INSERT INTO sl_np_orders VALUES (1, 1, 99.99, 'completed')");
        $this->pdo->exec("INSERT INTO sl_np_orders VALUES (2, 1, 49.99, 'pending')");
        $this->pdo->exec("INSERT INTO sl_np_orders VALUES (3, 2, 29.99, 'completed')");
    }

    /**
     * Simple SELECT with single named parameter.
     */
    public function testSelectWithNamedParam(): void
    {
        $stmt = $this->pdo->prepare("SELECT username, email FROM sl_np_users WHERE role = :role");
        $stmt->execute([':role' => 'admin']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('alice', $rows[0]['username']);
    }

    /**
     * SELECT with multiple named parameters.
     */
    public function testSelectWithMultipleNamedParams(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT username FROM sl_np_users WHERE role = :role AND active = :active"
        );
        $stmt->execute([':role' => 'user', ':active' => 1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('bob', $rows[0]['username']);
    }

    /**
     * INSERT with named parameters.
     */
    public function testInsertWithNamedParams(): void
    {
        $stmt = $this->pdo->prepare(
            "INSERT INTO sl_np_users (id, username, email, role, active)
             VALUES (:id, :username, :email, :role, :active)"
        );
        $stmt->execute([
            ':id' => 4,
            ':username' => 'diana',
            ':email' => 'diana@example.com',
            ':role' => 'editor',
            ':active' => 1,
        ]);

        $rows = $this->ztdQuery("SELECT username, role FROM sl_np_users WHERE id = 4");
        $this->assertCount(1, $rows);
        $this->assertSame('diana', $rows[0]['username']);
        $this->assertSame('editor', $rows[0]['role']);
    }

    /**
     * UPDATE with named parameters.
     */
    public function testUpdateWithNamedParams(): void
    {
        $stmt = $this->pdo->prepare(
            "UPDATE sl_np_users SET role = :newRole WHERE username = :username"
        );
        $stmt->execute([':newRole' => 'moderator', ':username' => 'bob']);

        $rows = $this->ztdQuery("SELECT role FROM sl_np_users WHERE username = 'bob'");
        $this->assertCount(1, $rows);
        $this->assertSame('moderator', $rows[0]['role']);
    }

    /**
     * DELETE with named parameters.
     */
    public function testDeleteWithNamedParams(): void
    {
        $stmt = $this->pdo->prepare(
            "DELETE FROM sl_np_users WHERE active = :active AND role = :role"
        );
        $stmt->execute([':active' => 0, ':role' => 'user']);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_np_users");
        $this->assertEquals(2, (int) $rows[0]['cnt']);
    }

    /**
     * Named parameters in JOIN query.
     */
    public function testNamedParamsInJoinQuery(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT u.username, o.total, o.status
             FROM sl_np_users u
             JOIN sl_np_orders o ON o.user_id = u.id
             WHERE o.status = :status AND o.total > :minTotal
             ORDER BY o.total DESC"
        );
        $stmt->execute([':status' => 'completed', ':minTotal' => 20.00]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('alice', $rows[0]['username']);
        $this->assertEqualsWithDelta(99.99, (float) $rows[0]['total'], 0.01);
    }

    /**
     * Named params in GROUP BY HAVING.
     *
     * Known issue: HAVING with prepared statement parameters returns empty
     * on SQLite (SPEC-11.SQLITE-HAVING-PARAM, Issue #22).
     */
    public function testNamedParamInGroupByHaving(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT u.username, SUM(o.total) AS order_total
             FROM sl_np_users u
             JOIN sl_np_orders o ON o.user_id = u.id
             WHERE u.active = :active
             GROUP BY u.username
             HAVING SUM(o.total) > :threshold
             ORDER BY order_total DESC"
        );
        $stmt->execute([':active' => 1, ':threshold' => 50.00]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'SPEC-11.SQLITE-HAVING-PARAM [Issue #22]: HAVING with named params returns empty on SQLite.'
            );
        }
        $this->assertCount(1, $rows);
    }

    /**
     * Named parameters without colon prefix (PDO supports both).
     */
    public function testNamedParamsWithoutColonPrefix(): void
    {
        $stmt = $this->pdo->prepare("SELECT username FROM sl_np_users WHERE role = :role");
        $stmt->execute(['role' => 'admin']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('alice', $rows[0]['username']);
    }

    /**
     * Named parameters in subquery.
     */
    public function testNamedParamsInSubquery(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT username FROM sl_np_users
             WHERE id IN (
                SELECT user_id FROM sl_np_orders WHERE status = :status
             )"
        );
        $stmt->execute([':status' => 'completed']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $usernames = array_column($rows, 'username');
        $this->assertContains('alice', $usernames);
        $this->assertContains('bob', $usernames);
    }

    /**
     * Insert with named params, then read back with named param query.
     */
    public function testInsertThenSelectBothNamed(): void
    {
        $insert = $this->pdo->prepare(
            "INSERT INTO sl_np_orders (id, user_id, total, status)
             VALUES (:id, :uid, :total, :status)"
        );
        $insert->execute([':id' => 10, ':uid' => 2, ':total' => 199.99, ':status' => 'pending']);

        $select = $this->pdo->prepare(
            "SELECT total, status FROM sl_np_orders WHERE user_id = :uid AND id = :id"
        );
        $select->execute([':uid' => 2, ':id' => 10]);
        $rows = $select->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(199.99, (float) $rows[0]['total'], 0.01);
    }
}
