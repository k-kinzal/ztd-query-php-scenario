<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests anti-join patterns (LEFT JOIN ... IS NULL, NOT EXISTS, NOT IN)
 * through SQLite CTE shadow store, comparing equivalence.
 *
 * These three patterns should return identical results. The CTE rewriter
 * may handle them differently, revealing inconsistencies.
 */
class SqliteAntiJoinNullPatternTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ajn_users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            )',
            'CREATE TABLE sl_ajn_logins (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                login_date TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ajn_logins', 'sl_ajn_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ajn_users VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO sl_ajn_users VALUES (2, 'Bob', 1)");
        $this->pdo->exec("INSERT INTO sl_ajn_users VALUES (3, 'Carol', 1)");
        $this->pdo->exec("INSERT INTO sl_ajn_users VALUES (4, 'Dave', 0)");

        // Only Alice and Carol have logins
        $this->pdo->exec("INSERT INTO sl_ajn_logins VALUES (1, 1, '2025-01-01')");
        $this->pdo->exec("INSERT INTO sl_ajn_logins VALUES (2, 1, '2025-01-02')");
        $this->pdo->exec("INSERT INTO sl_ajn_logins VALUES (3, 3, '2025-01-01')");
    }

    /**
     * Anti-join via LEFT JOIN ... IS NULL.
     * Users who have never logged in.
     */
    public function testAntiJoinLeftJoinIsNull(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name
             FROM sl_ajn_users u
             LEFT JOIN sl_ajn_logins l ON u.id = l.user_id
             WHERE l.id IS NULL
             ORDER BY u.name"
        );

        $names = array_column($rows, 'name');
        $this->assertCount(2, $names);
        $this->assertEquals(['Bob', 'Dave'], $names);
    }

    /**
     * Anti-join via NOT EXISTS.
     */
    public function testAntiJoinNotExists(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name
             FROM sl_ajn_users u
             WHERE NOT EXISTS (
                 SELECT 1 FROM sl_ajn_logins l WHERE l.user_id = u.id
             )
             ORDER BY u.name"
        );

        $names = array_column($rows, 'name');
        $this->assertCount(2, $names);
        $this->assertEquals(['Bob', 'Dave'], $names);
    }

    /**
     * Anti-join via NOT IN.
     */
    public function testAntiJoinNotIn(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name
             FROM sl_ajn_users u
             WHERE u.id NOT IN (
                 SELECT l.user_id FROM sl_ajn_logins l
             )
             ORDER BY u.name"
        );

        $names = array_column($rows, 'name');
        $this->assertCount(2, $names);
        $this->assertEquals(['Bob', 'Dave'], $names);
    }

    /**
     * All three anti-join patterns should return identical results.
     */
    public function testAntiJoinPatternsEquivalence(): void
    {
        $leftJoinResult = $this->ztdQuery(
            "SELECT u.id FROM sl_ajn_users u
             LEFT JOIN sl_ajn_logins l ON u.id = l.user_id
             WHERE l.id IS NULL ORDER BY u.id"
        );

        $notExistsResult = $this->ztdQuery(
            "SELECT u.id FROM sl_ajn_users u
             WHERE NOT EXISTS (SELECT 1 FROM sl_ajn_logins l WHERE l.user_id = u.id)
             ORDER BY u.id"
        );

        $notInResult = $this->ztdQuery(
            "SELECT u.id FROM sl_ajn_users u
             WHERE u.id NOT IN (SELECT l.user_id FROM sl_ajn_logins l)
             ORDER BY u.id"
        );

        $leftJoinIds = array_column($leftJoinResult, 'id');
        $notExistsIds = array_column($notExistsResult, 'id');
        $notInIds = array_column($notInResult, 'id');

        $this->assertEquals($leftJoinIds, $notExistsIds,
            'LEFT JOIN IS NULL and NOT EXISTS should return same results');
        $this->assertEquals($leftJoinIds, $notInIds,
            'LEFT JOIN IS NULL and NOT IN should return same results');
    }

    /**
     * Anti-join with additional filter — active users without logins.
     */
    public function testAntiJoinWithFilter(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name
             FROM sl_ajn_users u
             LEFT JOIN sl_ajn_logins l ON u.id = l.user_id
             WHERE l.id IS NULL AND u.active = 1
             ORDER BY u.name"
        );

        $names = array_column($rows, 'name');
        $this->assertCount(1, $names);
        $this->assertEquals(['Bob'], $names);
    }

    /**
     * Anti-join with prepared statement.
     */
    public function testAntiJoinPrepared(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT u.name
             FROM sl_ajn_users u
             WHERE NOT EXISTS (
                 SELECT 1 FROM sl_ajn_logins l
                 WHERE l.user_id = u.id AND l.login_date >= ?
             )
             ORDER BY u.name",
            ['2025-01-02']
        );

        // Only Alice has a login >= 2025-01-02
        $names = array_column($rows, 'name');
        $this->assertContains('Bob', $names);
        $this->assertContains('Carol', $names);
        $this->assertContains('Dave', $names);
    }

    /**
     * DELETE using anti-join pattern — remove users who never logged in.
     */
    public function testDeleteWithAntiJoinPattern(): void
    {
        $this->pdo->exec(
            "DELETE FROM sl_ajn_users
             WHERE id NOT IN (
                 SELECT DISTINCT user_id FROM sl_ajn_logins
             )"
        );

        $rows = $this->ztdQuery("SELECT name FROM sl_ajn_users ORDER BY name");
        $names = array_column($rows, 'name');
        $this->assertCount(2, $names);
        $this->assertEquals(['Alice', 'Carol'], $names);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM sl_ajn_users')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
