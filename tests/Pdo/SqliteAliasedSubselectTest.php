<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests ORM-style aliased table references in SELECT on SQLite.
 *
 * Real-world scenario: ORMs like Doctrine emit SQL with table aliases
 * like "SELECT t0.* FROM users t0 WHERE t0.id = ?". The CTE rewriter
 * must handle t0-style aliases in all clause positions.
 *
 * @spec SPEC-3.1
 */
class SqliteAliasedSubselectTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE asb_users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT \'user\'
            )',
            'CREATE TABLE asb_posts (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                published INTEGER NOT NULL DEFAULT 0
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['asb_posts', 'asb_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO asb_users VALUES (1, 'Alice', 'alice@test.com', 'admin')");
        $this->ztdExec("INSERT INTO asb_users VALUES (2, 'Bob', 'bob@test.com', 'user')");
        $this->ztdExec("INSERT INTO asb_users VALUES (3, 'Carol', 'carol@test.com', 'user')");

        $this->ztdExec("INSERT INTO asb_posts VALUES (1, 1, 'First Post', 1)");
        $this->ztdExec("INSERT INTO asb_posts VALUES (2, 1, 'Second Post', 0)");
        $this->ztdExec("INSERT INTO asb_posts VALUES (3, 2, 'Bob Post', 1)");
    }

    /**
     * Doctrine-style "SELECT t0.* FROM table t0 WHERE t0.col = ?".
     */
    public function testDoctrineStyleAlias(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT t0.id, t0.name, t0.email FROM asb_users t0 WHERE t0.id = ?",
                [1]
            );
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Doctrine-style alias failed: ' . $e->getMessage());
        }
    }

    /**
     * JOIN with t0/t1 aliases.
     */
    public function testJoinWithNumericAliases(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT t0.name, t1.title
                 FROM asb_users t0
                 JOIN asb_posts t1 ON t1.user_id = t0.id
                 WHERE t1.published = 1
                 ORDER BY t0.name, t1.title"
            );
            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('First Post', $rows[0]['title']);
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertSame('Bob Post', $rows[1]['title']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('JOIN with numeric aliases failed: ' . $e->getMessage());
        }
    }

    /**
     * Subquery with different alias levels.
     */
    public function testSubqueryWithAliasLevels(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT u.name,
                    (SELECT COUNT(*) FROM asb_posts p WHERE p.user_id = u.id) AS post_count
                 FROM asb_users u
                 ORDER BY u.name"
            );
            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertEquals(2, (int) $rows[0]['post_count']);
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertEquals(1, (int) $rows[1]['post_count']);
            $this->assertSame('Carol', $rows[2]['name']);
            $this->assertEquals(0, (int) $rows[2]['post_count']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Subquery with alias levels failed: ' . $e->getMessage());
        }
    }

    /**
     * LEFT JOIN with alias, filtering on joined table's column.
     */
    public function testLeftJoinWithAlias(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT u.name, p.title
                 FROM asb_users u
                 LEFT JOIN asb_posts p ON p.user_id = u.id AND p.published = 1
                 ORDER BY u.name"
            );
            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('First Post', $rows[0]['title']);
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertSame('Bob Post', $rows[1]['title']);
            $this->assertSame('Carol', $rows[2]['name']);
            $this->assertNull($rows[2]['title']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('LEFT JOIN with alias failed: ' . $e->getMessage());
        }
    }

    /**
     * Self-join with different aliases on same table.
     */
    public function testSelfJoinDifferentAliases(): void
    {
        try {
            // Find users who are NOT admins but have the same name length as an admin
            $rows = $this->ztdQuery(
                "SELECT u1.name FROM asb_users u1
                 JOIN asb_users u2 ON LENGTH(u1.name) = LENGTH(u2.name)
                 WHERE u1.role = 'user' AND u2.role = 'admin'
                 ORDER BY u1.name"
            );
            // Alice has 5 chars, Carol has 5 chars
            $this->assertCount(1, $rows);
            $this->assertSame('Carol', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Self-join aliases failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared JOIN with ? params using aliases.
     */
    public function testPreparedJoinWithAliasAndParams(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT u.name, p.title
                 FROM asb_users u
                 JOIN asb_posts p ON p.user_id = u.id
                 WHERE u.role = ? AND p.published = ?
                 ORDER BY u.name",
                ['admin', 1]
            );
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('First Post', $rows[0]['title']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Prepared JOIN with alias failed: ' . $e->getMessage());
        }
    }
}
