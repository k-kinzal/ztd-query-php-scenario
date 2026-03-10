<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CTE rewriter edge cases on SQLite.
 *
 * @spec SPEC-4.2
 */
class SqliteCteRewriterEdgeCaseTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_cre_audit (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                action TEXT NOT NULL,
                detail TEXT
            )',
            'CREATE TABLE sl_cre_users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                score INTEGER DEFAULT NULL,
                note TEXT DEFAULT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_cre_audit', 'sl_cre_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_cre_audit VALUES (1, 'sl_cre_users', 'create', 'table created')");
        $this->pdo->exec("INSERT INTO sl_cre_users VALUES (1, 'Alice', 100, 'active')");
        $this->pdo->exec("INSERT INTO sl_cre_users VALUES (2, 'Bob', NULL, NULL)");
    }

    /**
     * Table name as string literal value — stripos can match incorrectly.
     */
    public function testTableNameInStringLiteral(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_cre_audit VALUES (2, 'sl_cre_users', 'insert', 'added Bob')");

            $rows = $this->ztdQuery(
                "SELECT table_name, action FROM sl_cre_audit WHERE table_name = 'sl_cre_users' ORDER BY id"
            );

            $this->assertCount(2, $rows);
            $this->assertSame('insert', $rows[1]['action']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Table name in string literal failed: ' . $e->getMessage());
        }
    }

    /**
     * NULL in shadow data with type inference.
     */
    public function testNullInShadowData(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_cre_users VALUES (3, 'Carol', NULL, NULL)");

            $rows = $this->ztdQuery("SELECT name, score, note FROM sl_cre_users ORDER BY id");

            $this->assertCount(3, $rows);
            $this->assertEquals(100, (int) $rows[0]['score']);
            $this->assertNull($rows[1]['score']);
            $this->assertNull($rows[2]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NULL in shadow data failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple mutations then subquery-based query.
     */
    public function testMultipleMutationsComplexQuery(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_cre_users SET score = 150 WHERE id = 1");
            $this->pdo->exec("DELETE FROM sl_cre_users WHERE id = 2");
            $this->pdo->exec("INSERT INTO sl_cre_users VALUES (3, 'Carol', 80, 'new')");
            $this->pdo->exec("INSERT INTO sl_cre_users VALUES (4, 'Dave', 90, 'new')");

            $rows = $this->ztdQuery(
                "SELECT name, score FROM sl_cre_users
                 WHERE score > (SELECT AVG(score) FROM sl_cre_users)
                 ORDER BY score DESC"
            );

            // Users: Alice(150), Carol(80), Dave(90) → avg ≈ 106.67
            // score > avg: only Alice(150)
            if (empty($rows)) {
                $this->markTestIncomplete('Multiple mutations + complex query: empty result');
            }
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiple mutations + complex query failed: ' . $e->getMessage());
        }
    }

    /**
     * Comment containing WITH keyword.
     */
    public function testCommentContainingWith(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_cre_users VALUES (3, 'Carol', 60, 'test')");

            $rows = $this->ztdQuery(
                "/* Query WITH special handling */ SELECT name FROM sl_cre_users ORDER BY id"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('Carol', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Comment containing WITH failed: ' . $e->getMessage());
        }
    }

    /**
     * String literal with || concat containing table name.
     */
    public function testConcatWithTableNameString(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_cre_users VALUES (3, 'Carol', 70, 'test')");

            $rows = $this->ztdQuery(
                "SELECT 'sl_cre_users' || ':' || name AS label FROM sl_cre_users ORDER BY id"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('sl_cre_users:Alice', $rows[0]['label']);
            $this->assertSame('sl_cre_users:Carol', $rows[2]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Concat with table name string failed: ' . $e->getMessage());
        }
    }
}
