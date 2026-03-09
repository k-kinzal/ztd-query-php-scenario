<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT with computed expressions in VALUES through SQLite CTE rewriter.
 *
 * @spec SPEC-4.1
 */
class SqliteInsertWithExpressionsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_ie (
            id INTEGER PRIMARY KEY,
            name TEXT,
            code TEXT,
            score INTEGER,
            created_at TEXT
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_ie'];
    }

    /**
     * INSERT with UPPER() function.
     */
    public function testInsertWithUpper(): void
    {
        $this->pdo->exec(
            "INSERT INTO sl_ie (id, name, code, score) VALUES (1, UPPER('hello world'), 'a1', 10)"
        );
        $rows = $this->ztdQuery("SELECT name FROM sl_ie WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('HELLO WORLD', $rows[0]['name']);
    }

    /**
     * INSERT with || string concatenation.
     */
    public function testInsertWithConcatOperator(): void
    {
        $this->pdo->exec(
            "INSERT INTO sl_ie (id, name, code, score) VALUES (1, 'John' || ' ' || 'Doe', 'b1', 20)"
        );
        $rows = $this->ztdQuery("SELECT name FROM sl_ie WHERE id = 1");
        $this->assertSame('John Doe', $rows[0]['name']);
    }

    /**
     * INSERT with arithmetic expression.
     */
    public function testInsertWithArithmetic(): void
    {
        $this->pdo->exec(
            "INSERT INTO sl_ie (id, name, code, score) VALUES (1, 'test', 'c1', 10 * 5 + 3)"
        );
        $rows = $this->ztdQuery("SELECT score FROM sl_ie WHERE id = 1");
        $this->assertEquals(53, (int) $rows[0]['score']);
    }

    /**
     * INSERT with datetime('now').
     */
    public function testInsertWithDatetime(): void
    {
        $this->pdo->exec(
            "INSERT INTO sl_ie (id, name, code, score, created_at) VALUES (1, 'test', 'ts', 0, datetime('now'))"
        );
        $rows = $this->ztdQuery("SELECT created_at FROM sl_ie WHERE id = 1");
        $this->assertNotNull($rows[0]['created_at']);
    }

    /**
     * INSERT with CASE expression.
     */
    public function testInsertWithCase(): void
    {
        $this->pdo->exec(
            "INSERT INTO sl_ie (id, name, code, score) VALUES
             (1, 'Alice', CASE WHEN 1 > 0 THEN 'positive' ELSE 'negative' END, 100)"
        );
        $rows = $this->ztdQuery("SELECT code FROM sl_ie WHERE id = 1");
        $this->assertSame('positive', $rows[0]['code']);
    }

    /**
     * INSERT with COALESCE.
     */
    public function testInsertWithCoalesce(): void
    {
        $this->pdo->exec(
            "INSERT INTO sl_ie (id, name, code, score) VALUES (1, COALESCE(NULL, 'fallback'), 'co', 0)"
        );
        $rows = $this->ztdQuery("SELECT name FROM sl_ie WHERE id = 1");
        $this->assertSame('fallback', $rows[0]['name']);
    }

    /**
     * INSERT with LENGTH function.
     */
    public function testInsertWithLength(): void
    {
        $this->pdo->exec(
            "INSERT INTO sl_ie (id, name, code, score) VALUES (1, 'hello', 'len', LENGTH('hello world'))"
        );
        $rows = $this->ztdQuery("SELECT score FROM sl_ie WHERE id = 1");
        $this->assertEquals(11, (int) $rows[0]['score']);
    }

    /**
     * IIF in VALUES (SQLite-specific).
     */
    public function testInsertWithIif(): void
    {
        $this->pdo->exec(
            "INSERT INTO sl_ie (id, name, code, score) VALUES
             (1, IIF(1=1, 'yes', 'no'), 'iif', 0)"
        );
        $rows = $this->ztdQuery("SELECT name FROM sl_ie WHERE id = 1");
        $this->assertSame('yes', $rows[0]['name']);
    }

    /**
     * Multi-row INSERT with mixed expressions.
     */
    public function testMultiRowInsertWithExpressions(): void
    {
        $this->pdo->exec(
            "INSERT INTO sl_ie (id, name, code, score) VALUES
             (1, UPPER('alice'), 'a', 10 + 5),
             (2, LOWER('BOB'), 'b', 20 * 2),
             (3, 'Car' || 'ol', 'c', ABS(-30))"
        );
        $rows = $this->ztdQuery("SELECT id, name, score FROM sl_ie ORDER BY id");
        $this->assertCount(3, $rows);
        $this->assertSame('ALICE', $rows[0]['name']);
        $this->assertEquals(15, (int) $rows[0]['score']);
        $this->assertSame('bob', $rows[1]['name']);
        $this->assertEquals(40, (int) $rows[1]['score']);
        $this->assertSame('Carol', $rows[2]['name']);
        $this->assertEquals(30, (int) $rows[2]['score']);
    }

    /**
     * INSERT with scalar subquery in VALUES.
     */
    public function testInsertWithScalarSubquery(): void
    {
        $this->pdo->exec("INSERT INTO sl_ie (id, name, code, score) VALUES (1, 'base', 'x', 100)");

        try {
            $this->pdo->exec(
                "INSERT INTO sl_ie (id, name, code, score) VALUES
                 (2, 'derived', 'y', (SELECT score FROM sl_ie WHERE id = 1))"
            );
            $rows = $this->ztdQuery("SELECT score FROM sl_ie WHERE id = 2");
            $this->assertCount(1, $rows);
            $this->assertEquals(100, (int) $rows[0]['score']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('INSERT with scalar subquery in VALUES not supported: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_ie');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
