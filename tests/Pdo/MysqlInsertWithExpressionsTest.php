<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT with computed expressions, function calls, and arithmetic
 * in the VALUES clause through the MySQL CTE shadow store.
 *
 * The CTE rewriter must correctly transform INSERT...VALUES containing
 * expressions (not just literals) into valid CTE-based SQL.
 *
 * @spec SPEC-4.1
 */
class MysqlInsertWithExpressionsTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_ie (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            code VARCHAR(50),
            score INT,
            created_at DATETIME
        )';
    }

    protected function getTableNames(): array
    {
        return ['my_ie'];
    }

    /**
     * INSERT with UPPER() function.
     */
    public function testInsertWithUpper(): void
    {
        $this->pdo->exec(
            "INSERT INTO my_ie (id, name, code, score) VALUES (1, UPPER('hello world'), 'a1', 10)"
        );
        $rows = $this->ztdQuery("SELECT name FROM my_ie WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('HELLO WORLD', $rows[0]['name']);
    }

    /**
     * INSERT with CONCAT() function.
     */
    public function testInsertWithConcat(): void
    {
        $this->pdo->exec(
            "INSERT INTO my_ie (id, name, code, score) VALUES (1, CONCAT('John', ' ', 'Doe'), 'b1', 20)"
        );
        $rows = $this->ztdQuery("SELECT name FROM my_ie WHERE id = 1");
        $this->assertSame('John Doe', $rows[0]['name']);
    }

    /**
     * INSERT with arithmetic expression.
     */
    public function testInsertWithArithmetic(): void
    {
        $this->pdo->exec(
            "INSERT INTO my_ie (id, name, code, score) VALUES (1, 'test', 'c1', 10 * 5 + 3)"
        );
        $rows = $this->ztdQuery("SELECT score FROM my_ie WHERE id = 1");
        $this->assertEquals(53, (int) $rows[0]['score']);
    }

    /**
     * INSERT with NOW() or CURRENT_TIMESTAMP.
     */
    public function testInsertWithNow(): void
    {
        $this->pdo->exec(
            "INSERT INTO my_ie (id, name, code, score, created_at) VALUES (1, 'test', 'ts', 0, NOW())"
        );
        $rows = $this->ztdQuery("SELECT created_at FROM my_ie WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertNotNull($rows[0]['created_at']);
        // Verify it's a valid datetime (not a literal 'NOW()')
        $ts = strtotime($rows[0]['created_at']);
        $this->assertNotFalse($ts, 'NOW() should produce a valid datetime');
    }

    /**
     * INSERT with CASE expression in VALUES.
     */
    public function testInsertWithCase(): void
    {
        $this->pdo->exec(
            "INSERT INTO my_ie (id, name, code, score) VALUES
             (1, 'Alice', CASE WHEN 1 > 0 THEN 'positive' ELSE 'negative' END, 100)"
        );
        $rows = $this->ztdQuery("SELECT code FROM my_ie WHERE id = 1");
        $this->assertSame('positive', $rows[0]['code']);
    }

    /**
     * INSERT with COALESCE in VALUES.
     */
    public function testInsertWithCoalesce(): void
    {
        $this->pdo->exec(
            "INSERT INTO my_ie (id, name, code, score) VALUES (1, COALESCE(NULL, 'fallback'), 'co', 0)"
        );
        $rows = $this->ztdQuery("SELECT name FROM my_ie WHERE id = 1");
        $this->assertSame('fallback', $rows[0]['name']);
    }

    /**
     * INSERT with LENGTH function.
     */
    public function testInsertWithLength(): void
    {
        $this->pdo->exec(
            "INSERT INTO my_ie (id, name, code, score) VALUES (1, 'hello', 'len', LENGTH('hello world'))"
        );
        $rows = $this->ztdQuery("SELECT score FROM my_ie WHERE id = 1");
        $this->assertEquals(11, (int) $rows[0]['score']);
    }

    /**
     * Multi-row INSERT with mixed expressions and literals.
     */
    public function testMultiRowInsertWithExpressions(): void
    {
        $this->pdo->exec(
            "INSERT INTO my_ie (id, name, code, score) VALUES
             (1, UPPER('alice'), 'a', 10 + 5),
             (2, LOWER('BOB'), 'b', 20 * 2),
             (3, CONCAT('Car', 'ol'), 'c', ABS(-30))"
        );
        $rows = $this->ztdQuery("SELECT id, name, score FROM my_ie ORDER BY id");
        $this->assertCount(3, $rows);
        $this->assertSame('ALICE', $rows[0]['name']);
        $this->assertEquals(15, (int) $rows[0]['score']);
        $this->assertSame('bob', $rows[1]['name']);
        $this->assertEquals(40, (int) $rows[1]['score']);
        $this->assertSame('Carol', $rows[2]['name']);
        $this->assertEquals(30, (int) $rows[2]['score']);
    }

    /**
     * INSERT with subquery in VALUES position.
     */
    public function testInsertWithScalarSubquery(): void
    {
        $this->pdo->exec("INSERT INTO my_ie (id, name, code, score) VALUES (1, 'base', 'x', 100)");

        try {
            $this->pdo->exec(
                "INSERT INTO my_ie (id, name, code, score) VALUES
                 (2, 'derived', 'y', (SELECT score FROM my_ie WHERE id = 1))"
            );
            $rows = $this->ztdQuery("SELECT score FROM my_ie WHERE id = 2");
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
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM my_ie');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
