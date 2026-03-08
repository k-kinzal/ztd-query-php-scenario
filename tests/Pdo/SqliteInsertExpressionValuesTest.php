<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT with SQL expressions in VALUES clause on SQLite.
 *
 * When users write INSERT ... VALUES (1+1, UPPER('test'), ...), the
 * InsertTransformer converts VALUES to SELECT expressions for CTE shadowing.
 * This tests whether computed expressions in VALUES are correctly handled.
 * @spec pending
 */
class SqliteInsertExpressionValuesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE expr_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER, label TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['expr_test'];
    }


    /**
     * INSERT with arithmetic expression in VALUES.
     */
    public function testInsertWithArithmeticExpression(): void
    {
        $this->pdo->exec("INSERT INTO expr_test (id, name, score, label) VALUES (1, 'Alice', 40 + 50, 'computed')");

        $stmt = $this->pdo->query('SELECT score FROM expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(90, (int) $row['score']);
    }

    /**
     * INSERT with string function in VALUES.
     */
    public function testInsertWithStringFunction(): void
    {
        $this->pdo->exec("INSERT INTO expr_test (id, name, score, label) VALUES (1, UPPER('alice'), 90, 'upper')");

        $stmt = $this->pdo->query('SELECT name FROM expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('ALICE', $row['name']);
    }

    /**
     * INSERT with NULL expression in VALUES.
     */
    public function testInsertWithNullExpression(): void
    {
        $this->pdo->exec("INSERT INTO expr_test (id, name, score, label) VALUES (1, 'Alice', NULL, NULL)");

        $stmt = $this->pdo->query('SELECT score, label FROM expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['score']);
        $this->assertNull($row['label']);
    }

    /**
     * INSERT with COALESCE in VALUES.
     */
    public function testInsertWithCoalesceExpression(): void
    {
        $this->pdo->exec("INSERT INTO expr_test (id, name, score, label) VALUES (1, COALESCE(NULL, 'Fallback'), 90, 'coalesce')");

        $stmt = $this->pdo->query('SELECT name FROM expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Fallback', $row['name']);
    }

    /**
     * INSERT with CASE expression in VALUES.
     */
    public function testInsertWithCaseExpression(): void
    {
        $this->pdo->exec("INSERT INTO expr_test (id, name, score, label) VALUES (1, 'Alice', CASE WHEN 1 > 0 THEN 100 ELSE 0 END, 'case')");

        $stmt = $this->pdo->query('SELECT score FROM expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(100, (int) $row['score']);
    }

    /**
     * INSERT with concatenation in VALUES (SQLite uses ||).
     */
    public function testInsertWithConcatenation(): void
    {
        $this->pdo->exec("INSERT INTO expr_test (id, name, score, label) VALUES (1, 'Al' || 'ice', 90, 'concat')");

        $stmt = $this->pdo->query('SELECT name FROM expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * INSERT with LENGTH() in VALUES.
     */
    public function testInsertWithLengthFunction(): void
    {
        $this->pdo->exec("INSERT INTO expr_test (id, name, score, label) VALUES (1, 'Alice', LENGTH('hello world'), 'length')");

        $stmt = $this->pdo->query('SELECT score FROM expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(11, (int) $row['score']);
    }

    /**
     * INSERT with negative number in VALUES.
     */
    public function testInsertWithNegativeNumber(): void
    {
        $this->pdo->exec("INSERT INTO expr_test (id, name, score, label) VALUES (1, 'Alice', -10, 'negative')");

        $stmt = $this->pdo->query('SELECT score FROM expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(-10, (int) $row['score']);
    }

    /**
     * INSERT with ABS() in VALUES.
     */
    public function testInsertWithAbsFunction(): void
    {
        $this->pdo->exec("INSERT INTO expr_test (id, name, score, label) VALUES (1, 'Alice', ABS(-42), 'abs')");

        $stmt = $this->pdo->query('SELECT score FROM expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(42, (int) $row['score']);
    }

    /**
     * Physical isolation — expression-inserted data stays in shadow.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO expr_test (id, name, score, label) VALUES (1, UPPER('alice'), 40 + 50, 'test')");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM expr_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
