<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT with SQL expressions in VALUES clause on PostgreSQL.
 *
 * The InsertTransformer converts VALUES to SELECT expressions for CTE.
 * PostgreSQL-specific functions (||, INITCAP, etc.) are tested.
 * @spec SPEC-4.1
 */
class PostgresInsertExpressionValuesTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_expr_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, label VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['pg_expr_test'];
    }


    /**
     * INSERT with arithmetic expression.
     */
    public function testInsertWithArithmeticExpression(): void
    {
        $this->pdo->exec("INSERT INTO pg_expr_test (id, name, score, label) VALUES (1, 'Alice', 40 + 50, 'computed')");

        $stmt = $this->pdo->query('SELECT score FROM pg_expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(90, (int) $row['score']);
    }

    /**
     * INSERT with UPPER() function.
     */
    public function testInsertWithUpperFunction(): void
    {
        $this->pdo->exec("INSERT INTO pg_expr_test (id, name, score, label) VALUES (1, UPPER('alice'), 90, 'upper')");

        $stmt = $this->pdo->query('SELECT name FROM pg_expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('ALICE', $row['name']);
    }

    /**
     * INSERT with || concatenation (PostgreSQL).
     */
    public function testInsertWithConcatenation(): void
    {
        $this->pdo->exec("INSERT INTO pg_expr_test (id, name, score, label) VALUES (1, 'Al' || 'ice', 90, 'concat')");

        $stmt = $this->pdo->query('SELECT name FROM pg_expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * INSERT with COALESCE expression.
     */
    public function testInsertWithCoalesceExpression(): void
    {
        $this->pdo->exec("INSERT INTO pg_expr_test (id, name, score, label) VALUES (1, COALESCE(NULL, 'Fallback'), 90, 'coalesce')");

        $stmt = $this->pdo->query('SELECT name FROM pg_expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Fallback', $row['name']);
    }

    /**
     * INSERT with CASE expression.
     */
    public function testInsertWithCaseExpression(): void
    {
        $this->pdo->exec("INSERT INTO pg_expr_test (id, name, score, label) VALUES (1, 'Alice', CASE WHEN 1 > 0 THEN 100 ELSE 0 END, 'case')");

        $stmt = $this->pdo->query('SELECT score FROM pg_expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(100, (int) $row['score']);
    }

    /**
     * INSERT with GREATEST() function (PostgreSQL-specific).
     */
    public function testInsertWithGreatestFunction(): void
    {
        $this->pdo->exec("INSERT INTO pg_expr_test (id, name, score, label) VALUES (1, 'Alice', GREATEST(10, 20, 30), 'greatest')");

        $stmt = $this->pdo->query('SELECT score FROM pg_expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(30, (int) $row['score']);
    }

    /**
     * INSERT with negative value.
     */
    public function testInsertWithNegativeValue(): void
    {
        $this->pdo->exec("INSERT INTO pg_expr_test (id, name, score, label) VALUES (1, 'Alice', -42, 'neg')");

        $stmt = $this->pdo->query('SELECT score FROM pg_expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(-42, (int) $row['score']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_expr_test (id, name, score, label) VALUES (1, UPPER('alice'), 40 + 50, 'test')");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_expr_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
