<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT with SQL expressions in VALUES clause on MySQL.
 *
 * The InsertTransformer converts VALUES to SELECT expressions for CTE.
 * This tests whether computed expressions survive the transformation.
 * @spec pending
 */
class MysqlInsertExpressionValuesTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE expr_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, label VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['expr_test'];
    }


    /**
     * INSERT with arithmetic expression.
     */
    public function testInsertWithArithmeticExpression(): void
    {
        $this->pdo->exec("INSERT INTO expr_test (id, name, score, label) VALUES (1, 'Alice', 40 + 50, 'computed')");

        $stmt = $this->pdo->query('SELECT score FROM expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(90, (int) $row['score']);
    }

    /**
     * INSERT with UPPER() function.
     */
    public function testInsertWithUpperFunction(): void
    {
        $this->pdo->exec("INSERT INTO expr_test (id, name, score, label) VALUES (1, UPPER('alice'), 90, 'upper')");

        $stmt = $this->pdo->query('SELECT name FROM expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('ALICE', $row['name']);
    }

    /**
     * INSERT with CONCAT() function (MySQL-specific).
     */
    public function testInsertWithConcatFunction(): void
    {
        $this->pdo->exec("INSERT INTO expr_test (id, name, score, label) VALUES (1, CONCAT('Al', 'ice'), 90, 'concat')");

        $stmt = $this->pdo->query('SELECT name FROM expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * INSERT with IF() function (MySQL-specific).
     */
    public function testInsertWithIfFunction(): void
    {
        $this->pdo->exec("INSERT INTO expr_test (id, name, score, label) VALUES (1, 'Alice', IF(1 > 0, 100, 0), 'if')");

        $stmt = $this->pdo->query('SELECT score FROM expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(100, (int) $row['score']);
    }

    /**
     * INSERT with COALESCE expression.
     */
    public function testInsertWithCoalesceExpression(): void
    {
        $this->pdo->exec("INSERT INTO expr_test (id, name, score, label) VALUES (1, COALESCE(NULL, 'Fallback'), 90, 'coalesce')");

        $stmt = $this->pdo->query('SELECT name FROM expr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Fallback', $row['name']);
    }

    /**
     * INSERT with negative and zero values.
     */
    public function testInsertWithNegativeAndZero(): void
    {
        $this->pdo->exec("INSERT INTO expr_test (id, name, score, label) VALUES (1, 'Zero', 0, 'zero')");
        $this->pdo->exec("INSERT INTO expr_test (id, name, score, label) VALUES (2, 'Negative', -10, 'neg')");

        $stmt = $this->pdo->query('SELECT score FROM expr_test WHERE id = 1');
        $this->assertSame(0, (int) $stmt->fetch(PDO::FETCH_ASSOC)['score']);

        $stmt = $this->pdo->query('SELECT score FROM expr_test WHERE id = 2');
        $this->assertSame(-10, (int) $stmt->fetch(PDO::FETCH_ASSOC)['score']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO expr_test (id, name, score, label) VALUES (1, UPPER('alice'), 40 + 50, 'test')");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM expr_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
