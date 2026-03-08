<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests exec() return values and rowCount() accuracy across various operations.
 * @spec SPEC-4.4
 */
class SqliteExecReturnValueTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE rv_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, active INT)';
    }

    protected function getTableNames(): array
    {
        return ['rv_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec('CREATE TABLE rv_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, active INT)');

        }

    public function testExecInsertReturnsSingleRowCount(): void
    {
        $count = $this->pdo->exec("INSERT INTO rv_test VALUES (1, 'Alice', 100, 1)");
        $this->assertSame(1, $count);
    }

    public function testExecMultiRowInsertReturnsTotal(): void
    {
        $count = $this->pdo->exec(
            "INSERT INTO rv_test VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 1), (3, 'Charlie', 70, 0)"
        );
        $this->assertSame(3, $count);
    }

    public function testExecUpdateReturnsMatchedCount(): void
    {
        $this->pdo->exec("INSERT INTO rv_test VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 1), (3, 'Charlie', 70, 0)");

        $count = $this->pdo->exec("UPDATE rv_test SET score = 999 WHERE active = 1");
        $this->assertSame(2, $count);
    }

    public function testExecUpdateNoMatchReturnsZero(): void
    {
        $this->pdo->exec("INSERT INTO rv_test VALUES (1, 'Alice', 100, 1)");

        $count = $this->pdo->exec("UPDATE rv_test SET score = 999 WHERE id = 999");
        $this->assertSame(0, $count);
    }

    public function testExecDeleteReturnsDeletedCount(): void
    {
        $this->pdo->exec("INSERT INTO rv_test VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 1), (3, 'Charlie', 70, 0)");

        $count = $this->pdo->exec("DELETE FROM rv_test WHERE active = 0");
        $this->assertSame(1, $count);
    }

    public function testExecDeleteAllReturnsTotal(): void
    {
        $this->pdo->exec("INSERT INTO rv_test VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 1)");

        $count = $this->pdo->exec("DELETE FROM rv_test WHERE 1=1");
        $this->assertSame(2, $count);
    }

    public function testRowCountAfterPreparedUpdate(): void
    {
        $this->pdo->exec("INSERT INTO rv_test VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 1), (3, 'Charlie', 70, 0)");

        $stmt = $this->pdo->prepare('UPDATE rv_test SET score = ? WHERE active = ?');
        $stmt->execute([999, 1]);

        $this->assertSame(2, $stmt->rowCount());
    }

    public function testRowCountAfterPreparedDelete(): void
    {
        $this->pdo->exec("INSERT INTO rv_test VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 1), (3, 'Charlie', 70, 0)");

        $stmt = $this->pdo->prepare('DELETE FROM rv_test WHERE score < ?');
        $stmt->execute([90]);

        $this->assertSame(2, $stmt->rowCount());
    }

    public function testRowCountAfterPreparedInsert(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO rv_test (id, name, score, active) VALUES (?, ?, ?, ?)');
        $stmt->execute([1, 'Alice', 100, 1]);

        $this->assertSame(1, $stmt->rowCount());
    }

    public function testSequentialExecReturnValues(): void
    {
        $c1 = $this->pdo->exec("INSERT INTO rv_test VALUES (1, 'Alice', 100, 1)");
        $c2 = $this->pdo->exec("INSERT INTO rv_test VALUES (2, 'Bob', 85, 1)");
        $c3 = $this->pdo->exec("UPDATE rv_test SET score = 50 WHERE id = 1");
        $c4 = $this->pdo->exec("DELETE FROM rv_test WHERE id = 2");
        $c5 = $this->pdo->exec("UPDATE rv_test SET score = 200 WHERE id = 999");

        $this->assertSame(1, $c1);
        $this->assertSame(1, $c2);
        $this->assertSame(1, $c3);
        $this->assertSame(1, $c4);
        $this->assertSame(0, $c5);
    }

    public function testExecReturnsCorrectAfterMixedOperations(): void
    {
        $this->pdo->exec("INSERT INTO rv_test VALUES (1, 'A', 10, 1), (2, 'B', 20, 1), (3, 'C', 30, 0)");
        $this->pdo->exec("UPDATE rv_test SET score = score + 100 WHERE active = 1");
        $this->pdo->exec("DELETE FROM rv_test WHERE id = 3");

        // After mutations, further operations should return correct counts
        $count = $this->pdo->exec("UPDATE rv_test SET active = 0 WHERE score > 100");
        $this->assertSame(2, $count);
    }
}
