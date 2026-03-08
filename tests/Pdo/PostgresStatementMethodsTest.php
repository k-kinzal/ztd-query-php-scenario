<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/** @spec SPEC-4.12 */
class PostgresStatementMethodsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_stmt_test (id INT PRIMARY KEY, name VARCHAR(255), amount DECIMAL(10,2))';
    }

    protected function getTableNames(): array
    {
        return ['pg_stmt_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_stmt_test (id, name, amount) VALUES (1, 'Alice', 100.50)");
        $this->pdo->exec("INSERT INTO pg_stmt_test (id, name, amount) VALUES (2, 'Bob', 200.75)");
    }

    public function testSetFetchModeOnStatement(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM pg_stmt_test WHERE id = :id');
        $stmt->setFetchMode(PDO::FETCH_ASSOC);
        $stmt->bindValue(':id', 1);
        $stmt->execute();

        $row = $stmt->fetch();
        $this->assertIsArray($row);
        $this->assertArrayHasKey('name', $row);
        $this->assertArrayNotHasKey(0, $row);
    }

    public function testCloseCursorAllowsReExecution(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM pg_stmt_test WHERE id = :id');

        $stmt->execute([':id' => 1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $stmt->closeCursor();

        $stmt->execute([':id' => 2]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Bob', $row['name']);
    }

    public function testBindColumnBindsResultToVariable(): void
    {
        $stmt = $this->pdo->prepare('SELECT id, name FROM pg_stmt_test WHERE id = ?');
        $stmt->execute([1]);

        $id = null;
        $name = null;
        $stmt->bindColumn(1, $id);
        $stmt->bindColumn(2, $name);
        $stmt->fetch(PDO::FETCH_BOUND);

        $this->assertSame(1, (int) $id);
        $this->assertSame('Alice', $name);
    }

    public function testColumnCountReturnsCorrectCount(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, amount FROM pg_stmt_test WHERE id = 1');
        $this->assertSame(3, $stmt->columnCount());
    }

    public function testStatementIterator(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM pg_stmt_test ORDER BY id');
        $rows = [];
        foreach ($stmt as $row) {
            $rows[] = $row;
        }
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testQueryWithFetchMode(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM pg_stmt_test ORDER BY id', PDO::FETCH_NUM);
        $row = $stmt->fetch();
        $this->assertIsArray($row);
        $this->assertArrayHasKey(0, $row);
        $this->assertArrayHasKey(1, $row);
    }
}
