<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests exec() return values and rowCount() accuracy on MySQL ZTD PDO.
 * @spec SPEC-4.4
 */
class MysqlExecReturnValueTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE rv_mysql (id INT PRIMARY KEY, name VARCHAR(50), score INT, active INT)';
    }

    protected function getTableNames(): array
    {
        return ['rv_mysql'];
    }


    public function testExecInsertReturnsOne(): void
    {
        $count = $this->pdo->exec("INSERT INTO rv_mysql VALUES (1, 'Alice', 100, 1)");
        $this->assertSame(1, $count);
    }

    public function testExecMultiRowInsertReturnsTotal(): void
    {
        $count = $this->pdo->exec(
            "INSERT INTO rv_mysql VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 1), (3, 'Charlie', 70, 0)"
        );
        $this->assertSame(3, $count);
    }

    public function testExecUpdateReturnsMatchedCount(): void
    {
        $this->pdo->exec("INSERT INTO rv_mysql VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 1)");
        $count = $this->pdo->exec("UPDATE rv_mysql SET score = 999 WHERE active = 1");
        $this->assertSame(2, $count);
    }

    public function testExecDeleteReturnsCount(): void
    {
        $this->pdo->exec("INSERT INTO rv_mysql VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 0)");
        $count = $this->pdo->exec("DELETE FROM rv_mysql WHERE active = 0");
        $this->assertSame(1, $count);
    }

    public function testRowCountAfterPreparedUpdate(): void
    {
        $this->pdo->exec("INSERT INTO rv_mysql VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 1)");
        $stmt = $this->pdo->prepare('UPDATE rv_mysql SET score = ? WHERE active = ?');
        $stmt->execute([999, 1]);
        $this->assertSame(2, $stmt->rowCount());
    }
}
