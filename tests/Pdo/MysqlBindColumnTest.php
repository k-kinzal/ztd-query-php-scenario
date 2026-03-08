<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests bindColumn() and FETCH_BOUND mode on MySQL ZTD PDO.
 * @spec pending
 */
class MysqlBindColumnTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE bc_mysql (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['bc_mysql'];
    }


    public function testBindColumnByNumber(): void
    {
        $this->pdo->exec("INSERT INTO bc_mysql VALUES (1, 'Alice', 100)");
        $stmt = $this->pdo->query('SELECT id, name, score FROM bc_mysql WHERE id = 1');
        $stmt->bindColumn(1, $id);
        $stmt->bindColumn(2, $name);
        $stmt->bindColumn(3, $score);
        $stmt->fetch(PDO::FETCH_BOUND);

        $this->assertSame(1, (int) $id);
        $this->assertSame('Alice', $name);
        $this->assertSame(100, (int) $score);
    }

    public function testBindColumnIteration(): void
    {
        $this->pdo->exec("INSERT INTO bc_mysql VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO bc_mysql VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->query('SELECT name FROM bc_mysql ORDER BY id');
        $stmt->bindColumn(1, $name);

        $names = [];
        while ($stmt->fetch(PDO::FETCH_BOUND)) {
            $names[] = $name;
        }
        $this->assertSame(['Alice', 'Bob'], $names);
    }

    public function testBindColumnWithPreparedStatement(): void
    {
        $this->pdo->exec("INSERT INTO bc_mysql VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO bc_mysql VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->prepare('SELECT name, score FROM bc_mysql WHERE score > ?');
        $stmt->execute([80]);
        $stmt->bindColumn('name', $name);

        $names = [];
        while ($stmt->fetch(PDO::FETCH_BOUND)) {
            $names[] = $name;
        }
        $this->assertCount(2, $names);
    }
}
