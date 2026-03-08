<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests bindColumn() and FETCH_BOUND mode on PostgreSQL ZTD PDO.
 * @spec pending
 */
class PostgresBindColumnTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE bc_pg (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['bc_pg'];
    }


    public function testBindColumnByNumber(): void
    {
        $this->pdo->exec("INSERT INTO bc_pg VALUES (1, 'Alice', 100)");
        $stmt = $this->pdo->query('SELECT id, name, score FROM bc_pg WHERE id = 1');
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
        $this->pdo->exec("INSERT INTO bc_pg VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO bc_pg VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->query('SELECT name FROM bc_pg ORDER BY id');
        $stmt->bindColumn(1, $name);

        $names = [];
        while ($stmt->fetch(PDO::FETCH_BOUND)) {
            $names[] = $name;
        }
        $this->assertSame(['Alice', 'Bob'], $names);
    }

    public function testBindColumnWithPreparedStatement(): void
    {
        $this->pdo->exec("INSERT INTO bc_pg VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO bc_pg VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->prepare('SELECT name, score FROM bc_pg WHERE score > ?');
        $stmt->execute([80]);
        $stmt->bindColumn('name', $name);

        $names = [];
        while ($stmt->fetch(PDO::FETCH_BOUND)) {
            $names[] = $name;
        }
        $this->assertCount(2, $names);
    }
}
