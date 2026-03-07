<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

class SqlitePreparedStatementTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE prep_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)');

        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    public function testBindParamWithByReference(): void
    {
        $this->pdo->exec("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->prepare('SELECT * FROM prep_test WHERE id = :id');
        $id = 1;
        $stmt->bindParam(':id', $id, PDO::PARAM_INT);
        $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testExecuteWithPositionalParameterArray(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO prep_test (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([1, 'Alice', 100]);

        $stmt = $this->pdo->query('SELECT * FROM prep_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testFetch(): void
    {
        $this->pdo->exec("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO prep_test (id, name, score) VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->query('SELECT name FROM prep_test ORDER BY id');

        $row1 = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row1['name']);

        $row2 = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Bob', $row2['name']);

        $row3 = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertFalse($row3);
    }

    public function testFetchColumn(): void
    {
        $this->pdo->exec("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->query('SELECT id, name, score FROM prep_test WHERE id = 1');
        $name = $stmt->fetchColumn(1);

        $this->assertSame('Alice', $name);
    }

    public function testFetchObject(): void
    {
        $this->pdo->exec("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->query('SELECT * FROM prep_test WHERE id = 1');
        $obj = $stmt->fetchObject();

        $this->assertIsObject($obj);
        $this->assertSame('Alice', $obj->name);
    }

    public function testPreparedUpdateRowCount(): void
    {
        $this->pdo->exec("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO prep_test (id, name, score) VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->prepare('UPDATE prep_test SET score = ? WHERE score < ?');
        $stmt->execute([0, 95]);

        $this->assertSame(1, $stmt->rowCount());
    }

    public function testReExecutePreparedStatement(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO prep_test (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([1, 'Alice', 100]);
        $stmt->execute([2, 'Bob', 85]);

        $stmt = $this->pdo->query('SELECT * FROM prep_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }
}
