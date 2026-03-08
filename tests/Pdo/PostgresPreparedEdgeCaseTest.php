<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests prepared statement edge cases in ZTD mode on PostgreSQL via PDO:
 * parameter types, NULL binding, re-execution, named params.
 * @spec pending
 */
class PostgresPreparedEdgeCaseTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_prep_edge (id INT PRIMARY KEY, name VARCHAR(255), score INT, active BOOLEAN)';
    }

    protected function getTableNames(): array
    {
        return ['pg_prep_edge'];
    }


    public function testBindParamNullType(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_prep_edge (id, name, score) VALUES (?, ?, ?)');
        $stmt->bindValue(1, 1, PDO::PARAM_INT);
        $stmt->bindValue(2, 'Alice', PDO::PARAM_STR);
        $stmt->bindValue(3, null, PDO::PARAM_NULL);
        $stmt->execute();

        $stmt = $this->pdo->query('SELECT * FROM pg_prep_edge WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertNull($row['score']);
    }

    public function testReExecuteWithDifferentParams(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_prep_edge (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([1, 'Alice', 100]);
        $stmt->execute([2, 'Bob', 85]);
        $stmt->execute([3, 'Charlie', 70]);

        $stmt = $this->pdo->query('SELECT COUNT(*) as cnt FROM pg_prep_edge');
        $this->assertSame(3, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    public function testNamedParamsInsert(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_prep_edge (id, name, score) VALUES (:id, :name, :score)');
        $stmt->execute([':id' => 1, ':name' => 'Alice', ':score' => 100]);

        $stmt = $this->pdo->query('SELECT * FROM pg_prep_edge WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    public function testPreparedSelectWithMultipleConditions(): void
    {
        // Avoid boolean false due to known PostgreSQL CTE casting issue (#6)
        $this->pdo->exec("INSERT INTO pg_prep_edge (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO pg_prep_edge (id, name, score) VALUES (2, 'Bob', 85)");
        $this->pdo->exec("INSERT INTO pg_prep_edge (id, name, score) VALUES (3, 'Charlie', 70)");

        $stmt = $this->pdo->prepare('SELECT name FROM pg_prep_edge WHERE score >= ? AND score <= ? ORDER BY name');
        $stmt->execute([80, 100]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testPreparedUpdate(): void
    {
        $this->pdo->exec("INSERT INTO pg_prep_edge (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->prepare('UPDATE pg_prep_edge SET score = ? WHERE id = ?');
        $stmt->execute([95, 1]);
        $this->assertSame(1, $stmt->rowCount());

        $stmt = $this->pdo->query('SELECT score FROM pg_prep_edge WHERE id = 1');
        $this->assertSame(95, (int) $stmt->fetch(PDO::FETCH_ASSOC)['score']);
    }

    public function testPreparedDelete(): void
    {
        $this->pdo->exec("INSERT INTO pg_prep_edge (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO pg_prep_edge (id, name, score) VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->prepare('DELETE FROM pg_prep_edge WHERE id = ?');
        $stmt->execute([1]);
        $this->assertSame(1, $stmt->rowCount());

        $stmt = $this->pdo->query('SELECT COUNT(*) as c FROM pg_prep_edge');
        $this->assertSame(1, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);
    }

    public function testPreparedInsertThenSelectReusePrepared(): void
    {
        $insert = $this->pdo->prepare('INSERT INTO pg_prep_edge (id, name, score) VALUES (?, ?, ?)');
        $insert->execute([1, 'Alice', 100]);
        $insert->execute([2, 'Bob', 85]);

        $select = $this->pdo->prepare('SELECT name FROM pg_prep_edge WHERE id = ?');

        $select->execute([1]);
        $this->assertSame('Alice', $select->fetch(PDO::FETCH_ASSOC)['name']);

        $select->execute([2]);
        $this->assertSame('Bob', $select->fetch(PDO::FETCH_ASSOC)['name']);
    }
}
