<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests common statement reuse patterns (ORM-style) with ZTD on PostgreSQL.
 * Focuses on patterns like prepare-once/execute-many, batch reads, and mixed workflows.
 * @spec pending
 */
class PostgresStatementReusePatternTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE reuse_users_pg (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(10), amount INT)';
    }

    protected function getTableNames(): array
    {
        return ['reuse_users_pg'];
    }


    public function testBatchInsertViaPreparedStatement(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO reuse_users_pg (id, name, category, amount) VALUES (?, ?, ?, ?)');

        $data = [
            [1, 'Alice', 'A', 100],
            [2, 'Bob', 'B', 200],
            [3, 'Charlie', 'A', 150],
            [4, 'Diana', 'B', 75],
            [5, 'Eve', 'C', 300],
        ];

        foreach ($data as $row) {
            $stmt->execute($row);
        }

        $count = $this->pdo->query('SELECT COUNT(*) FROM reuse_users_pg')->fetchColumn();
        $this->assertSame(5, (int) $count);
    }

    public function testPreparedSelectReuseWithDifferentParams(): void
    {
        $this->seedData();

        $stmt = $this->pdo->prepare('SELECT name FROM reuse_users_pg WHERE category = ?');

        $stmt->execute(['A']);
        $namesA = $stmt->fetchAll(PDO::FETCH_COLUMN);

        $stmt->execute(['B']);
        $namesB = $stmt->fetchAll(PDO::FETCH_COLUMN);

        $stmt->execute(['C']);
        $namesC = $stmt->fetchAll(PDO::FETCH_COLUMN);

        $this->assertCount(2, $namesA); // Alice, Charlie
        $this->assertCount(2, $namesB); // Bob, Diana
        $this->assertCount(1, $namesC); // Eve
    }

    public function testPreparedSelectWithBindValueReuse(): void
    {
        $this->seedData();

        $stmt = $this->pdo->prepare('SELECT SUM(amount) FROM reuse_users_pg WHERE category = :cat');

        $stmt->bindValue(':cat', 'A');
        $stmt->execute();
        $sumA = (int) $stmt->fetchColumn();

        $stmt->bindValue(':cat', 'B');
        $stmt->execute();
        $sumB = (int) $stmt->fetchColumn();

        $this->assertSame(250, $sumA); // 100 + 150
        $this->assertSame(275, $sumB); // 200 + 75
    }

    public function testPreparedSelectWithBindParamReuse(): void
    {
        $this->seedData();

        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM reuse_users_pg WHERE amount > :min');
        $min = 100;
        $stmt->bindParam(':min', $min);

        $stmt->execute();
        $count1 = (int) $stmt->fetchColumn();

        $min = 200;
        $stmt->execute();
        $count2 = (int) $stmt->fetchColumn();

        $this->assertSame(3, $count1); // 200, 150, 300
        $this->assertSame(1, $count2); // 300
    }

    public function testOrmStyleFindById(): void
    {
        $this->seedData();

        $findById = $this->pdo->prepare('SELECT id, name, category, amount FROM reuse_users_pg WHERE id = ?');

        // Simulating ORM find() calls
        $findById->execute([1]);
        $alice = $findById->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $alice['name']);

        $findById->execute([3]);
        $charlie = $findById->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Charlie', $charlie['name']);

        $findById->execute([999]);
        $notFound = $findById->fetch(PDO::FETCH_ASSOC);
        $this->assertFalse($notFound);
    }

    public function testOrmStyleFindAll(): void
    {
        $this->seedData();

        $findAll = $this->pdo->prepare('SELECT * FROM reuse_users_pg ORDER BY id LIMIT ? OFFSET ?');

        // Page 1
        $findAll->execute([2, 0]);
        $page1 = $findAll->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $page1);
        $this->assertSame('Alice', $page1[0]['name']);

        // Page 2
        $findAll->execute([2, 2]);
        $page2 = $findAll->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $page2);
        $this->assertSame('Charlie', $page2[0]['name']);

        // Page 3
        $findAll->execute([2, 4]);
        $page3 = $findAll->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $page3);
        $this->assertSame('Eve', $page3[0]['name']);
    }

    public function testExecInsertThenPreparedSelectWorkflow(): void
    {
        // ORM pattern: exec() for writes, prepare() for reads
        $this->pdo->exec("INSERT INTO reuse_users_pg VALUES (1, 'Alice', 'A', 100)");
        $this->pdo->exec("INSERT INTO reuse_users_pg VALUES (2, 'Bob', 'B', 200)");

        $stmt = $this->pdo->prepare('SELECT name FROM reuse_users_pg WHERE amount >= ?');
        $stmt->execute([150]);
        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);

        $this->assertSame(['Bob'], $names);
    }

    public function testExecUpdateThenPreparedSelectReflectsMutation(): void
    {
        $this->seedData();

        $this->pdo->exec("UPDATE reuse_users_pg SET amount = 999 WHERE category = 'A'");

        $stmt = $this->pdo->prepare('SELECT name, amount FROM reuse_users_pg WHERE category = ?');
        $stmt->execute(['A']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame(999, (int) $rows[0]['amount']);
        $this->assertSame(999, (int) $rows[1]['amount']);
    }

    public function testMultiplePreparedStatementsCoexist(): void
    {
        $this->seedData();

        $findByCategory = $this->pdo->prepare('SELECT name FROM reuse_users_pg WHERE category = ?');
        $findByAmount = $this->pdo->prepare('SELECT name FROM reuse_users_pg WHERE amount > ?');
        $countAll = $this->pdo->prepare('SELECT COUNT(*) FROM reuse_users_pg');

        // Use them interleaved
        $countAll->execute();
        $total = (int) $countAll->fetchColumn();
        $this->assertSame(5, $total);

        $findByCategory->execute(['A']);
        $catA = $findByCategory->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(2, $catA);

        $findByAmount->execute([200]);
        $highAmount = $findByAmount->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(1, $highAmount);
        $this->assertSame('Eve', $highAmount[0]);
    }

    private function seedData(): void
    {
        $this->pdo->exec("INSERT INTO reuse_users_pg VALUES (1, 'Alice', 'A', 100)");
        $this->pdo->exec("INSERT INTO reuse_users_pg VALUES (2, 'Bob', 'B', 200)");
        $this->pdo->exec("INSERT INTO reuse_users_pg VALUES (3, 'Charlie', 'A', 150)");
        $this->pdo->exec("INSERT INTO reuse_users_pg VALUES (4, 'Diana', 'B', 75)");
        $this->pdo->exec("INSERT INTO reuse_users_pg VALUES (5, 'Eve', 'C', 300)");
    }
}
