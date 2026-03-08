<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests common statement reuse patterns (ORM-style) with ZTD on MySQLi.
 * Focuses on patterns like prepare-once/execute-many, batch reads, and mixed workflows.
 * @spec SPEC-3.2
 */
class StatementReusePatternTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_reuse_users (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(10), amount INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_reuse_users'];
    }

    public function testBatchInsertViaPreparedStatement(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO mi_reuse_users (id, name, category, amount) VALUES (?, ?, ?, ?)');

        $data = [
            [1, 'Alice', 'A', 100],
            [2, 'Bob', 'B', 200],
            [3, 'Charlie', 'A', 150],
            [4, 'Diana', 'B', 75],
            [5, 'Eve', 'C', 300],
        ];

        foreach ($data as $row) {
            $id = $row[0];
            $name = $row[1];
            $cat = $row[2];
            $amount = $row[3];
            $stmt->bind_param('issi', $id, $name, $cat, $amount);
            $stmt->execute();
        }

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_reuse_users');
        $row = $result->fetch_assoc();
        $this->assertSame(5, (int) $row['cnt']);
    }

    public function testPreparedSelectReuseWithDifferentParams(): void
    {
        $this->seedData();

        $stmt = $this->mysqli->prepare('SELECT name FROM mi_reuse_users WHERE category = ?');

        $cat = 'A';
        $stmt->bind_param('s', $cat);
        $stmt->execute();
        $result = $stmt->get_result();
        $namesA = $result->fetch_all(MYSQLI_ASSOC);

        $cat = 'B';
        $stmt->bind_param('s', $cat);
        $stmt->execute();
        $result = $stmt->get_result();
        $namesB = $result->fetch_all(MYSQLI_ASSOC);

        $cat = 'C';
        $stmt->bind_param('s', $cat);
        $stmt->execute();
        $result = $stmt->get_result();
        $namesC = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $namesA); // Alice, Charlie
        $this->assertCount(2, $namesB); // Bob, Diana
        $this->assertCount(1, $namesC); // Eve
    }

    public function testOrmStyleFindById(): void
    {
        $this->seedData();

        $stmt = $this->mysqli->prepare('SELECT id, name, category, amount FROM mi_reuse_users WHERE id = ?');

        $id = 1;
        $stmt->bind_param('i', $id);
        $stmt->execute();
        $result = $stmt->get_result();
        $alice = $result->fetch_assoc();
        $this->assertSame('Alice', $alice['name']);

        $id = 3;
        $stmt->bind_param('i', $id);
        $stmt->execute();
        $result = $stmt->get_result();
        $charlie = $result->fetch_assoc();
        $this->assertSame('Charlie', $charlie['name']);

        $id = 999;
        $stmt->bind_param('i', $id);
        $stmt->execute();
        $result = $stmt->get_result();
        $notFound = $result->fetch_assoc();
        $this->assertNull($notFound);
    }

    public function testExecInsertThenPreparedSelectWorkflow(): void
    {
        $this->mysqli->query("INSERT INTO mi_reuse_users VALUES (1, 'Alice', 'A', 100)");
        $this->mysqli->query("INSERT INTO mi_reuse_users VALUES (2, 'Bob', 'B', 200)");

        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM mi_reuse_users WHERE amount >= ?',
            [150]
        );

        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testExecUpdateThenPreparedSelectReflectsMutation(): void
    {
        $this->seedData();

        $this->mysqli->query("UPDATE mi_reuse_users SET amount = 999 WHERE category = 'A'");

        $rows = $this->ztdPrepareAndExecute(
            'SELECT name, amount FROM mi_reuse_users WHERE category = ?',
            ['A']
        );

        $this->assertCount(2, $rows);
        $this->assertSame(999, (int) $rows[0]['amount']);
        $this->assertSame(999, (int) $rows[1]['amount']);
    }

    public function testMultiplePreparedStatementsCoexist(): void
    {
        $this->seedData();

        $findByCategory = $this->mysqli->prepare('SELECT name FROM mi_reuse_users WHERE category = ?');
        $findByAmount = $this->mysqli->prepare('SELECT name FROM mi_reuse_users WHERE amount > ?');

        // Use them interleaved
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_reuse_users');
        $total = (int) $result->fetch_assoc()['cnt'];
        $this->assertSame(5, $total);

        $cat = 'A';
        $findByCategory->bind_param('s', $cat);
        $findByCategory->execute();
        $catA = $findByCategory->get_result()->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $catA);

        $amount = 200;
        $findByAmount->bind_param('i', $amount);
        $findByAmount->execute();
        $highAmount = $findByAmount->get_result()->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $highAmount);
        $this->assertSame('Eve', $highAmount[0]['name']);
    }

    private function seedData(): void
    {
        $this->mysqli->query("INSERT INTO mi_reuse_users VALUES (1, 'Alice', 'A', 100)");
        $this->mysqli->query("INSERT INTO mi_reuse_users VALUES (2, 'Bob', 'B', 200)");
        $this->mysqli->query("INSERT INTO mi_reuse_users VALUES (3, 'Charlie', 'A', 150)");
        $this->mysqli->query("INSERT INTO mi_reuse_users VALUES (4, 'Diana', 'B', 75)");
        $this->mysqli->query("INSERT INTO mi_reuse_users VALUES (5, 'Eve', 'C', 300)");
    }
}
