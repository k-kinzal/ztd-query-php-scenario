<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Scenarios\PreparedStatementScenario;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests prepared statement patterns in ZTD mode on MySQL via PDO.
 * @spec SPEC-3.2
 */
class MysqlPreparedStatementTest extends AbstractMysqlPdoTestCase
{
    use PreparedStatementScenario;

    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE prep_test (id INT PRIMARY KEY, name VARCHAR(255), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['prep_test'];
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

    public function testExecuteWithNamedParameterArray(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO prep_test (id, name, score) VALUES (:id, :name, :score)');
        $stmt->execute([':id' => 1, ':name' => 'Bob', ':score' => 85]);

        $stmt = $this->pdo->query('SELECT * FROM prep_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testFetchNum(): void
    {
        $this->pdo->exec("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->query('SELECT id, name, score FROM prep_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_NUM);

        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0][0]);
        $this->assertSame('Alice', $rows[0][1]);
    }

    public function testFetchBoth(): void
    {
        $this->pdo->exec("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->query('SELECT id, name FROM prep_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_BOTH);

        // FETCH_BOTH returns both numeric and associative keys
        $this->assertSame(1, (int) $row['id']);
        $this->assertSame(1, (int) $row[0]);
        $this->assertSame('Alice', $row['name']);
        $this->assertSame('Alice', $row[1]);
    }

    public function testPreparedDeleteRowCount(): void
    {
        $this->pdo->exec("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO prep_test (id, name, score) VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->prepare('DELETE FROM prep_test WHERE id = ?');
        $stmt->execute([1]);

        $this->assertSame(1, $stmt->rowCount());

        // Verify deletion in shadow store
        $stmt = $this->pdo->query('SELECT * FROM prep_test');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testQueryRewrittenAtPrepareTime(): void
    {
        $this->pdo->exec("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->prepare('SELECT * FROM prep_test WHERE id = ?');

        $this->pdo->disableZtd();
        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);

        $this->pdo->enableZtd();
    }
}
