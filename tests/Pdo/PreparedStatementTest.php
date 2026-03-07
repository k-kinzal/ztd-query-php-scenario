<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

class PreparedStatementTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS prep_test');
        $raw->exec('CREATE TABLE prep_test (id INT PRIMARY KEY, name VARCHAR(255), score INT)');

        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
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
        $this->assertSame(100, (int) $rows[0]['score']);
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

    public function testPreparedInsertThenSelectWithFetch(): void
    {
        $this->pdo->exec("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO prep_test (id, name, score) VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->prepare('SELECT name, score FROM prep_test WHERE score > ? ORDER BY name');
        $stmt->execute([80]);

        // Use fetch() to iterate row by row
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
        $this->assertSame(1, (int) $obj->id);
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

    public function testPreparedUpdateRowCount(): void
    {
        $this->pdo->exec("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO prep_test (id, name, score) VALUES (2, 'Bob', 85)");
        $this->pdo->exec("INSERT INTO prep_test (id, name, score) VALUES (3, 'Charlie', 70)");

        $stmt = $this->pdo->prepare('UPDATE prep_test SET score = ? WHERE score < ?');
        $stmt->execute([0, 90]);

        $this->assertSame(2, $stmt->rowCount());
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

    public function testColumnCount(): void
    {
        $this->pdo->exec("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->query('SELECT id, name, score FROM prep_test');
        $this->assertSame(3, $stmt->columnCount());
    }

    public function testQueryRewrittenAtPrepareTime(): void
    {
        $this->pdo->exec("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");

        // Prepare with ZTD enabled - query is rewritten at prepare time
        $stmt = $this->pdo->prepare('SELECT * FROM prep_test WHERE id = ?');

        // Even if ZTD is disabled before execute, the prepared query still uses the CTE rewrite
        $this->pdo->disableZtd();
        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // The query was rewritten at prepare time, so shadow data is still visible
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);

        $this->pdo->enableZtd();
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS prep_test');
    }
}
