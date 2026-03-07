<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

class PreparedStatementTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS prep_test');
        $raw->query('CREATE TABLE prep_test (id INT PRIMARY KEY, name VARCHAR(255), score INT)');
        $raw->close();

        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
    }

    public function testPreparedInsertAndSelect(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO prep_test (id, name, score) VALUES (?, ?, ?)');
        $id = 1;
        $name = 'Alice';
        $score = 100;
        $stmt->bind_param('isi', $id, $name, $score);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT * FROM prep_test WHERE id = 1');
        $row = $result->fetch_assoc();

        $this->assertSame('Alice', $row['name']);
        $this->assertSame(100, (int) $row['score']);
    }

    public function testPreparedSelectWithGetResult(): void
    {
        $this->mysqli->query("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->mysqli->prepare('SELECT * FROM prep_test WHERE id = ?');
        $id = 1;
        $stmt->bind_param('i', $id);
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();

        $this->assertSame('Alice', $row['name']);
    }

    public function testPreparedUpdateAffectedRows(): void
    {
        $this->mysqli->query("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO prep_test (id, name, score) VALUES (2, 'Bob', 85)");
        $this->mysqli->query("INSERT INTO prep_test (id, name, score) VALUES (3, 'Charlie', 70)");

        $stmt = $this->mysqli->prepare('UPDATE prep_test SET score = ? WHERE score < ?');
        $newScore = 0;
        $threshold = 90;
        $stmt->bind_param('ii', $newScore, $threshold);
        $stmt->execute();

        // Use ztdAffectedRows() for ZTD-aware affected rows
        $this->assertSame(2, $stmt->ztdAffectedRows());
    }

    public function testPreparedDeleteAffectedRows(): void
    {
        $this->mysqli->query("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO prep_test (id, name, score) VALUES (2, 'Bob', 85)");

        $stmt = $this->mysqli->prepare('DELETE FROM prep_test WHERE id = ?');
        $id = 1;
        $stmt->bind_param('i', $id);
        $stmt->execute();

        $this->assertSame(1, $stmt->ztdAffectedRows());

        // Verify deletion in shadow store
        $result = $this->mysqli->query('SELECT * FROM prep_test');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testReExecutePreparedStatement(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO prep_test (id, name, score) VALUES (?, ?, ?)');
        $id = 0;
        $name = '';
        $score = 0;
        $stmt->bind_param('isi', $id, $name, $score);

        $id = 1;
        $name = 'Alice';
        $score = 100;
        $stmt->execute();

        $id = 2;
        $name = 'Bob';
        $score = 85;
        $stmt->execute();

        $result = $this->mysqli->query('SELECT * FROM prep_test ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testExecuteQueryMethod(): void
    {
        $this->mysqli->query("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");

        // execute_query is available in PHP 8.2+
        if (!method_exists($this->mysqli, 'execute_query')) {
            $this->markTestSkipped('execute_query requires PHP 8.2+');
        }

        $result = $this->mysqli->execute_query(
            'SELECT * FROM prep_test WHERE id = ?',
            [1]
        );
        $row = $result->fetch_assoc();

        $this->assertSame('Alice', $row['name']);
    }

    public function testExecuteQueryInsert(): void
    {
        if (!method_exists($this->mysqli, 'execute_query')) {
            $this->markTestSkipped('execute_query requires PHP 8.2+');
        }

        $this->mysqli->execute_query(
            'INSERT INTO prep_test (id, name, score) VALUES (?, ?, ?)',
            [1, 'Alice', 100]
        );

        $result = $this->mysqli->query('SELECT * FROM prep_test WHERE id = 1');
        $row = $result->fetch_assoc();

        $this->assertSame('Alice', $row['name']);
    }

    public function testQueryRewrittenAtPrepareTime(): void
    {
        $this->mysqli->query("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");

        // Prepare with ZTD enabled - query is rewritten at prepare time
        $stmt = $this->mysqli->prepare('SELECT * FROM prep_test WHERE id = ?');
        $id = 1;
        $stmt->bind_param('i', $id);

        // Even if ZTD is disabled before execute, the prepared query still uses the CTE rewrite
        $this->mysqli->disableZtd();
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        // The query was rewritten at prepare time, so shadow data is still visible
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);

        $this->mysqli->enableZtd();
    }

    protected function tearDown(): void
    {
        $this->mysqli->close();
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS prep_test');
        $raw->close();
    }
}
