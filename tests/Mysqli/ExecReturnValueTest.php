<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests lastAffectedRows() accuracy across various operations on MySQLi ZTD.
 */
class ExecReturnValueTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_rv');
        $raw->query('CREATE TABLE mi_rv (id INT PRIMARY KEY, name VARCHAR(50), score INT, active INT)');
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
    }

    public function testLastAffectedRowsAfterInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_rv VALUES (1, 'Alice', 100, 1)");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());
    }

    public function testLastAffectedRowsAfterMultiRowInsert(): void
    {
        $this->mysqli->query(
            "INSERT INTO mi_rv VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 1), (3, 'Charlie', 70, 0)"
        );
        $this->assertSame(3, $this->mysqli->lastAffectedRows());
    }

    public function testLastAffectedRowsAfterUpdate(): void
    {
        $this->mysqli->query("INSERT INTO mi_rv VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 1)");
        $this->mysqli->query("UPDATE mi_rv SET score = 999 WHERE active = 1");
        $this->assertSame(2, $this->mysqli->lastAffectedRows());
    }

    public function testLastAffectedRowsAfterDelete(): void
    {
        $this->mysqli->query("INSERT INTO mi_rv VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 0)");
        $this->mysqli->query("DELETE FROM mi_rv WHERE active = 0");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());
    }

    public function testLastAffectedRowsNoMatch(): void
    {
        $this->mysqli->query("INSERT INTO mi_rv VALUES (1, 'Alice', 100, 1)");
        $this->mysqli->query("UPDATE mi_rv SET score = 999 WHERE id = 999");
        $this->assertSame(0, $this->mysqli->lastAffectedRows());
    }

    public function testZtdAffectedRowsOnPreparedUpdate(): void
    {
        $this->mysqli->query("INSERT INTO mi_rv VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 1)");

        $stmt = $this->mysqli->prepare('UPDATE mi_rv SET score = ? WHERE active = ?');
        $score = 999;
        $active = 1;
        $stmt->bind_param('ii', $score, $active);
        $stmt->execute();

        $this->assertSame(2, $stmt->ztdAffectedRows());
    }

    public function testZtdAffectedRowsOnPreparedDelete(): void
    {
        $this->mysqli->query("INSERT INTO mi_rv VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 0)");

        $stmt = $this->mysqli->prepare('DELETE FROM mi_rv WHERE active = ?');
        $active = 0;
        $stmt->bind_param('i', $active);
        $stmt->execute();

        $this->assertSame(1, $stmt->ztdAffectedRows());
    }

    public function testSequentialAffectedRows(): void
    {
        $this->mysqli->query("INSERT INTO mi_rv VALUES (1, 'A', 10, 1)");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        $this->mysqli->query("INSERT INTO mi_rv VALUES (2, 'B', 20, 1)");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        $this->mysqli->query("UPDATE mi_rv SET score = 999 WHERE active = 1");
        $this->assertSame(2, $this->mysqli->lastAffectedRows());

        $this->mysqli->query("DELETE FROM mi_rv WHERE id = 1");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());
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
        $raw->query('DROP TABLE IF EXISTS mi_rv');
        $raw->close();
    }
}
