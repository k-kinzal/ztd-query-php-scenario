<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests ALTER TABLE ADD COLUMN behavior with shadow store via MySQLi.
 *
 * Cross-platform parity with MysqlAlterTableAfterDataTest (PDO).
 */
class AlterTableAfterDataTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_evolve');
        $raw->query('CREATE TABLE mi_evolve (id INT PRIMARY KEY, name VARCHAR(50))');
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

    /**
     * On MySQL, SELECT with new column works because physical table has it.
     */
    public function testSelectNewColumnWorksOnMysql(): void
    {
        $this->mysqli->query("INSERT INTO mi_evolve VALUES (1, 'Alice')");
        $this->mysqli->query('ALTER TABLE mi_evolve ADD COLUMN score INT');

        $result = $this->mysqli->query('SELECT name, score FROM mi_evolve WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
    }

    public function testInsertWithNewColumnSucceeds(): void
    {
        $this->mysqli->query("INSERT INTO mi_evolve VALUES (1, 'Alice')");
        $this->mysqli->query('ALTER TABLE mi_evolve ADD COLUMN score INT');

        $this->mysqli->query("INSERT INTO mi_evolve (id, name, score) VALUES (2, 'Bob', 100)");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_evolve');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    public function testOriginalColumnsStillWorkAfterAlter(): void
    {
        $this->mysqli->query("INSERT INTO mi_evolve VALUES (1, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_evolve VALUES (2, 'Bob')");
        $this->mysqli->query('ALTER TABLE mi_evolve ADD COLUMN score INT');

        $result = $this->mysqli->query('SELECT name FROM mi_evolve WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    protected function tearDown(): void
    {
        if (isset($this->mysqli)) {
            $this->mysqli->close();
        }
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new \mysqli(
                MySQLContainer::getHost(),
                'root',
                'root',
                'test',
                MySQLContainer::getPort(),
            );
            $raw->query('DROP TABLE IF EXISTS mi_evolve');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
