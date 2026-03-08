<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests CREATE TABLE IF NOT EXISTS via MySQLi.
 *
 * Cross-platform parity with MysqlCreateTableIfNotExistsTest (PDO).
 */
class CreateTableIfNotExistsTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_ctine_test');
        $raw->query('CREATE TABLE mi_ctine_test (id INT PRIMARY KEY, name VARCHAR(50))');
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
     * CREATE TABLE IF NOT EXISTS on existing table is no-op.
     */
    public function testCreateIfNotExistsOnExisting(): void
    {
        $this->mysqli->query("INSERT INTO mi_ctine_test VALUES (1, 'Alice')");

        $this->mysqli->query('CREATE TABLE IF NOT EXISTS mi_ctine_test (id INT PRIMARY KEY, name VARCHAR(50))');

        $result = $this->mysqli->query('SELECT name FROM mi_ctine_test WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    /**
     * CREATE TABLE IF NOT EXISTS creates new table.
     */
    public function testCreateIfNotExistsNew(): void
    {
        $this->mysqli->query('CREATE TABLE IF NOT EXISTS mi_ctine_new (id INT PRIMARY KEY, val VARCHAR(50))');
        $this->mysqli->query("INSERT INTO mi_ctine_new VALUES (1, 'test')");

        $result = $this->mysqli->query('SELECT val FROM mi_ctine_new WHERE id = 1');
        $this->assertSame('test', $result->fetch_assoc()['val']);
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
            $raw->query('DROP TABLE IF EXISTS mi_ctine_test');
            $raw->query('DROP TABLE IF EXISTS mi_ctine_new');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
