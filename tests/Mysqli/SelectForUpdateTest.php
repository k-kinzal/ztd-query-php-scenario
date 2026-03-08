<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests SELECT...FOR UPDATE locking clause via MySQLi.
 *
 * Cross-platform parity with MysqlSelectForUpdateTest (PDO).
 */
class SelectForUpdateTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_fu_test');
        $raw->query('CREATE TABLE mi_fu_test (id INT PRIMARY KEY, name VARCHAR(50), balance INT)');
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

        $this->mysqli->query("INSERT INTO mi_fu_test VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO mi_fu_test VALUES (2, 'Bob', 200)");
    }

    /**
     * SELECT...FOR UPDATE returns correct shadow data.
     */
    public function testSelectForUpdateReturnsData(): void
    {
        $result = $this->mysqli->query('SELECT name, balance FROM mi_fu_test WHERE id = 1 FOR UPDATE');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(100, (int) $row['balance']);
    }

    /**
     * SELECT...FOR SHARE also works.
     */
    public function testSelectForShareReturnsData(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_fu_test WHERE id = 2 FOR SHARE');
        $this->assertSame('Bob', $result->fetch_assoc()['name']);
    }

    /**
     * Physical isolation maintained.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_fu_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
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
            $raw->query('DROP TABLE IF EXISTS mi_fu_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
