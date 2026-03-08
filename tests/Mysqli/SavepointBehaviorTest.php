<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests SAVEPOINT behavior via MySQLi.
 *
 * Cross-platform parity with MysqlSavepointBehaviorTest (PDO).
 */
class SavepointBehaviorTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_sp_test');
        $raw->query('CREATE TABLE mi_sp_test (id INT PRIMARY KEY, name VARCHAR(50))');
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
        $this->mysqli->query("INSERT INTO mi_sp_test VALUES (1, 'Alice')");
    }

    /**
     * SAVEPOINT throws on MySQL.
     */
    public function testSavepointThrows(): void
    {
        $this->expectException(\Throwable::class);
        $this->mysqli->query('SAVEPOINT sp1');
    }

    /**
     * ROLLBACK TO SAVEPOINT throws on MySQL.
     */
    public function testRollbackToSavepointThrows(): void
    {
        $this->expectException(\Throwable::class);
        $this->mysqli->query('ROLLBACK TO SAVEPOINT sp1');
    }

    /**
     * Shadow data unaffected by failed SAVEPOINT.
     */
    public function testShadowDataUnaffected(): void
    {
        try {
            $this->mysqli->query('SAVEPOINT sp1');
        } catch (\Throwable $e) {
            // Expected
        }

        $result = $this->mysqli->query('SELECT name FROM mi_sp_test WHERE id = 1');
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
            $raw->query('DROP TABLE IF EXISTS mi_sp_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
