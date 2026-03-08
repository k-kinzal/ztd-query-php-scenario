<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests DELETE without WHERE clause via MySQLi.
 *
 * Cross-platform parity with MysqlDeleteWithoutWhereTest (PDO).
 */
class DeleteWithoutWhereTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_dww_test');
        $raw->query('CREATE TABLE mi_dww_test (id INT PRIMARY KEY, name VARCHAR(50))');
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

        $this->mysqli->query("INSERT INTO mi_dww_test VALUES (1, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_dww_test VALUES (2, 'Bob')");
        $this->mysqli->query("INSERT INTO mi_dww_test VALUES (3, 'Charlie')");
    }

    /**
     * DELETE without WHERE works correctly on MySQL.
     */
    public function testDeleteWithoutWhereWorks(): void
    {
        $this->mysqli->query('DELETE FROM mi_dww_test');

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_dww_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * DELETE with WHERE 1=1 also works.
     */
    public function testDeleteWithWhereTrueWorks(): void
    {
        $this->mysqli->query('DELETE FROM mi_dww_test WHERE 1=1');

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_dww_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query('DELETE FROM mi_dww_test');

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_dww_test');
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
            $raw->query('DROP TABLE IF EXISTS mi_dww_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
