<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests DDL operations mid-session via MySQLi.
 *
 * Cross-platform parity with MysqlDdlMidSessionTest (PDO).
 */
class DdlMidSessionTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_ddl_ms');
        $raw->query('CREATE TABLE mi_ddl_ms (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
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

    public function testDropTableClearsShadowAndFallsToPhysical(): void
    {
        $this->mysqli->query("INSERT INTO mi_ddl_ms VALUES (1, 'Alice', 100)");

        $this->mysqli->query('DROP TABLE mi_ddl_ms');

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ddl_ms');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    public function testDropAndRecreateTableInShadow(): void
    {
        $this->mysqli->query("INSERT INTO mi_ddl_ms VALUES (1, 'Alice', 100)");
        $this->mysqli->query('DROP TABLE mi_ddl_ms');

        $this->mysqli->query('CREATE TABLE mi_ddl_ms (id INT PRIMARY KEY, name VARCHAR(50))');
        $this->mysqli->query("INSERT INTO mi_ddl_ms VALUES (1, 'NewAlice')");

        $result = $this->mysqli->query('SELECT name FROM mi_ddl_ms WHERE id = 1');
        $this->assertSame('NewAlice', $result->fetch_assoc()['name']);
    }

    public function testShadowDataPersistenceAcrossTableDrop(): void
    {
        $this->mysqli->query('CREATE TABLE mi_ddl_other (id INT PRIMARY KEY, tag VARCHAR(20))');
        $this->mysqli->query("INSERT INTO mi_ddl_other VALUES (1, 'important')");

        $this->mysqli->query("INSERT INTO mi_ddl_ms VALUES (1, 'Alice', 100)");
        $this->mysqli->query('DROP TABLE mi_ddl_ms');

        $result = $this->mysqli->query('SELECT tag FROM mi_ddl_other WHERE id = 1');
        $this->assertSame('important', $result->fetch_assoc()['tag']);
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
            $raw->query('DROP TABLE IF EXISTS mi_ddl_ms');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
