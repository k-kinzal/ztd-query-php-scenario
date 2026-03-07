<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;
use ZtdQuery\Adapter\Mysqli\ZtdMysqliException;

class DdlOperationsTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS ddl_existing');
        $raw->query('CREATE TABLE ddl_existing (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->query('DROP TABLE IF EXISTS ddl_new');
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

    public function testCreateTableThrowsWhenTableExistsPhysically(): void
    {
        $this->expectException(ZtdMysqliException::class);
        $this->expectExceptionMessageMatches('/already exists/i');

        $this->mysqli->query('CREATE TABLE ddl_existing (id INT PRIMARY KEY)');
    }

    public function testCreateTableInShadowWhenNotPhysical(): void
    {
        $this->mysqli->query('CREATE TABLE ddl_new (id INT PRIMARY KEY, name VARCHAR(255))');

        $this->mysqli->query("INSERT INTO ddl_new (id, name) VALUES (1, 'shadow')");

        $result = $this->mysqli->query('SELECT * FROM ddl_new WHERE id = 1');
        $row = $result->fetch_assoc();

        $this->assertSame(1, (int) $row['id']);
        $this->assertSame('shadow', $row['name']);
    }

    public function testDropTableClearsShadowData(): void
    {
        $this->mysqli->query("INSERT INTO ddl_existing (id, val) VALUES (1, 'shadow')");

        $result = $this->mysqli->query('SELECT * FROM ddl_existing WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('shadow', $row['val']);

        $this->mysqli->query('DROP TABLE ddl_existing');

        // After DROP, shadow data is cleared; query falls through to physical table
        $result = $this->mysqli->query('SELECT * FROM ddl_existing WHERE id = 1');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(0, $rows);
    }

    public function testTruncateClearsShadowedData(): void
    {
        $this->mysqli->query("INSERT INTO ddl_existing (id, val) VALUES (1, 'a')");
        $this->mysqli->query("INSERT INTO ddl_existing (id, val) VALUES (2, 'b')");

        $result = $this->mysqli->query('SELECT * FROM ddl_existing');
        $this->assertSame(2, $result->num_rows);

        $this->mysqli->query('TRUNCATE TABLE ddl_existing');

        $result = $this->mysqli->query('SELECT * FROM ddl_existing');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(0, $rows);
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
        $raw->query('DROP TABLE IF EXISTS ddl_existing');
        $raw->query('DROP TABLE IF EXISTS ddl_new');
        $raw->close();
    }
}
