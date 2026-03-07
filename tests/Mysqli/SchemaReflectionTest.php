<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;
use ZtdQuery\Adapter\Mysqli\ZtdMysqliException;

class SchemaReflectionTest extends TestCase
{
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
        $raw->query('DROP TABLE IF EXISTS reflect_test');
        $raw->close();
    }

    public function testAdapterConstructedAfterTableReflectsSchema(): void
    {
        // Create table first
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('CREATE TABLE reflect_test (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->close();

        // Adapter constructed AFTER table exists → schema reflected
        $ztd = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

        // INSERT, UPDATE, DELETE all work correctly
        $ztd->query("INSERT INTO reflect_test (id, val) VALUES (1, 'original')");
        $ztd->query("UPDATE reflect_test SET val = 'updated' WHERE id = 1");

        $result = $ztd->query('SELECT val FROM reflect_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('updated', $row['val']);

        $ztd->query("DELETE FROM reflect_test WHERE id = 1");
        $result = $ztd->query('SELECT * FROM reflect_test');
        $this->assertSame(0, $result->num_rows);

        $ztd->close();
    }

    public function testUpdateFailsWhenSchemaNotReflected(): void
    {
        // Construct adapter BEFORE table exists → schema NOT reflected
        $ztd = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

        // Create table after adapter
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('CREATE TABLE reflect_test (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->query("INSERT INTO reflect_test VALUES (1, 'physical')");
        $raw->close();

        // INSERT works (doesn't need primary key info)
        $ztd->query("INSERT INTO reflect_test (id, val) VALUES (2, 'shadow')");

        // UPDATE fails because schema was not reflected (requires primary keys)
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/requires primary keys/i');
        $ztd->query("UPDATE reflect_test SET val = 'updated' WHERE id = 1");

        $ztd->close();
    }

    protected function tearDown(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS reflect_test');
        $raw->close();
    }
}
