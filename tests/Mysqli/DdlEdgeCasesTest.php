<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;
use ZtdQuery\Adapter\Mysqli\ZtdMysqliException;

/**
 * Tests DDL edge cases on MySQL via MySQLi adapter:
 * CREATE TABLE IF NOT EXISTS, DROP TABLE IF EXISTS, TRUNCATE isolation.
 */
class DdlEdgeCasesTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mysqli_ddl_edge');
        $raw->query('CREATE TABLE mysqli_ddl_edge (id INT PRIMARY KEY, val VARCHAR(255))');
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

    public function testCreateTableIfNotExistsOnExistingTable(): void
    {
        // Should NOT throw because IF NOT EXISTS is specified
        $this->mysqli->query('CREATE TABLE IF NOT EXISTS mysqli_ddl_edge (id INT PRIMARY KEY, val VARCHAR(255))');

        $this->mysqli->query("INSERT INTO mysqli_ddl_edge (id, val) VALUES (1, 'test')");
        $result = $this->mysqli->query('SELECT * FROM mysqli_ddl_edge WHERE id = 1');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
    }

    public function testCreateTableWithoutIfNotExistsThrows(): void
    {
        $this->expectException(ZtdMysqliException::class);
        $this->expectExceptionMessageMatches('/already exists/i');
        $this->mysqli->query('CREATE TABLE mysqli_ddl_edge (id INT PRIMARY KEY)');
    }

    public function testDropTableIfExistsOnNonExistent(): void
    {
        // Should NOT throw because IF EXISTS is specified
        $this->mysqli->query('DROP TABLE IF EXISTS mysqli_nonexistent_ddl_table');
        $this->assertTrue(true);
    }

    public function testTruncateIsolation(): void
    {
        // Insert into shadow
        $this->mysqli->query("INSERT INTO mysqli_ddl_edge (id, val) VALUES (1, 'shadow_data')");

        // Truncate clears shadow data
        $this->mysqli->query('TRUNCATE TABLE mysqli_ddl_edge');

        $result = $this->mysqli->query('SELECT * FROM mysqli_ddl_edge');
        $this->assertCount(0, $result->fetch_all(MYSQLI_ASSOC));

        // Physical table should still exist and be empty (it was already empty)
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT * FROM mysqli_ddl_edge');
        $this->assertCount(0, $result->fetch_all(MYSQLI_ASSOC));
    }

    public function testTruncateThenInsert(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_ddl_edge (id, val) VALUES (1, 'before')");
        $this->mysqli->query('TRUNCATE TABLE mysqli_ddl_edge');
        $this->mysqli->query("INSERT INTO mysqli_ddl_edge (id, val) VALUES (2, 'after')");

        $result = $this->mysqli->query('SELECT * FROM mysqli_ddl_edge');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('after', $rows[0]['val']);
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
        $raw->query('DROP TABLE IF EXISTS mysqli_ddl_edge');
        $raw->close();
    }
}
