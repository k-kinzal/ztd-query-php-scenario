<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests EXCEPT and INTERSECT behavior via MySQLi.
 *
 * Cross-platform parity with MysqlExceptIntersectTest (PDO).
 * MySQL CTE rewriter misparses EXCEPT/INTERSECT as multi-statement queries.
 */
class ExceptIntersectTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_ei_a');
        $raw->query('DROP TABLE IF EXISTS mi_ei_b');
        $raw->query('CREATE TABLE mi_ei_a (id INT PRIMARY KEY, name VARCHAR(50))');
        $raw->query('CREATE TABLE mi_ei_b (id INT PRIMARY KEY, name VARCHAR(50))');
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

        $this->mysqli->query("INSERT INTO mi_ei_a VALUES (1, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_ei_a VALUES (2, 'Bob')");
        $this->mysqli->query("INSERT INTO mi_ei_a VALUES (3, 'Charlie')");

        $this->mysqli->query("INSERT INTO mi_ei_b VALUES (2, 'Bob')");
        $this->mysqli->query("INSERT INTO mi_ei_b VALUES (3, 'Charlie')");
        $this->mysqli->query("INSERT INTO mi_ei_b VALUES (4, 'Diana')");
    }

    /**
     * EXCEPT throws exception on MySQL.
     */
    public function testExceptThrowsOnMysql(): void
    {
        $this->expectException(\Throwable::class);
        $this->mysqli->query('SELECT name FROM mi_ei_a EXCEPT SELECT name FROM mi_ei_b');
    }

    /**
     * INTERSECT throws exception on MySQL.
     */
    public function testIntersectThrowsOnMysql(): void
    {
        $this->expectException(\Throwable::class);
        $this->mysqli->query('SELECT name FROM mi_ei_a INTERSECT SELECT name FROM mi_ei_b');
    }

    /**
     * UNION works correctly on MySQL.
     */
    public function testUnionWorksOnMysql(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_ei_a UNION SELECT name FROM mi_ei_b ORDER BY name');
        $rows = [];
        while ($row = $result->fetch_assoc()) {
            $rows[] = $row['name'];
        }
        $this->assertCount(4, $rows);
        $this->assertSame('Alice', $rows[0]);
    }

    /**
     * NOT IN workaround for EXCEPT.
     */
    public function testNotInWorkaroundForExcept(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_ei_a WHERE name NOT IN (SELECT name FROM mi_ei_b) ORDER BY name');
        $rows = [];
        while ($row = $result->fetch_assoc()) {
            $rows[] = $row['name'];
        }
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]);
    }

    /**
     * IN workaround for INTERSECT.
     */
    public function testInWorkaroundForIntersect(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_ei_a WHERE name IN (SELECT name FROM mi_ei_b) ORDER BY name');
        $rows = [];
        while ($row = $result->fetch_assoc()) {
            $rows[] = $row['name'];
        }
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]);
        $this->assertSame('Charlie', $rows[1]);
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
            $raw->query('DROP TABLE IF EXISTS mi_ei_a');
            $raw->query('DROP TABLE IF EXISTS mi_ei_b');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
