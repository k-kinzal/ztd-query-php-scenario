<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests UNION queries with mutations on MySQLi.
 */
class UnionMutationTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_um_contractors');
        $raw->query('DROP TABLE IF EXISTS mi_um_employees');
        $raw->query('CREATE TABLE mi_um_employees (id INT PRIMARY KEY, name VARCHAR(50), dept VARCHAR(20))');
        $raw->query('CREATE TABLE mi_um_contractors (id INT PRIMARY KEY, name VARCHAR(50), dept VARCHAR(20))');
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

        $this->mysqli->query("INSERT INTO mi_um_employees (id, name, dept) VALUES (1, 'Alice', 'Eng')");
        $this->mysqli->query("INSERT INTO mi_um_employees (id, name, dept) VALUES (2, 'Bob', 'Sales')");
        $this->mysqli->query("INSERT INTO mi_um_contractors (id, name, dept) VALUES (1, 'Dave', 'Eng')");
    }

    public function testUnionReflectsInserts(): void
    {
        $this->mysqli->query("INSERT INTO mi_um_contractors (id, name, dept) VALUES (2, 'Frank', 'Eng')");

        $result = $this->mysqli->query("
            SELECT name FROM mi_um_employees WHERE dept = 'Eng'
            UNION ALL
            SELECT name FROM mi_um_contractors WHERE dept = 'Eng'
            ORDER BY name
        ");
        $names = array_column($result->fetch_all(MYSQLI_ASSOC), 'name');
        $this->assertSame(['Alice', 'Dave', 'Frank'], $names);
    }

    public function testUnionReflectsDeletes(): void
    {
        $this->mysqli->query("DELETE FROM mi_um_employees WHERE name = 'Alice'");

        $result = $this->mysqli->query("
            SELECT name FROM mi_um_employees
            UNION ALL
            SELECT name FROM mi_um_contractors
            ORDER BY name
        ");
        $names = array_column($result->fetch_all(MYSQLI_ASSOC), 'name');
        $this->assertSame(['Bob', 'Dave'], $names);
    }

    public function testUnionDistinct(): void
    {
        $this->mysqli->query("INSERT INTO mi_um_contractors (id, name, dept) VALUES (2, 'Alice', 'Eng')");

        $result = $this->mysqli->query("
            SELECT name FROM mi_um_employees
            UNION
            SELECT name FROM mi_um_contractors
            ORDER BY name
        ");
        $names = array_column($result->fetch_all(MYSQLI_ASSOC), 'name');
        $this->assertSame(['Alice', 'Bob', 'Dave'], $names);
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
        $raw->query('DROP TABLE IF EXISTS mi_um_contractors');
        $raw->query('DROP TABLE IF EXISTS mi_um_employees');
        $raw->close();
    }
}
