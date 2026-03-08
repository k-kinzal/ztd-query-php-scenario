<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Config\UnsupportedSqlBehavior;
use ZtdQuery\Config\ZtdConfig;

/**
 * Tests EXCEPT and INTERSECT behavior on MySQL PDO.
 *
 * MySQL CTE rewriter misparses EXCEPT/INTERSECT as multi-statement queries,
 * throwing UnsupportedSqlException.
 */
class MysqlExceptIntersectTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pdo_ei_a');
        $raw->exec('DROP TABLE IF EXISTS pdo_ei_b');
        $raw->exec('CREATE TABLE pdo_ei_a (id INT PRIMARY KEY, name VARCHAR(50))');
        $raw->exec('CREATE TABLE pdo_ei_b (id INT PRIMARY KEY, name VARCHAR(50))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pdo_ei_a VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pdo_ei_a VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO pdo_ei_a VALUES (3, 'Charlie')");

        $this->pdo->exec("INSERT INTO pdo_ei_b VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO pdo_ei_b VALUES (3, 'Charlie')");
        $this->pdo->exec("INSERT INTO pdo_ei_b VALUES (4, 'Diana')");
    }

    /**
     * EXCEPT throws exception on MySQL due to misparse.
     */
    public function testExceptThrowsOnMysql(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->query('SELECT name FROM pdo_ei_a EXCEPT SELECT name FROM pdo_ei_b');
    }

    /**
     * INTERSECT throws exception on MySQL due to misparse.
     */
    public function testIntersectThrowsOnMysql(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->query('SELECT name FROM pdo_ei_a INTERSECT SELECT name FROM pdo_ei_b');
    }

    /**
     * UNION works correctly on MySQL (not affected).
     */
    public function testUnionWorksOnMysql(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM pdo_ei_a UNION SELECT name FROM pdo_ei_b ORDER BY name');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(4, $rows);
        $this->assertSame('Alice', $rows[0]);
    }

    /**
     * NOT IN workaround for EXCEPT.
     */
    public function testNotInWorkaroundForExcept(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM pdo_ei_a WHERE name NOT IN (SELECT name FROM pdo_ei_b) ORDER BY name');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]);
    }

    /**
     * IN workaround for INTERSECT.
     */
    public function testInWorkaroundForIntersect(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM pdo_ei_a WHERE name IN (SELECT name FROM pdo_ei_b) ORDER BY name');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]);
        $this->assertSame('Charlie', $rows[1]);
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP TABLE IF EXISTS pdo_ei_a');
            $raw->exec('DROP TABLE IF EXISTS pdo_ei_b');
        } catch (\Exception $e) {
        }
    }
}
