<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests UNION queries with mutations on MySQL PDO.
 * Note: EXCEPT and INTERSECT are NOT tested here as they throw
 * UnsupportedSqlException on MySQL (see spec 3.3d).
 */
class MysqlUnionMutationTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS um_contractors');
        $raw->exec('DROP TABLE IF EXISTS um_employees');
        $raw->exec('CREATE TABLE um_employees (id INT PRIMARY KEY, name VARCHAR(50), dept VARCHAR(20))');
        $raw->exec('CREATE TABLE um_contractors (id INT PRIMARY KEY, name VARCHAR(50), dept VARCHAR(20))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO um_employees (id, name, dept) VALUES (1, 'Alice', 'Eng')");
        $this->pdo->exec("INSERT INTO um_employees (id, name, dept) VALUES (2, 'Bob', 'Sales')");
        $this->pdo->exec("INSERT INTO um_employees (id, name, dept) VALUES (3, 'Carol', 'Eng')");
        $this->pdo->exec("INSERT INTO um_contractors (id, name, dept) VALUES (1, 'Dave', 'Eng')");
        $this->pdo->exec("INSERT INTO um_contractors (id, name, dept) VALUES (2, 'Eve', 'Ops')");
    }

    public function testUnionReflectsInserts(): void
    {
        $this->pdo->exec("INSERT INTO um_contractors (id, name, dept) VALUES (3, 'Frank', 'Eng')");

        $stmt = $this->pdo->query("
            SELECT name FROM um_employees WHERE dept = 'Eng'
            UNION ALL
            SELECT name FROM um_contractors WHERE dept = 'Eng'
            ORDER BY name
        ");
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        $this->assertSame(['Alice', 'Carol', 'Dave', 'Frank'], $names);
    }

    public function testUnionReflectsUpdates(): void
    {
        $this->pdo->exec("UPDATE um_employees SET dept = 'Eng' WHERE name = 'Bob'");

        $stmt = $this->pdo->query("
            SELECT name FROM um_employees WHERE dept = 'Eng'
            UNION ALL
            SELECT name FROM um_contractors WHERE dept = 'Eng'
            ORDER BY name
        ");
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        $this->assertSame(['Alice', 'Bob', 'Carol', 'Dave'], $names);
    }

    public function testUnionReflectsDeletes(): void
    {
        $this->pdo->exec("DELETE FROM um_employees WHERE name = 'Alice'");

        $stmt = $this->pdo->query("
            SELECT name FROM um_employees WHERE dept = 'Eng'
            UNION ALL
            SELECT name FROM um_contractors WHERE dept = 'Eng'
            ORDER BY name
        ");
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        $this->assertSame(['Carol', 'Dave'], $names);
    }

    public function testUnionDistinct(): void
    {
        $this->pdo->exec("INSERT INTO um_contractors (id, name, dept) VALUES (3, 'Alice', 'Eng')");

        $stmt = $this->pdo->query("
            SELECT name FROM um_employees WHERE dept = 'Eng'
            UNION
            SELECT name FROM um_contractors WHERE dept = 'Eng'
            ORDER BY name
        ");
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        $this->assertSame(['Alice', 'Carol', 'Dave'], $names);
    }

    public function testUnionWithPreparedStatement(): void
    {
        $stmt = $this->pdo->prepare("
            SELECT name FROM um_employees WHERE dept = ?
            UNION ALL
            SELECT name FROM um_contractors WHERE dept = ?
            ORDER BY name
        ");
        $stmt->execute(['Eng', 'Eng']);
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        $this->assertSame(['Alice', 'Carol', 'Dave'], $names);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS um_contractors');
        $raw->exec('DROP TABLE IF EXISTS um_employees');
    }
}
