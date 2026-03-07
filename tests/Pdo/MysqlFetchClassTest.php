<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use Tests\Support\UserDto;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests PDO::FETCH_CLASS with custom classes on MySQL ZTD.
 */
class MysqlFetchClassTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS fc_class_m');
        $raw->exec('CREATE TABLE fc_class_m (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public function testFetchClassWithSimpleDto(): void
    {
        $this->pdo->exec("INSERT INTO fc_class_m VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->query('SELECT name, score FROM fc_class_m WHERE id = 1');
        $stmt->setFetchMode(PDO::FETCH_CLASS, UserDto::class);
        $obj = $stmt->fetch();

        $this->assertInstanceOf(UserDto::class, $obj);
        $this->assertSame('Alice', $obj->name);
        $this->assertSame(100, (int) $obj->score);
    }

    public function testFetchAllWithFetchClass(): void
    {
        $this->pdo->exec("INSERT INTO fc_class_m VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO fc_class_m VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->query('SELECT name, score FROM fc_class_m ORDER BY id');
        $objects = $stmt->fetchAll(PDO::FETCH_CLASS, UserDto::class);

        $this->assertCount(2, $objects);
        $this->assertInstanceOf(UserDto::class, $objects[0]);
        $this->assertSame('Alice', $objects[0]->name);
        $this->assertSame('Bob', $objects[1]->name);
    }

    public function testFetchClassAfterShadowInsert(): void
    {
        $this->pdo->exec("INSERT INTO fc_class_m VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->query('SELECT name, score FROM fc_class_m WHERE id = 1');
        $stmt->setFetchMode(PDO::FETCH_CLASS, UserDto::class);
        $obj = $stmt->fetch();

        $this->assertInstanceOf(UserDto::class, $obj);
        $this->assertSame('Alice', $obj->name);
    }

    public function testFetchClassWithPreparedStatement(): void
    {
        $this->pdo->exec("INSERT INTO fc_class_m VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO fc_class_m VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->prepare('SELECT name, score FROM fc_class_m WHERE score > ?');
        $stmt->execute([80]);
        $objects = $stmt->fetchAll(PDO::FETCH_CLASS, UserDto::class);

        $this->assertCount(2, $objects);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS fc_class_m');
    }
}
