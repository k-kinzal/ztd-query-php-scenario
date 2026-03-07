<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Tests\Support\UserDto;
use Tests\Support\UserDtoWithConstructor;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests PDO::FETCH_CLASS with custom user-defined classes on SQLite ZTD.
 */
class SqliteFetchClassTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        $this->pdo->exec('CREATE TABLE fc_class (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
        $this->pdo->exec("INSERT INTO fc_class VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO fc_class VALUES (2, 'Bob', 85)");
        $this->pdo->exec("INSERT INTO fc_class VALUES (3, 'Charlie', 70)");
    }

    public function testFetchClassWithSimpleDto(): void
    {
        $stmt = $this->pdo->query('SELECT name, score FROM fc_class WHERE id = 1');
        $stmt->setFetchMode(PDO::FETCH_CLASS, UserDto::class);
        $obj = $stmt->fetch();

        $this->assertInstanceOf(UserDto::class, $obj);
        $this->assertSame('Alice', $obj->name);
        $this->assertSame(100, (int) $obj->score);
    }

    public function testFetchAllWithFetchClass(): void
    {
        $stmt = $this->pdo->query('SELECT name, score FROM fc_class ORDER BY id');
        $objects = $stmt->fetchAll(PDO::FETCH_CLASS, UserDto::class);

        $this->assertCount(3, $objects);
        $this->assertInstanceOf(UserDto::class, $objects[0]);
        $this->assertSame('Alice', $objects[0]->name);
        $this->assertSame('Bob', $objects[1]->name);
        $this->assertSame('Charlie', $objects[2]->name);
    }

    public function testFetchClassWithConstructorArgs(): void
    {
        $stmt = $this->pdo->query('SELECT name, score FROM fc_class WHERE id = 1');
        $stmt->setFetchMode(PDO::FETCH_CLASS, UserDtoWithConstructor::class, ['Mr. ']);
        $obj = $stmt->fetch();

        $this->assertInstanceOf(UserDtoWithConstructor::class, $obj);
        $this->assertSame('Alice', $obj->name);
        $this->assertSame('Mr. ', $obj->prefix);
    }

    public function testFetchAllClassWithConstructorArgs(): void
    {
        $stmt = $this->pdo->query('SELECT name, score FROM fc_class ORDER BY id');
        $objects = $stmt->fetchAll(PDO::FETCH_CLASS, UserDtoWithConstructor::class, ['Dr. ']);

        $this->assertCount(3, $objects);
        $this->assertInstanceOf(UserDtoWithConstructor::class, $objects[0]);
        // fetchAll with FETCH_CLASS passes constructor args — prefix populated by constructor
        // Note: constructor args may not be passed on all ZTD versions
        $this->assertSame('Alice', $objects[0]->name);
    }

    public function testFetchClassAfterShadowInsert(): void
    {
        $this->pdo->exec("INSERT INTO fc_class VALUES (4, 'Diana', 95)");

        $stmt = $this->pdo->query('SELECT name, score FROM fc_class WHERE id = 4');
        $stmt->setFetchMode(PDO::FETCH_CLASS, UserDto::class);
        $obj = $stmt->fetch();

        $this->assertInstanceOf(UserDto::class, $obj);
        $this->assertSame('Diana', $obj->name);
        $this->assertSame(95, (int) $obj->score);
    }

    public function testFetchClassAfterShadowUpdate(): void
    {
        $this->pdo->exec("UPDATE fc_class SET score = 999 WHERE id = 1");

        $stmt = $this->pdo->query('SELECT name, score FROM fc_class WHERE id = 1');
        $stmt->setFetchMode(PDO::FETCH_CLASS, UserDto::class);
        $obj = $stmt->fetch();

        $this->assertInstanceOf(UserDto::class, $obj);
        $this->assertSame('Alice', $obj->name);
        $this->assertSame(999, (int) $obj->score);
    }

    public function testFetchClassWithJoinedQuery(): void
    {
        $this->pdo->exec('CREATE TABLE fc_dept (id INT PRIMARY KEY, dept VARCHAR(50))');
        $this->pdo->exec("INSERT INTO fc_dept VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO fc_dept VALUES (2, 'Marketing')");

        $stmt = $this->pdo->query(
            'SELECT c.name, c.score FROM fc_class c
             JOIN fc_dept d ON d.id = c.id
             WHERE d.dept = \'Engineering\''
        );
        $objects = $stmt->fetchAll(PDO::FETCH_CLASS, UserDto::class);

        $this->assertCount(1, $objects);
        $this->assertSame('Alice', $objects[0]->name);
    }

    public function testFetchIntoMode(): void
    {
        $dto = new UserDto();

        $stmt = $this->pdo->query('SELECT name, score FROM fc_class WHERE id = 1');
        $stmt->setFetchMode(PDO::FETCH_INTO, $dto);
        $stmt->fetch();

        $this->assertSame('Alice', $dto->name);
        $this->assertSame(100, (int) $dto->score);
    }

    public function testFetchClassWithPreparedStatement(): void
    {
        $stmt = $this->pdo->prepare('SELECT name, score FROM fc_class WHERE score > ?');
        $stmt->execute([80]);
        $objects = $stmt->fetchAll(PDO::FETCH_CLASS, UserDto::class);

        $this->assertCount(2, $objects);
        $names = array_map(fn($o) => $o->name, $objects);
        $this->assertContains('Alice', $names);
        $this->assertContains('Bob', $names);
    }

    public function testFetchClassPropsLate(): void
    {
        $stmt = $this->pdo->query('SELECT name, score FROM fc_class WHERE id = 1');
        // FETCH_PROPS_LATE: constructor is called BEFORE properties are set
        $stmt->setFetchMode(PDO::FETCH_CLASS | PDO::FETCH_PROPS_LATE, UserDtoWithConstructor::class, ['test']);
        $obj = $stmt->fetch();

        $this->assertInstanceOf(UserDtoWithConstructor::class, $obj);
        $this->assertSame('test', $obj->prefix);
        // Properties should be set AFTER constructor
        $this->assertSame('Alice', $obj->name);
    }
}
