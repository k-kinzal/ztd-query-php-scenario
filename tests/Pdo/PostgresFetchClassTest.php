<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;
use Tests\Support\UserDto;

/**
 * Tests PDO::FETCH_CLASS with custom classes on PostgreSQL ZTD.
 * @spec SPEC-4.10
 */
class PostgresFetchClassTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE fc_class_pg (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['fc_class_pg'];
    }


    public function testFetchClassWithSimpleDto(): void
    {
        $this->pdo->exec("INSERT INTO fc_class_pg VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->query('SELECT name, score FROM fc_class_pg WHERE id = 1');
        $stmt->setFetchMode(PDO::FETCH_CLASS, UserDto::class);
        $obj = $stmt->fetch();

        $this->assertInstanceOf(UserDto::class, $obj);
        $this->assertSame('Alice', $obj->name);
        $this->assertSame(100, (int) $obj->score);
    }

    public function testFetchAllWithFetchClass(): void
    {
        $this->pdo->exec("INSERT INTO fc_class_pg VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO fc_class_pg VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->query('SELECT name, score FROM fc_class_pg ORDER BY id');
        $objects = $stmt->fetchAll(PDO::FETCH_CLASS, UserDto::class);

        $this->assertCount(2, $objects);
        $this->assertInstanceOf(UserDto::class, $objects[0]);
        $this->assertSame('Alice', $objects[0]->name);
        $this->assertSame('Bob', $objects[1]->name);
    }

    public function testFetchClassAfterShadowUpdate(): void
    {
        $this->pdo->exec("INSERT INTO fc_class_pg VALUES (1, 'Alice', 100)");
        $this->pdo->exec("UPDATE fc_class_pg SET score = 999 WHERE id = 1");

        $stmt = $this->pdo->query('SELECT name, score FROM fc_class_pg WHERE id = 1');
        $stmt->setFetchMode(PDO::FETCH_CLASS, UserDto::class);
        $obj = $stmt->fetch();

        $this->assertInstanceOf(UserDto::class, $obj);
        $this->assertSame(999, (int) $obj->score);
    }

    public function testFetchClassWithPreparedStatement(): void
    {
        $this->pdo->exec("INSERT INTO fc_class_pg VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO fc_class_pg VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->prepare('SELECT name, score FROM fc_class_pg WHERE score > ?');
        $stmt->execute([80]);
        $objects = $stmt->fetchAll(PDO::FETCH_CLASS, UserDto::class);

        $this->assertCount(2, $objects);
    }
}
