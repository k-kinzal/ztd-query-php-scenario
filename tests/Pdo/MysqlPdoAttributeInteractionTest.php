<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;
use Tests\Support\MySQLContainer;

/**
 * Tests PDO attribute interactions with ZTD on MySQL.
 * EMULATE_PREPARES is particularly important on MySQL.
 * @spec pending
 */
class MysqlPdoAttributeInteractionTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE attr_mysql (id INT PRIMARY KEY, name VARCHAR(50), score DECIMAL(10,2))';
    }

    protected function getTableNames(): array
    {
        return ['attr_mysql'];
    }


    public function testEmulatePreparesTrueWithShadow(): void
    {
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_EMULATE_PREPARES => true,
            ],
        );

        $pdo->exec("INSERT INTO attr_mysql VALUES (1, 'Alice', 99.50)");

        $stmt = $pdo->prepare('SELECT name, score FROM attr_mysql WHERE id = ?');
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertSame('Alice', $row['name']);
        $this->assertSame('99.50', (string) $row['score']);
    }

    public function testEmulatePreparesFalseWithShadow(): void
    {
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_EMULATE_PREPARES => false,
            ],
        );

        $pdo->exec("INSERT INTO attr_mysql VALUES (1, 'Alice', 99.50)");

        $stmt = $pdo->prepare('SELECT name, score FROM attr_mysql WHERE id = ?');
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertSame('Alice', $row['name']);
    }

    public function testStringifyFetchesWithShadow(): void
    {
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_STRINGIFY_FETCHES => true,
            ],
        );

        $pdo->exec("INSERT INTO attr_mysql VALUES (1, 'Alice', 99.50)");

        $stmt = $pdo->query('SELECT id, score FROM attr_mysql WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        // With STRINGIFY_FETCHES, all values should be strings
        $this->assertIsString($row['id']);
        $this->assertIsString($row['score']);
    }

    public function testNamedParamsWithEmulatePrepares(): void
    {
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_EMULATE_PREPARES => true,
            ],
        );

        $pdo->exec("INSERT INTO attr_mysql VALUES (1, 'Alice', 100)");
        $pdo->exec("INSERT INTO attr_mysql VALUES (2, 'Bob', 85)");

        $stmt = $pdo->prepare('SELECT name FROM attr_mysql WHERE score > :min_score ORDER BY id');
        $stmt->execute([':min_score' => 80]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }
}
