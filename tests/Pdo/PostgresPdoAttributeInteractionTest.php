<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests PDO attribute interactions with ZTD on PostgreSQL.
 * PostgreSQL does not support EMULATE_PREPARES the same way MySQL does,
 * but the attribute can still be set and should not break ZTD behavior.
 */
class PostgresPdoAttributeInteractionTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS attr_pg');
        $raw->exec('CREATE TABLE attr_pg (id INT PRIMARY KEY, name VARCHAR(50), score DECIMAL(10,2))');
    }

    public function testEmulatePreparesTrueWithShadow(): void
    {
        $pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_EMULATE_PREPARES => true,
            ],
        );

        $pdo->exec("INSERT INTO attr_pg VALUES (1, 'Alice', 99.50)");

        $stmt = $pdo->prepare('SELECT name, score FROM attr_pg WHERE id = ?');
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertSame('Alice', $row['name']);
        $this->assertSame('99.50', (string) $row['score']);
    }

    public function testEmulatePreparesFalseWithShadow(): void
    {
        $pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_EMULATE_PREPARES => false,
            ],
        );

        $pdo->exec("INSERT INTO attr_pg VALUES (1, 'Alice', 99.50)");

        $stmt = $pdo->prepare('SELECT name, score FROM attr_pg WHERE id = ?');
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertSame('Alice', $row['name']);
    }

    public function testStringifyFetchesWithShadow(): void
    {
        $pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_STRINGIFY_FETCHES => true,
            ],
        );

        $pdo->exec("INSERT INTO attr_pg VALUES (1, 'Alice', 99.50)");

        $stmt = $pdo->query('SELECT id, score FROM attr_pg WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        // With STRINGIFY_FETCHES, all values should be strings
        $this->assertIsString($row['id']);
        $this->assertIsString($row['score']);
    }

    public function testNamedParamsWithEmulatePrepares(): void
    {
        $pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_EMULATE_PREPARES => true,
            ],
        );

        $pdo->exec("INSERT INTO attr_pg VALUES (1, 'Alice', 100)");
        $pdo->exec("INSERT INTO attr_pg VALUES (2, 'Bob', 85)");

        $stmt = $pdo->prepare('SELECT name FROM attr_pg WHERE score > :min_score ORDER BY id');
        $stmt->execute([':min_score' => 80]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testCaseAttributeNatural(): void
    {
        $pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_CASE => PDO::CASE_NATURAL,
            ],
        );

        // PostgreSQL lowercases unquoted identifiers, so use lowercase column names
        $pdo->exec('CREATE TABLE case_pg (id INT PRIMARY KEY, username VARCHAR(50))');
        $pdo->exec("INSERT INTO case_pg VALUES (1, 'Alice')");

        $stmt = $pdo->query('SELECT username FROM case_pg WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        // CASE_NATURAL preserves original case (lowercase for PostgreSQL)
        $this->assertArrayHasKey('username', $row);
    }

    public function testMultipleInsertsWithDifferentAttributes(): void
    {
        $pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            ],
        );

        $pdo->exec('CREATE TABLE attr_multi_pg (id INT PRIMARY KEY, val TEXT)');
        $pdo->exec("INSERT INTO attr_multi_pg VALUES (1, 'first')");

        // Change attribute mid-session
        $pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_OBJ);
        $pdo->exec("INSERT INTO attr_multi_pg VALUES (2, 'second')");

        $stmt = $pdo->query('SELECT val FROM attr_multi_pg ORDER BY id');
        $row = $stmt->fetch();

        // Should now use OBJ mode
        $this->assertIsObject($row);
        $this->assertSame('first', $row->val);
    }

    public function testEmulatePreparesSwitchDuringSession(): void
    {
        $pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            ],
        );

        $pdo->exec('CREATE TABLE switch_pg (id INT PRIMARY KEY, name TEXT)');
        $pdo->exec("INSERT INTO switch_pg VALUES (1, 'Alice')");

        // Set emulate prepares to true
        $pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, true);
        $stmt1 = $pdo->prepare('SELECT name FROM switch_pg WHERE id = ?');
        $stmt1->execute([1]);
        $row1 = $stmt1->fetch(PDO::FETCH_ASSOC);

        // Set emulate prepares to false
        $pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
        $stmt2 = $pdo->prepare('SELECT name FROM switch_pg WHERE id = ?');
        $stmt2->execute([1]);
        $row2 = $stmt2->fetch(PDO::FETCH_ASSOC);

        // Both should work correctly with shadow store
        $this->assertSame('Alice', $row1['name']);
        $this->assertSame('Alice', $row2['name']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS attr_pg');
        $raw->exec('DROP TABLE IF EXISTS case_pg');
        $raw->exec('DROP TABLE IF EXISTS attr_multi_pg');
        $raw->exec('DROP TABLE IF EXISTS switch_pg');
    }
}
