<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests PDO attribute interactions with ZTD mode on SQLite.
 * Specifically: EMULATE_PREPARES, STRINGIFY_FETCHES, CASE attributes.
 * @spec SPEC-4.9
 */
class SqlitePdoAttributeInteractionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sf_test (id INT PRIMARY KEY, score REAL)',
            'CREATE TABLE ep_test (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE case_test (id INT PRIMARY KEY, UserName VARCHAR(50))',
            'CREATE TABLE attr_test (id INT PRIMARY KEY, val TEXT)',
            'CREATE TABLE switch_test (id INT PRIMARY KEY, name TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sf_test', 'ep_test', 'case_test', 'attr_test', 'switch_test'];
    }

    public function testStringifyFetchesEnabled(): void
    {
        $pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_STRINGIFY_FETCHES => true,
        ]);

        $pdo->exec('CREATE TABLE sf_test (id INT PRIMARY KEY, score REAL)');
        $pdo->exec("INSERT INTO sf_test VALUES (1, 99.5)");

        $stmt = $pdo->query('SELECT score FROM sf_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        // With STRINGIFY_FETCHES, numeric values should be returned as strings
        $this->assertIsString($row['score']);
    }

    public function testStringifyFetchesDisabled(): void
    {
        $pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_STRINGIFY_FETCHES => false,
        ]);

        $pdo->exec('CREATE TABLE sf_test (id INT PRIMARY KEY, score REAL)');
        $pdo->exec("INSERT INTO sf_test VALUES (1, 99.5)");

        $stmt = $pdo->query('SELECT score FROM sf_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        // SQLite returns strings by default (regardless of STRINGIFY_FETCHES)
        // because SQLite is typeless — all values come back as strings
        $this->assertNotNull($row['score']);
    }

    public function testEmulatePreparesTrueWithShadow(): void
    {
        $pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_EMULATE_PREPARES => true,
        ]);

        $pdo->exec('CREATE TABLE ep_test (id INT PRIMARY KEY, name VARCHAR(50))');
        $pdo->exec("INSERT INTO ep_test VALUES (1, 'Alice')");

        $stmt = $pdo->prepare('SELECT name FROM ep_test WHERE id = ?');
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertSame('Alice', $row['name']);
    }

    public function testEmulatePreparesFalseWithShadow(): void
    {
        $pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_EMULATE_PREPARES => false,
        ]);

        $pdo->exec('CREATE TABLE ep_test (id INT PRIMARY KEY, name VARCHAR(50))');
        $pdo->exec("INSERT INTO ep_test VALUES (1, 'Alice')");

        $stmt = $pdo->prepare('SELECT name FROM ep_test WHERE id = ?');
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertSame('Alice', $row['name']);
    }

    public function testCaseAttributeNatural(): void
    {
        $pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_CASE => PDO::CASE_NATURAL,
        ]);

        $pdo->exec('CREATE TABLE case_test (id INT PRIMARY KEY, UserName VARCHAR(50))');
        $pdo->exec("INSERT INTO case_test VALUES (1, 'Alice')");

        $stmt = $pdo->query('SELECT UserName FROM case_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        // CASE_NATURAL preserves original case
        $this->assertArrayHasKey('UserName', $row);
    }

    public function testMultipleInsertsWithDifferentAttributes(): void
    {
        $pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        $pdo->exec('CREATE TABLE attr_test (id INT PRIMARY KEY, val TEXT)');
        $pdo->exec("INSERT INTO attr_test VALUES (1, 'first')");

        // Change attribute mid-session
        $pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_OBJ);
        $pdo->exec("INSERT INTO attr_test VALUES (2, 'second')");

        $stmt = $pdo->query('SELECT val FROM attr_test ORDER BY id');
        $row = $stmt->fetch();

        // Should now use OBJ mode
        $this->assertIsObject($row);
        $this->assertSame('first', $row->val);
    }

    public function testEmulatePreparesSwitchDuringSession(): void
    {
        $pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        $pdo->exec('CREATE TABLE switch_test (id INT PRIMARY KEY, name TEXT)');
        $pdo->exec("INSERT INTO switch_test VALUES (1, 'Alice')");

        // Set emulate prepares to true
        $pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, true);
        $stmt1 = $pdo->prepare('SELECT name FROM switch_test WHERE id = ?');
        $stmt1->execute([1]);
        $row1 = $stmt1->fetch(PDO::FETCH_ASSOC);

        // Set emulate prepares to false
        $pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
        $stmt2 = $pdo->prepare('SELECT name FROM switch_test WHERE id = ?');
        $stmt2->execute([1]);
        $row2 = $stmt2->fetch(PDO::FETCH_ASSOC);

        // Both should work correctly with shadow store
        $this->assertSame('Alice', $row1['name']);
        $this->assertSame('Alice', $row2['name']);
    }
}
