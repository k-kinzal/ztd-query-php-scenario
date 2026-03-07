<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests rowCount() behavior after write operations in ZTD mode on SQLite PDO.
 */
class SqliteRowCountTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE rc_items (id INTEGER PRIMARY KEY, name TEXT, category TEXT, active INTEGER)');

        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO rc_items (id, name, category, active) VALUES (1, 'Alpha', 'A', 1)");
        $this->pdo->exec("INSERT INTO rc_items (id, name, category, active) VALUES (2, 'Beta', 'A', 1)");
        $this->pdo->exec("INSERT INTO rc_items (id, name, category, active) VALUES (3, 'Gamma', 'B', 0)");
        $this->pdo->exec("INSERT INTO rc_items (id, name, category, active) VALUES (4, 'Delta', 'B', 1)");
    }

    public function testRowCountAfterInsert(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO rc_items (id, name, category, active) VALUES (?, ?, ?, ?)');
        $stmt->execute([5, 'Epsilon', 'C', 1]);
        $this->assertSame(1, $stmt->rowCount());
    }

    public function testRowCountAfterUpdateSingleRow(): void
    {
        $stmt = $this->pdo->prepare('UPDATE rc_items SET name = ? WHERE id = ?');
        $stmt->execute(['AlphaUpdated', 1]);
        $this->assertSame(1, $stmt->rowCount());
    }

    public function testRowCountAfterUpdateMultipleRows(): void
    {
        $stmt = $this->pdo->prepare('UPDATE rc_items SET active = ? WHERE category = ?');
        $stmt->execute([0, 'A']);
        $this->assertSame(2, $stmt->rowCount());
    }

    public function testRowCountAfterUpdateNoMatch(): void
    {
        $stmt = $this->pdo->prepare('UPDATE rc_items SET name = ? WHERE id = ?');
        $stmt->execute(['NoMatch', 999]);
        $this->assertSame(0, $stmt->rowCount());
    }

    public function testRowCountAfterDeleteSingleRow(): void
    {
        $stmt = $this->pdo->prepare('DELETE FROM rc_items WHERE id = ?');
        $stmt->execute([1]);
        $this->assertSame(1, $stmt->rowCount());
    }

    public function testRowCountAfterDeleteMultipleRows(): void
    {
        $stmt = $this->pdo->prepare('DELETE FROM rc_items WHERE category = ?');
        $stmt->execute(['B']);
        $this->assertSame(2, $stmt->rowCount());
    }

    public function testRowCountAfterDeleteNoMatch(): void
    {
        $stmt = $this->pdo->prepare('DELETE FROM rc_items WHERE id = ?');
        $stmt->execute([999]);
        $this->assertSame(0, $stmt->rowCount());
    }

    public function testExecReturnsAffectedRowCount(): void
    {
        $count = $this->pdo->exec("UPDATE rc_items SET active = 0 WHERE category = 'A'");
        $this->assertSame(2, $count);
    }

    public function testExecDeleteReturnsAffectedRowCount(): void
    {
        $count = $this->pdo->exec("DELETE FROM rc_items WHERE active = 0");
        $this->assertSame(1, $count);
    }

    public function testRowCountReExecuteWithFrozenSnapshot(): void
    {
        $stmt = $this->pdo->prepare('DELETE FROM rc_items WHERE category = ?');

        $stmt->execute(['A']);
        $this->assertSame(2, $stmt->rowCount());

        $stmt->execute(['B']);
        $this->assertSame(2, $stmt->rowCount());

        // CTE snapshot is frozen at prepare time — the DELETE still "sees" 'A'
        // rows in the snapshot even though they were already deleted.
        $stmt->execute(['A']);
        $this->assertSame(2, $stmt->rowCount());
    }
}
