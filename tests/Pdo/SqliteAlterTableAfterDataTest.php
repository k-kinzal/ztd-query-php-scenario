<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests ALTER TABLE ADD COLUMN behavior with the shadow store on SQLite PDO.
 *
 * Discovery: ALTER TABLE ADD COLUMN does NOT update the reflected schema.
 * The CTE rewriter uses the schema from CREATE TABLE time.
 * - INSERT with new column values succeeds (goes to shadow store)
 * - SELECT referencing the new column fails ("no such column")
 * - Queries using only original columns still work
 */
class SqliteAlterTableAfterDataTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        $this->pdo->exec('CREATE TABLE evolve (id INT PRIMARY KEY, name VARCHAR(50))');
    }

    /**
     * SELECT referencing the new column fails after ALTER TABLE ADD COLUMN.
     */
    public function testSelectNewColumnFailsAfterAlter(): void
    {
        $this->pdo->exec("INSERT INTO evolve VALUES (1, 'Alice')");
        $this->pdo->exec('ALTER TABLE evolve ADD COLUMN score INT');

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('no such column: score');
        $this->pdo->query('SELECT name, score FROM evolve WHERE id = 1');
    }

    /**
     * INSERT with the new column succeeds — shadow store accepts the data.
     */
    public function testInsertWithNewColumnSucceeds(): void
    {
        $this->pdo->exec("INSERT INTO evolve VALUES (1, 'Alice')");
        $this->pdo->exec('ALTER TABLE evolve ADD COLUMN score INT');

        // This does NOT throw — INSERT goes to shadow store
        $this->pdo->exec("INSERT INTO evolve (id, name, score) VALUES (2, 'Bob', 100)");

        // But we can't verify the new column via SELECT
        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM evolve');
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    /**
     * Original columns still work after ALTER TABLE.
     */
    public function testOriginalColumnsStillWorkAfterAlter(): void
    {
        $this->pdo->exec("INSERT INTO evolve VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO evolve VALUES (2, 'Bob')");
        $this->pdo->exec('ALTER TABLE evolve ADD COLUMN score INT');

        $stmt = $this->pdo->query('SELECT name FROM evolve WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * Even ALTER TABLE BEFORE any data — new column still not in reflected schema.
     */
    public function testAlterBeforeInsertNewColumnStillNotVisible(): void
    {
        $this->pdo->exec('ALTER TABLE evolve ADD COLUMN score INT');
        $this->pdo->exec("INSERT INTO evolve (id, name, score) VALUES (1, 'Alice', 95)");

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('no such column: score');
        $this->pdo->query('SELECT name, score FROM evolve WHERE id = 1');
    }
}
