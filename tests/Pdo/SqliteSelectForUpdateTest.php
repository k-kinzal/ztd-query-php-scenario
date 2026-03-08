<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests SELECT...FOR UPDATE behavior on SQLite.
 *
 * SQLite does not support FOR UPDATE syntax — it should throw.
 */
class SqliteSelectForUpdateTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:');
        $raw->exec('CREATE TABLE sfu_test (id INT PRIMARY KEY, name VARCHAR(50))');
        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO sfu_test VALUES (1, 'Alice')");
    }

    /**
     * SELECT...FOR UPDATE throws on SQLite (not supported).
     */
    public function testSelectForUpdateThrowsOnSqlite(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->query('SELECT name FROM sfu_test WHERE id = 1 FOR UPDATE');
    }

    /**
     * Regular SELECT works normally.
     */
    public function testRegularSelectWorks(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM sfu_test WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }
}
