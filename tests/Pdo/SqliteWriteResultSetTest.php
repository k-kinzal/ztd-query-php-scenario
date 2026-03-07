<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

class SqliteWriteResultSetTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE write_result_test (id INTEGER PRIMARY KEY, val TEXT)');

        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    public function testInsertViaExecReturnsAffectedCount(): void
    {
        $count = $this->pdo->exec("INSERT INTO write_result_test (id, val) VALUES (1, 'a')");
        $this->assertSame(1, $count);
    }

    public function testInsertViaPreparedStatementFetchAllReturnsEmpty(): void
    {
        $stmt = $this->pdo->prepare("INSERT INTO write_result_test (id, val) VALUES (?, ?)");
        $stmt->execute([1, 'a']);

        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame([], $rows);
    }

    public function testUpdateViaPreparedStatementFetchAllReturnsEmpty(): void
    {
        $this->pdo->exec("INSERT INTO write_result_test (id, val) VALUES (1, 'a')");

        $stmt = $this->pdo->prepare("UPDATE write_result_test SET val = ? WHERE id = ?");
        $stmt->execute(['b', 1]);

        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame([], $rows);
    }

    public function testDeleteViaPreparedStatementFetchAllReturnsEmpty(): void
    {
        $this->pdo->exec("INSERT INTO write_result_test (id, val) VALUES (1, 'a')");

        $stmt = $this->pdo->prepare("DELETE FROM write_result_test WHERE id = ?");
        $stmt->execute([1]);

        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame([], $rows);
    }
}
