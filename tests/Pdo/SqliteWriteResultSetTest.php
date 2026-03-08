<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/** @spec SPEC-4.5 */
class SqliteWriteResultSetTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE write_result_test (id INTEGER PRIMARY KEY, val TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['write_result_test'];
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
