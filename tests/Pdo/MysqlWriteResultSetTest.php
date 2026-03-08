<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests write operation result sets in ZTD mode on MySQL via PDO:
 * affected row counts and empty fetch results from write statements.
 * @spec SPEC-4.5
 */
class MysqlWriteResultSetTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mysql_write_rs (id INT PRIMARY KEY, val VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['mysql_write_rs'];
    }


    public function testInsertViaExecReturnsAffectedCount(): void
    {
        $count = $this->pdo->exec("INSERT INTO mysql_write_rs (id, val) VALUES (1, 'a')");
        $this->assertSame(1, $count);
    }

    public function testInsertViaPreparedStatementFetchAllReturnsEmpty(): void
    {
        $stmt = $this->pdo->prepare("INSERT INTO mysql_write_rs (id, val) VALUES (?, ?)");
        $stmt->execute([1, 'a']);

        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame([], $rows);
    }

    public function testUpdateViaPreparedStatementFetchAllReturnsEmpty(): void
    {
        $this->pdo->exec("INSERT INTO mysql_write_rs (id, val) VALUES (1, 'a')");

        $stmt = $this->pdo->prepare("UPDATE mysql_write_rs SET val = ? WHERE id = ?");
        $stmt->execute(['b', 1]);

        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame([], $rows);
    }

    public function testDeleteViaPreparedStatementFetchAllReturnsEmpty(): void
    {
        $this->pdo->exec("INSERT INTO mysql_write_rs (id, val) VALUES (1, 'a')");

        $stmt = $this->pdo->prepare("DELETE FROM mysql_write_rs WHERE id = ?");
        $stmt->execute([1]);

        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame([], $rows);
    }
}
