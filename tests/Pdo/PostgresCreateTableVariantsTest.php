<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests CREATE TABLE LIKE, CREATE TABLE AS SELECT, and ALTER TABLE behavior on PostgreSQL.
 * @spec SPEC-5.1b
 */
class PostgresCreateTableVariantsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_ctv_source (id INT PRIMARY KEY, val VARCHAR(255))',
            'CREATE TABLE pg_ctv_target (LIKE pg_ctv_source)',
            'CREATE TABLE pg_ctv_ctas AS SELECT * FROM pg_ctv_source',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_ctv_target', 'pg_ctv_ctas', 'pg_ctv_source', 'LIKE', 'AS'];
    }


    public function testCreateTableLike(): void
    {
        $this->pdo->exec('CREATE TABLE pg_ctv_target (LIKE pg_ctv_source)');

        $this->pdo->exec("INSERT INTO pg_ctv_target (id, val) VALUES (1, 'hello')");

        $stmt = $this->pdo->query('SELECT * FROM pg_ctv_target WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['val']);
    }

    public function testCreateTableAsSelect(): void
    {
        $this->pdo->exec("INSERT INTO pg_ctv_source (id, val) VALUES (1, 'hello')");
        $this->pdo->exec("INSERT INTO pg_ctv_source (id, val) VALUES (2, 'world')");

        $this->pdo->exec('CREATE TABLE pg_ctv_ctas AS SELECT * FROM pg_ctv_source');

        $stmt = $this->pdo->query('SELECT * FROM pg_ctv_ctas ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('hello', $rows[0]['val']);
        $this->assertSame('world', $rows[1]['val']);
    }

    public function testAlterTableThrowsOnPostgresql(): void
    {
        // ALTER TABLE is not supported on PostgreSQL — throws ZtdPdoException
        $this->expectException(ZtdPdoException::class);
        $this->pdo->exec('ALTER TABLE pg_ctv_source ADD COLUMN extra VARCHAR(100)');
    }
}
