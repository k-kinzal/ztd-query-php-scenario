<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SELECT...FOR UPDATE behavior on SQLite.
 *
 * SQLite does not support FOR UPDATE syntax — it should throw.
 * @spec SPEC-10.2.11
 */
class SqliteSelectForUpdateTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sfu_test (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['sfu_test'];
    }



    protected function setUp(): void
    {
        parent::setUp();

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
