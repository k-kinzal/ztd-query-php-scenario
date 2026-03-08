<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;
use Tests\Support\UserDto;

/**
 * Tests ZtdPdo::query() with fetchMode arguments on SQLite.
 *
 * SQLite variant of the query-with-fetchMode tests.
 * Uses in-memory database, no container needed.
 * @spec pending
 */
class SqliteQueryFetchModeArgsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sq_qfm_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['sq_qfm_test'];
    }


    /**
     * query() with FETCH_ASSOC.
     */
    public function testQueryWithFetchAssoc(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM sq_qfm_test ORDER BY id', PDO::FETCH_ASSOC);
        $rows = $stmt->fetchAll();
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * query() with FETCH_NUM.
     */
    public function testQueryWithFetchNum(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM sq_qfm_test ORDER BY id', PDO::FETCH_NUM);
        $rows = $stmt->fetchAll();
        $this->assertSame('Alice', $rows[0][1]);
    }

    /**
     * query() with FETCH_OBJ.
     */
    public function testQueryWithFetchObj(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM sq_qfm_test ORDER BY id', PDO::FETCH_OBJ);
        $rows = $stmt->fetchAll();
        $this->assertIsObject($rows[0]);
        $this->assertSame('Alice', $rows[0]->name);
    }

    /**
     * query() with FETCH_COLUMN + column index.
     */
    public function testQueryWithFetchColumn(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM sq_qfm_test ORDER BY id', PDO::FETCH_COLUMN, 1);
        $rows = $stmt->fetchAll();
        $this->assertSame(['Alice', 'Bob'], $rows);
    }

    /**
     * query() with FETCH_CLASS.
     */
    public function testQueryWithFetchClass(): void
    {
        $stmt = $this->pdo->query(
            'SELECT id, name FROM sq_qfm_test ORDER BY id',
            PDO::FETCH_CLASS,
            UserDto::class,
        );
        $rows = $stmt->fetchAll();
        $this->assertCount(2, $rows);
        $this->assertInstanceOf(UserDto::class, $rows[0]);
        $this->assertSame('Alice', $rows[0]->name);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sq_qfm_test', PDO::FETCH_COLUMN, 0);
        $count = $stmt->fetch();
        $this->assertSame(2, (int) $count);

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sq_qfm_test', PDO::FETCH_COLUMN, 0);
        $count = $stmt->fetch();
        $this->assertSame(0, (int) $count);
    }
}
