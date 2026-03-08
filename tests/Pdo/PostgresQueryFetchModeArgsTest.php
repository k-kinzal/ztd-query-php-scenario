<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;
use Tests\Support\UserDto;

/**
 * Tests ZtdPdo::query() with fetchMode arguments on PostgreSQL.
 *
 * Same scenarios as MysqlQueryFetchModeArgsTest but against PostgreSQL.
 * Validates that query()'s setFetchMode($fetchMode, ...$fetchModeArgs)
 * works consistently across database drivers.
 * @spec pending
 */
class PostgresQueryFetchModeArgsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_qfm_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['pg_qfm_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_qfm_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pg_qfm_test (id, name, score) VALUES (2, 'Bob', 80)");
    }

    /**
     * query() with FETCH_ASSOC mode.
     */
    public function testQueryWithFetchAssoc(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM pg_qfm_test ORDER BY id', PDO::FETCH_ASSOC);
        $rows = $stmt->fetchAll();
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * query() with FETCH_NUM mode.
     */
    public function testQueryWithFetchNum(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM pg_qfm_test ORDER BY id', PDO::FETCH_NUM);
        $rows = $stmt->fetchAll();
        $this->assertSame('Alice', $rows[0][1]);
    }

    /**
     * query() with FETCH_OBJ mode.
     */
    public function testQueryWithFetchObj(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM pg_qfm_test ORDER BY id', PDO::FETCH_OBJ);
        $rows = $stmt->fetchAll();
        $this->assertIsObject($rows[0]);
        $this->assertSame('Alice', $rows[0]->name);
    }

    /**
     * query() with FETCH_COLUMN + column index.
     */
    public function testQueryWithFetchColumn(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM pg_qfm_test ORDER BY id', PDO::FETCH_COLUMN, 1);
        $rows = $stmt->fetchAll();
        $this->assertSame(['Alice', 'Bob'], $rows);
    }

    /**
     * query() with FETCH_CLASS + class name.
     */
    public function testQueryWithFetchClass(): void
    {
        $stmt = $this->pdo->query(
            'SELECT id, name FROM pg_qfm_test ORDER BY id',
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
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_qfm_test', PDO::FETCH_COLUMN, 0);
        $count = $stmt->fetch();
        $this->assertSame(2, (int) $count);

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_qfm_test', PDO::FETCH_COLUMN, 0);
        $count = $stmt->fetch();
        $this->assertSame(0, (int) $count);
    }
}
