<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Offset-based pagination on PostgreSQL via PDO.
 * @spec SPEC-3.1, SPEC-3.2
 */
class PostgresOffsetPaginationTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_op_items (id INT PRIMARY KEY, name VARCHAR(100), created_at DATE)';
    }

    protected function getTableNames(): array
    {
        return ['pg_op_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        for ($i = 1; $i <= 10; $i++) {
            $this->pdo->exec("INSERT INTO pg_op_items VALUES ({$i}, 'Item {$i}', '2024-01-{$i}')");
        }
    }

    public function testFirstPage(): void
    {
        $rows = $this->ztdQuery('SELECT name FROM pg_op_items ORDER BY id LIMIT 3 OFFSET 0');
        $this->assertCount(3, $rows);
        $this->assertSame('Item 1', $rows[0]['name']);
    }

    public function testSecondPage(): void
    {
        $rows = $this->ztdQuery('SELECT name FROM pg_op_items ORDER BY id LIMIT 3 OFFSET 3');
        $this->assertCount(3, $rows);
        $this->assertSame('Item 4', $rows[0]['name']);
    }

    public function testLastPartialPage(): void
    {
        $rows = $this->ztdQuery('SELECT name FROM pg_op_items ORDER BY id LIMIT 3 OFFSET 9');
        $this->assertCount(1, $rows);
        $this->assertSame('Item 10', $rows[0]['name']);
    }

    public function testOffsetBeyondData(): void
    {
        $rows = $this->ztdQuery('SELECT name FROM pg_op_items ORDER BY id LIMIT 3 OFFSET 20');
        $this->assertCount(0, $rows);
    }

    public function testPreparedPagination(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM pg_op_items ORDER BY id LIMIT ? OFFSET ?',
            [3, 0]
        );
        $this->assertCount(3, $rows);
        $this->assertSame('Item 1', $rows[0]['name']);
    }

    public function testPreparedPaginationPage2(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM pg_op_items ORDER BY id LIMIT ? OFFSET ?',
            [3, 3]
        );
        $this->assertCount(3, $rows);
        $this->assertSame('Item 4', $rows[0]['name']);
    }

    public function testPaginationAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO pg_op_items VALUES (11, 'Item 11', '2024-01-11')");

        $rows = $this->ztdQuery('SELECT name FROM pg_op_items ORDER BY id LIMIT 3 OFFSET 9');
        $this->assertCount(2, $rows);
        $this->assertSame('Item 10', $rows[0]['name']);
        $this->assertSame('Item 11', $rows[1]['name']);
    }

    public function testPaginationAfterDelete(): void
    {
        $this->pdo->exec('DELETE FROM pg_op_items WHERE id = 5');

        $rows = $this->ztdQuery('SELECT name FROM pg_op_items ORDER BY id LIMIT 3 OFFSET 3');
        $this->assertCount(3, $rows);
        $this->assertSame('Item 4', $rows[0]['name']);
        $this->assertSame('Item 6', $rows[1]['name']);
    }

    public function testTotalCountForPagination(): void
    {
        $rows = $this->ztdQuery('SELECT COUNT(*) AS total FROM pg_op_items');
        $this->assertSame(10, (int) $rows[0]['total']);
    }

    public function testPaginationWithWhereClause(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM pg_op_items WHERE id > ? ORDER BY id LIMIT ? OFFSET ?',
            [5, 2, 0]
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Item 6', $rows[0]['name']);
    }
}
