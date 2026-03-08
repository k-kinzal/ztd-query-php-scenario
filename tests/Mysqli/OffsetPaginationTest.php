<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Offset-based pagination (LIMIT/OFFSET) with prepared parameters.
 * Common web app pattern for paginated lists.
 * @spec SPEC-3.1, SPEC-3.2
 */
class OffsetPaginationTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_op_items (id INT PRIMARY KEY, name VARCHAR(100), created_at DATE)';
    }

    protected function getTableNames(): array
    {
        return ['mi_op_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        for ($i = 1; $i <= 10; $i++) {
            $this->mysqli->query("INSERT INTO mi_op_items VALUES ({$i}, 'Item {$i}', '2024-01-{$i}')");
        }
    }

    public function testFirstPage(): void
    {
        $rows = $this->ztdQuery('SELECT name FROM mi_op_items ORDER BY id LIMIT 3 OFFSET 0');
        $this->assertCount(3, $rows);
        $this->assertSame('Item 1', $rows[0]['name']);
        $this->assertSame('Item 3', $rows[2]['name']);
    }

    public function testSecondPage(): void
    {
        $rows = $this->ztdQuery('SELECT name FROM mi_op_items ORDER BY id LIMIT 3 OFFSET 3');
        $this->assertCount(3, $rows);
        $this->assertSame('Item 4', $rows[0]['name']);
        $this->assertSame('Item 6', $rows[2]['name']);
    }

    public function testLastPartialPage(): void
    {
        $rows = $this->ztdQuery('SELECT name FROM mi_op_items ORDER BY id LIMIT 3 OFFSET 9');
        $this->assertCount(1, $rows);
        $this->assertSame('Item 10', $rows[0]['name']);
    }

    public function testOffsetBeyondData(): void
    {
        $rows = $this->ztdQuery('SELECT name FROM mi_op_items ORDER BY id LIMIT 3 OFFSET 20');
        $this->assertCount(0, $rows);
    }

    public function testPreparedPagination(): void
    {
        $stmt = $this->mysqli->prepare('SELECT name FROM mi_op_items ORDER BY id LIMIT ? OFFSET ?');
        $limit = 3;
        $offset = 0;
        $stmt->bind_param('ii', $limit, $offset);

        // Page 1
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Item 1', $rows[0]['name']);

        // Re-prepare for page 2 (snapshot is frozen at prepare time)
        $stmt2 = $this->mysqli->prepare('SELECT name FROM mi_op_items ORDER BY id LIMIT ? OFFSET ?');
        $offset2 = 3;
        $stmt2->bind_param('ii', $limit, $offset2);
        $stmt2->execute();
        $result2 = $stmt2->get_result();
        $rows2 = $result2->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows2);
        $this->assertSame('Item 4', $rows2[0]['name']);
    }

    public function testPaginationAfterInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_op_items VALUES (11, 'Item 11', '2024-01-11')");

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_op_items');
        $this->assertSame(11, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery('SELECT name FROM mi_op_items ORDER BY id LIMIT 3 OFFSET 9');
        $this->assertCount(2, $rows);
        $this->assertSame('Item 10', $rows[0]['name']);
        $this->assertSame('Item 11', $rows[1]['name']);
    }

    public function testPaginationAfterDelete(): void
    {
        $this->mysqli->query('DELETE FROM mi_op_items WHERE id = 5');

        $rows = $this->ztdQuery('SELECT name FROM mi_op_items ORDER BY id LIMIT 3 OFFSET 3');
        $this->assertCount(3, $rows);
        $this->assertSame('Item 4', $rows[0]['name']);
        $this->assertSame('Item 6', $rows[1]['name']);
        $this->assertSame('Item 7', $rows[2]['name']);
    }

    public function testTotalCountForPagination(): void
    {
        $total = $this->ztdQuery('SELECT COUNT(*) AS total FROM mi_op_items');
        $this->assertSame(10, (int) $total[0]['total']);

        $pageSize = 3;
        $expectedPages = (int) ceil(10 / $pageSize);
        $this->assertSame(4, $expectedPages);
    }

    public function testPaginationWithWhereClause(): void
    {
        $rows = $this->ztdQuery("SELECT name FROM mi_op_items WHERE id > 5 ORDER BY id LIMIT 2 OFFSET 0");
        $this->assertCount(2, $rows);
        $this->assertSame('Item 6', $rows[0]['name']);
        $this->assertSame('Item 7', $rows[1]['name']);
    }
}
