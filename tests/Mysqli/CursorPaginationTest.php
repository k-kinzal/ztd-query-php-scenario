<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Cursor-based (keyset) pagination: WHERE id > ? ORDER BY id LIMIT N.
 * Standard pattern for REST APIs and infinite-scroll UIs.
 * @spec SPEC-3.1, SPEC-3.2
 */
class CursorPaginationTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_cp_articles (id INT PRIMARY KEY AUTO_INCREMENT, title VARCHAR(255), published_at DATE)';
    }

    protected function getTableNames(): array
    {
        return ['mi_cp_articles'];
    }

    private function seed(int $count = 10): void
    {
        for ($i = 1; $i <= $count; $i++) {
            $this->mysqli->query("INSERT INTO mi_cp_articles (id, title, published_at) VALUES ({$i}, 'Article {$i}', '2024-01-{$i}')");
        }
    }

    public function testFirstPage(): void
    {
        $this->seed();

        $rows = $this->ztdQuery('SELECT id, title FROM mi_cp_articles ORDER BY id ASC LIMIT 3');
        $this->assertCount(3, $rows);
        $this->assertSame(1, (int) $rows[0]['id']);
        $this->assertSame(3, (int) $rows[2]['id']);
    }

    public function testSecondPageWithCursor(): void
    {
        $this->seed();

        $rows = $this->ztdQuery('SELECT id, title FROM mi_cp_articles WHERE id > 3 ORDER BY id ASC LIMIT 3');
        $this->assertCount(3, $rows);
        $this->assertSame(4, (int) $rows[0]['id']);
        $this->assertSame(6, (int) $rows[2]['id']);
    }

    public function testLastPagePartial(): void
    {
        $this->seed();

        $rows = $this->ztdQuery('SELECT id, title FROM mi_cp_articles WHERE id > 8 ORDER BY id ASC LIMIT 3');
        $this->assertCount(2, $rows);
        $this->assertSame(9, (int) $rows[0]['id']);
        $this->assertSame(10, (int) $rows[1]['id']);
    }

    public function testEmptyPageBeyondData(): void
    {
        $this->seed();

        $rows = $this->ztdQuery('SELECT id, title FROM mi_cp_articles WHERE id > 10 ORDER BY id ASC LIMIT 3');
        $this->assertCount(0, $rows);
    }

    public function testPreparedCursorPagination(): void
    {
        $this->seed();

        $page1 = $this->ztdPrepareAndExecute(
            'SELECT id, title FROM mi_cp_articles WHERE id > ? ORDER BY id ASC LIMIT 3',
            [0]
        );
        $this->assertCount(3, $page1);
        $lastId = (int) $page1[2]['id'];

        $page2 = $this->ztdPrepareAndExecute(
            'SELECT id, title FROM mi_cp_articles WHERE id > ? ORDER BY id ASC LIMIT 3',
            [$lastId]
        );
        $this->assertCount(3, $page2);
        $this->assertSame($lastId + 1, (int) $page2[0]['id']);
    }

    public function testCursorPaginationAfterInsert(): void
    {
        $this->seed(5);

        $rows = $this->ztdQuery('SELECT id FROM mi_cp_articles WHERE id > 3 ORDER BY id ASC LIMIT 10');
        $this->assertCount(2, $rows);

        // Insert more rows, then re-paginate
        $this->mysqli->query("INSERT INTO mi_cp_articles (id, title, published_at) VALUES (6, 'Article 6', '2024-01-06')");
        $this->mysqli->query("INSERT INTO mi_cp_articles (id, title, published_at) VALUES (7, 'Article 7', '2024-01-07')");

        $rows = $this->ztdQuery('SELECT id FROM mi_cp_articles WHERE id > 3 ORDER BY id ASC LIMIT 10');
        $this->assertCount(4, $rows);
    }

    public function testCursorPaginationAfterDelete(): void
    {
        $this->seed();

        // Delete row 5 from the middle
        $this->mysqli->query("DELETE FROM mi_cp_articles WHERE id = 5");

        $rows = $this->ztdQuery('SELECT id FROM mi_cp_articles WHERE id > 3 ORDER BY id ASC LIMIT 3');
        $this->assertCount(3, $rows);
        // id=5 is gone, so we get 4, 6, 7
        $this->assertSame(4, (int) $rows[0]['id']);
        $this->assertSame(6, (int) $rows[1]['id']);
        $this->assertSame(7, (int) $rows[2]['id']);
    }

    public function testDescendingCursorPagination(): void
    {
        $this->seed();

        $rows = $this->ztdQuery('SELECT id FROM mi_cp_articles WHERE id < 8 ORDER BY id DESC LIMIT 3');
        $this->assertCount(3, $rows);
        $this->assertSame(7, (int) $rows[0]['id']);
        $this->assertSame(5, (int) $rows[2]['id']);
    }
}
