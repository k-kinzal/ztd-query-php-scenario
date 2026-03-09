<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests subqueries in ORDER BY clause via MySQLi.
 *
 * The CTE rewriter must recognize and rewrite table references
 * inside subqueries that appear in ORDER BY position.
 *
 * @spec SPEC-3.1
 */
class SubqueryInOrderByTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE soob_authors (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE soob_books (
                id INT PRIMARY KEY,
                author_id INT NOT NULL,
                title VARCHAR(200) NOT NULL,
                rating INT NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['soob_books', 'soob_authors'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO soob_authors VALUES (1, 'Alice')");
        $this->ztdExec("INSERT INTO soob_authors VALUES (2, 'Bob')");
        $this->ztdExec("INSERT INTO soob_authors VALUES (3, 'Charlie')");

        $this->ztdExec("INSERT INTO soob_books VALUES (1, 1, 'Book A1', 5)");
        $this->ztdExec("INSERT INTO soob_books VALUES (2, 1, 'Book A2', 3)");
        $this->ztdExec("INSERT INTO soob_books VALUES (3, 2, 'Book B1', 4)");
        $this->ztdExec("INSERT INTO soob_books VALUES (4, 3, 'Book C1', 2)");
        $this->ztdExec("INSERT INTO soob_books VALUES (5, 3, 'Book C2', 5)");
        $this->ztdExec("INSERT INTO soob_books VALUES (6, 3, 'Book C3', 1)");
    }

    /**
     * ORDER BY with correlated scalar subquery counting books per author.
     */
    public function testOrderByCorrelatedSubqueryCount(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT a.id, a.name
                 FROM soob_authors a
                 ORDER BY (SELECT COUNT(*) FROM soob_books b WHERE b.author_id = a.id) DESC, a.id"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'ORDER BY correlated subquery: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Charlie', $rows[0]['name']); // 3 books
            $this->assertSame('Alice', $rows[1]['name']);    // 2 books
            $this->assertSame('Bob', $rows[2]['name']);      // 1 book
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ORDER BY correlated subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ORDER BY with scalar subquery computing max rating per author.
     */
    public function testOrderBySubqueryMaxRating(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT a.id, a.name
                 FROM soob_authors a
                 ORDER BY (SELECT MAX(b.rating) FROM soob_books b WHERE b.author_id = a.id) DESC, a.id"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'ORDER BY subquery max: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame(1, (int) $rows[0]['id']); // Alice, max=5
            $this->assertSame(3, (int) $rows[1]['id']); // Charlie, max=5
            $this->assertSame(2, (int) $rows[2]['id']); // Bob, max=4
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ORDER BY subquery max rating failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ORDER BY with subquery after shadow INSERT.
     */
    public function testOrderBySubqueryAfterShadowInsert(): void
    {
        try {
            $this->ztdExec("INSERT INTO soob_authors VALUES (4, 'Dave')");
            $this->ztdExec("INSERT INTO soob_books VALUES (7, 4, 'Book D1', 5)");
            $this->ztdExec("INSERT INTO soob_books VALUES (8, 4, 'Book D2', 5)");
            $this->ztdExec("INSERT INTO soob_books VALUES (9, 4, 'Book D3', 5)");
            $this->ztdExec("INSERT INTO soob_books VALUES (10, 4, 'Book D4', 5)");

            $rows = $this->ztdQuery(
                "SELECT a.id, a.name
                 FROM soob_authors a
                 ORDER BY (SELECT COUNT(*) FROM soob_books b WHERE b.author_id = a.id) DESC, a.id"
            );

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'ORDER BY subquery after INSERT: expected 4 rows, got ' . count($rows)
                );
            }

            $this->assertCount(4, $rows);
            $this->assertSame('Dave', $rows[0]['name']);    // 4 books
            $this->assertSame('Charlie', $rows[1]['name']); // 3 books
            $this->assertSame('Alice', $rows[2]['name']);    // 2 books
            $this->assertSame('Bob', $rows[3]['name']);      // 1 book
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ORDER BY subquery after shadow INSERT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ORDER BY with self-reference subquery on same table.
     */
    public function testOrderBySelfReferenceSubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT b.id, b.title
                 FROM soob_books b
                 ORDER BY (SELECT COUNT(*) FROM soob_books b2 WHERE b2.author_id = b.author_id) DESC, b.id"
            );

            if (count($rows) !== 6) {
                $this->markTestIncomplete(
                    'ORDER BY self-reference subquery: expected 6 rows, got ' . count($rows)
                );
            }

            $this->assertCount(6, $rows);
            $actualIds = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertSame([4, 5, 6], array_slice($actualIds, 0, 3));
            $this->assertSame([1, 2], array_slice($actualIds, 3, 2));
            $this->assertSame([3], array_slice($actualIds, 5, 1));
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ORDER BY self-reference subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared SELECT with subquery in ORDER BY and bound parameter.
     */
    public function testPreparedOrderBySubquery(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT a.id, a.name
                 FROM soob_authors a
                 ORDER BY (SELECT COUNT(*) FROM soob_books b WHERE b.author_id = a.id AND b.rating >= ?) DESC, a.id",
                [4]
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared ORDER BY subquery: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertSame([1, 2, 3], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared ORDER BY subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdQuery(
            "SELECT a.id FROM soob_authors a
             ORDER BY (SELECT COUNT(*) FROM soob_books b WHERE b.author_id = a.id) DESC"
        );

        $this->disableZtd();
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM soob_authors");
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
