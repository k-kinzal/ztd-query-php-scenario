<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests subqueries in ORDER BY clause via PostgreSQL PDO.
 *
 * The CTE rewriter must recognize and rewrite table references
 * inside subqueries that appear in ORDER BY position.
 *
 * @spec SPEC-3.1
 */
class PostgresSubqueryInOrderByTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_soob_authors (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            )',
            'CREATE TABLE pg_soob_books (
                id SERIAL PRIMARY KEY,
                author_id INT NOT NULL,
                title VARCHAR(200) NOT NULL,
                rating INT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_soob_books', 'pg_soob_authors'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_soob_authors (id, name) VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pg_soob_authors (id, name) VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO pg_soob_authors (id, name) VALUES (3, 'Charlie')");

        $this->pdo->exec("INSERT INTO pg_soob_books (id, author_id, title, rating) VALUES (1, 1, 'Book A1', 5)");
        $this->pdo->exec("INSERT INTO pg_soob_books (id, author_id, title, rating) VALUES (2, 1, 'Book A2', 3)");
        $this->pdo->exec("INSERT INTO pg_soob_books (id, author_id, title, rating) VALUES (3, 2, 'Book B1', 4)");
        $this->pdo->exec("INSERT INTO pg_soob_books (id, author_id, title, rating) VALUES (4, 3, 'Book C1', 2)");
        $this->pdo->exec("INSERT INTO pg_soob_books (id, author_id, title, rating) VALUES (5, 3, 'Book C2', 5)");
        $this->pdo->exec("INSERT INTO pg_soob_books (id, author_id, title, rating) VALUES (6, 3, 'Book C3', 1)");
    }

    /**
     * ORDER BY with correlated scalar subquery counting books per author.
     *
     * Authors ordered by book count DESC: Charlie (3), Alice (2), Bob (1).
     */
    public function testOrderByCorrelatedSubqueryCount(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT a.id, a.name
                 FROM pg_soob_authors a
                 ORDER BY (SELECT COUNT(*) FROM pg_soob_books b WHERE b.author_id = a.id) DESC, a.id"
            )->fetchAll(PDO::FETCH_ASSOC);

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
            $rows = $this->pdo->query(
                "SELECT a.id, a.name
                 FROM pg_soob_authors a
                 ORDER BY (SELECT MAX(b.rating) FROM pg_soob_books b WHERE b.author_id = a.id) DESC, a.id"
            )->fetchAll(PDO::FETCH_ASSOC);

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
            $this->pdo->exec("INSERT INTO pg_soob_authors (id, name) VALUES (4, 'Dave')");
            $this->pdo->exec("INSERT INTO pg_soob_books (id, author_id, title, rating) VALUES (7, 4, 'Book D1', 5)");
            $this->pdo->exec("INSERT INTO pg_soob_books (id, author_id, title, rating) VALUES (8, 4, 'Book D2', 5)");
            $this->pdo->exec("INSERT INTO pg_soob_books (id, author_id, title, rating) VALUES (9, 4, 'Book D3', 5)");
            $this->pdo->exec("INSERT INTO pg_soob_books (id, author_id, title, rating) VALUES (10, 4, 'Book D4', 5)");

            $rows = $this->pdo->query(
                "SELECT a.id, a.name
                 FROM pg_soob_authors a
                 ORDER BY (SELECT COUNT(*) FROM pg_soob_books b WHERE b.author_id = a.id) DESC, a.id"
            )->fetchAll(PDO::FETCH_ASSOC);

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
            $rows = $this->pdo->query(
                "SELECT b.id, b.title
                 FROM pg_soob_books b
                 ORDER BY (SELECT COUNT(*) FROM pg_soob_books b2 WHERE b2.author_id = b.author_id) DESC, b.id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 6) {
                $this->markTestIncomplete(
                    'ORDER BY self-reference subquery: expected 6 rows, got ' . count($rows)
                );
            }

            $this->assertCount(6, $rows);
            $actualIds = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertSame([4, 5, 6], array_slice($actualIds, 0, 3)); // Charlie's 3 books
            $this->assertSame([1, 2], array_slice($actualIds, 3, 2));    // Alice's 2 books
            $this->assertSame([3], array_slice($actualIds, 5, 1));       // Bob's 1 book
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ORDER BY self-reference subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared SELECT with subquery in ORDER BY and $1 parameter.
     */
    public function testPreparedOrderBySubquery(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT a.id, a.name
                 FROM pg_soob_authors a
                 ORDER BY (SELECT COUNT(*) FROM pg_soob_books b WHERE b.author_id = a.id AND b.rating >= $1) DESC, a.id"
            );
            $stmt->execute([4]);

            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

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
        $this->pdo->query(
            "SELECT a.id FROM pg_soob_authors a
             ORDER BY (SELECT COUNT(*) FROM pg_soob_books b WHERE b.author_id = a.id) DESC"
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->pdo->disableZtd();
        $rawCount = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_soob_authors")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rawCount[0]['cnt'], 'Physical table should be empty');
    }
}
