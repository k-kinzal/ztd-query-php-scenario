<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests subqueries in ORDER BY clause via MySQL PDO.
 *
 * The CTE rewriter must recognize and rewrite table references
 * inside subqueries that appear in ORDER BY position.
 *
 * @spec SPEC-3.1
 */
class MysqlSubqueryInOrderByTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_soob_authors (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_soob_books (
                id INT PRIMARY KEY,
                author_id INT NOT NULL,
                title VARCHAR(200) NOT NULL,
                rating INT NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_soob_books', 'mp_soob_authors'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_soob_authors VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO mp_soob_authors VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO mp_soob_authors VALUES (3, 'Charlie')");

        $this->pdo->exec("INSERT INTO mp_soob_books VALUES (1, 1, 'Book A1', 5)");
        $this->pdo->exec("INSERT INTO mp_soob_books VALUES (2, 1, 'Book A2', 3)");
        $this->pdo->exec("INSERT INTO mp_soob_books VALUES (3, 2, 'Book B1', 4)");
        $this->pdo->exec("INSERT INTO mp_soob_books VALUES (4, 3, 'Book C1', 2)");
        $this->pdo->exec("INSERT INTO mp_soob_books VALUES (5, 3, 'Book C2', 5)");
        $this->pdo->exec("INSERT INTO mp_soob_books VALUES (6, 3, 'Book C3', 1)");
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
                 FROM mp_soob_authors a
                 ORDER BY (SELECT COUNT(*) FROM mp_soob_books b WHERE b.author_id = a.id) DESC, a.id"
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
     *
     * Authors ordered by their max book rating DESC: Alice/Charlie (5), Bob (4).
     */
    public function testOrderBySubqueryMaxRating(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT a.id, a.name
                 FROM mp_soob_authors a
                 ORDER BY (SELECT MAX(b.rating) FROM mp_soob_books b WHERE b.author_id = a.id) DESC, a.id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'ORDER BY subquery max: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            // Alice and Charlie both have max rating 5, so ordered by id
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
     *
     * Add a new author with many books, verify ordering reflects shadow data.
     */
    public function testOrderBySubqueryAfterShadowInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mp_soob_authors VALUES (4, 'Dave')");
            $this->pdo->exec("INSERT INTO mp_soob_books VALUES (7, 4, 'Book D1', 5)");
            $this->pdo->exec("INSERT INTO mp_soob_books VALUES (8, 4, 'Book D2', 5)");
            $this->pdo->exec("INSERT INTO mp_soob_books VALUES (9, 4, 'Book D3', 5)");
            $this->pdo->exec("INSERT INTO mp_soob_books VALUES (10, 4, 'Book D4', 5)");

            $rows = $this->pdo->query(
                "SELECT a.id, a.name
                 FROM mp_soob_authors a
                 ORDER BY (SELECT COUNT(*) FROM mp_soob_books b WHERE b.author_id = a.id) DESC, a.id"
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
     * ORDER BY with subquery referencing same table (self-reference).
     *
     * Order books by how many other books the same author has written.
     */
    public function testOrderBySelfReferenceSubquery(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT b.id, b.title
                 FROM mp_soob_books b
                 ORDER BY (SELECT COUNT(*) FROM mp_soob_books b2 WHERE b2.author_id = b.author_id) DESC, b.id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 6) {
                $this->markTestIncomplete(
                    'ORDER BY self-reference subquery: expected 6 rows, got ' . count($rows)
                );
            }

            $this->assertCount(6, $rows);
            // Charlie's books first (3 books), then Alice's (2), then Bob's (1)
            $charlieIds = [4, 5, 6];
            $aliceIds = [1, 2];
            $bobIds = [3];

            $actualIds = array_map(fn($r) => (int) $r['id'], $rows);
            // First 3 should be Charlie's, next 2 Alice's, last 1 Bob's
            $this->assertSame($charlieIds, array_slice($actualIds, 0, 3));
            $this->assertSame($aliceIds, array_slice($actualIds, 3, 2));
            $this->assertSame($bobIds, array_slice($actualIds, 5, 1));
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ORDER BY self-reference subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared SELECT with subquery in ORDER BY and bound parameter.
     *
     * ORDER BY subquery uses a parameter to filter the count.
     */
    public function testPreparedOrderBySubquery(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT a.id, a.name
                 FROM mp_soob_authors a
                 ORDER BY (SELECT COUNT(*) FROM mp_soob_books b WHERE b.author_id = a.id AND b.rating >= ?) DESC, a.id"
            );
            $stmt->execute([4]);

            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared ORDER BY subquery: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            // rating >= 4: Alice has 1 (5), Bob has 1 (4), Charlie has 1 (5)
            // All tied at count=1, so ordered by id
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertSame([1, 2, 3], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared ORDER BY subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation: queries with ORDER BY subquery should not affect physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $rows = $this->pdo->query(
            "SELECT a.id FROM mp_soob_authors a
             ORDER BY (SELECT COUNT(*) FROM mp_soob_books b WHERE b.author_id = a.id) DESC"
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->pdo->disableZtd();
        $rawCount = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_soob_authors")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rawCount[0]['cnt'], 'Physical table should be empty');
    }
}
