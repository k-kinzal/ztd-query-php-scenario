<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests three-table JOIN chain (authors -> books -> reviews) through ZTD shadow store.
 *
 * Verifies that the CTE rewriter can handle multiple table references in a single
 * query where all three tables need shadow data.
 * @spec SPEC-3.3
 */
class MysqlThreeTableJoinChainTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE ttj_authors (id INT PRIMARY KEY, name VARCHAR(50)) ENGINE=InnoDB',
            'CREATE TABLE ttj_books (id INT PRIMARY KEY, author_id INT, title VARCHAR(100), genre VARCHAR(30)) ENGINE=InnoDB',
            'CREATE TABLE ttj_reviews (id INT PRIMARY KEY, book_id INT, rating INT, reviewer VARCHAR(50)) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['ttj_reviews', 'ttj_books', 'ttj_authors'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO ttj_authors (id, name) VALUES (1, 'Tolkien'), (2, 'Asimov'), (3, 'Bradbury')");
        $this->pdo->exec("INSERT INTO ttj_books (id, author_id, title, genre) VALUES
            (1, 1, 'The Hobbit', 'Fantasy'), (2, 1, 'LOTR', 'Fantasy'),
            (3, 2, 'Foundation', 'SciFi'), (4, 3, 'Fahrenheit 451', 'SciFi')");
        $this->pdo->exec("INSERT INTO ttj_reviews (id, book_id, rating, reviewer) VALUES
            (1, 1, 5, 'Alice'), (2, 1, 4, 'Bob'),
            (3, 2, 5, 'Carol'), (4, 3, 3, 'Dave'),
            (5, 3, 4, 'Eve'), (6, 4, 5, 'Frank')");
    }

    /**
     * Three-table INNER JOIN: authors -> books -> reviews.
     */
    public function testThreeTableInnerJoin(): void
    {
        $rows = $this->ztdQuery(
            'SELECT a.name AS author, b.title, r.rating
             FROM ttj_authors a
             JOIN ttj_books b ON b.author_id = a.id
             JOIN ttj_reviews r ON r.book_id = b.id
             ORDER BY a.name, b.title'
        );

        $this->assertCount(6, $rows);
        // Asimov -> Foundation has 2 reviews (Dave:3, Eve:4)
        $this->assertSame('Asimov', $rows[0]['author']);
        $this->assertSame('Foundation', $rows[0]['title']);
        // Bradbury -> Fahrenheit 451 has 1 review (Frank:5)
        $this->assertSame('Bradbury', $rows[2]['author']);
        $this->assertSame('Fahrenheit 451', $rows[2]['title']);
        $this->assertEquals(5, (int) $rows[2]['rating']);
        // Tolkien -> LOTR has 1 review (Carol:5)
        $this->assertSame('Tolkien', $rows[3]['author']);
        $this->assertSame('LOTR', $rows[3]['title']);
        // Tolkien -> The Hobbit has 2 reviews (Alice:5, Bob:4)
        $this->assertSame('Tolkien', $rows[4]['author']);
        $this->assertSame('The Hobbit', $rows[4]['title']);
    }

    /**
     * Three-table JOIN with aggregate functions (COUNT, AVG).
     */
    public function testThreeTableWithAggregate(): void
    {
        $rows = $this->ztdQuery(
            'SELECT a.name AS author, COUNT(r.id) AS review_count, AVG(r.rating) AS avg_rating
             FROM ttj_authors a
             JOIN ttj_books b ON b.author_id = a.id
             JOIN ttj_reviews r ON r.book_id = b.id
             GROUP BY a.id, a.name
             ORDER BY avg_rating DESC'
        );

        $this->assertCount(3, $rows);
        $byAuthor = array_column($rows, null, 'author');
        // Bradbury: 1 review, avg 5.0
        $this->assertEquals(1, (int) $byAuthor['Bradbury']['review_count']);
        $this->assertEqualsWithDelta(5.0, (float) $byAuthor['Bradbury']['avg_rating'], 0.01);
        // Tolkien: 3 reviews, avg ~4.67
        $this->assertEquals(3, (int) $byAuthor['Tolkien']['review_count']);
        $this->assertEqualsWithDelta(4.67, (float) $byAuthor['Tolkien']['avg_rating'], 0.01);
        // Asimov: 2 reviews, avg 3.5
        $this->assertEquals(2, (int) $byAuthor['Asimov']['review_count']);
        $this->assertEqualsWithDelta(3.5, (float) $byAuthor['Asimov']['avg_rating'], 0.01);
    }

    /**
     * Three-table LEFT JOIN: includes authors/books with no reviews.
     */
    public function testThreeTableLeftJoin(): void
    {
        $rows = $this->ztdQuery(
            'SELECT a.name, b.title, COUNT(r.id) AS reviews
             FROM ttj_authors a
             LEFT JOIN ttj_books b ON b.author_id = a.id
             LEFT JOIN ttj_reviews r ON r.book_id = b.id
             GROUP BY a.id, a.name, b.id, b.title
             ORDER BY a.name, b.title'
        );

        // 4 rows: one per book (each author has at least one book)
        $this->assertCount(4, $rows);
        // Asimov -> Foundation: 2 reviews
        $this->assertSame('Asimov', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['reviews']);
        // Bradbury -> Fahrenheit 451: 1 review
        $this->assertSame('Bradbury', $rows[1]['name']);
        $this->assertEquals(1, (int) $rows[1]['reviews']);
        // Tolkien -> LOTR: 1 review
        $this->assertSame('Tolkien', $rows[2]['name']);
        $this->assertSame('LOTR', $rows[2]['title']);
        $this->assertEquals(1, (int) $rows[2]['reviews']);
        // Tolkien -> The Hobbit: 2 reviews
        $this->assertSame('Tolkien', $rows[3]['name']);
        $this->assertSame('The Hobbit', $rows[3]['title']);
        $this->assertEquals(2, (int) $rows[3]['reviews']);
    }

    /**
     * Mutation followed by three-table JOIN: new data should appear.
     */
    public function testThreeTableAfterMutations(): void
    {
        // Add a new book for Bradbury and a review for it
        $this->pdo->exec("INSERT INTO ttj_books (id, author_id, title, genre) VALUES (5, 3, 'The Martian Chronicles', 'SciFi')");
        $this->pdo->exec("INSERT INTO ttj_reviews (id, book_id, rating, reviewer) VALUES (7, 5, 4, 'Grace')");

        $rows = $this->ztdQuery(
            "SELECT a.name AS author, b.title, r.rating
             FROM ttj_authors a
             JOIN ttj_books b ON b.author_id = a.id
             JOIN ttj_reviews r ON r.book_id = b.id
             WHERE a.name = 'Bradbury'
             ORDER BY b.title"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Fahrenheit 451', $rows[0]['title']);
        $this->assertEquals(5, (int) $rows[0]['rating']);
        $this->assertSame('The Martian Chronicles', $rows[1]['title']);
        $this->assertEquals(4, (int) $rows[1]['rating']);
    }

    /**
     * Three-table JOIN with HAVING clause.
     */
    public function testThreeTableWithHaving(): void
    {
        $rows = $this->ztdQuery(
            'SELECT a.name, AVG(r.rating) AS avg_rating
             FROM ttj_authors a
             JOIN ttj_books b ON b.author_id = a.id
             JOIN ttj_reviews r ON r.book_id = b.id
             GROUP BY a.id, a.name
             HAVING AVG(r.rating) >= 4
             ORDER BY avg_rating DESC'
        );

        // Bradbury avg=5.0, Tolkien avg~4.67 qualify; Asimov avg=3.5 does not
        $this->assertCount(2, $rows);
        $this->assertSame('Bradbury', $rows[0]['name']);
        $this->assertEqualsWithDelta(5.0, (float) $rows[0]['avg_rating'], 0.01);
        $this->assertSame('Tolkien', $rows[1]['name']);
        $this->assertEqualsWithDelta(4.67, (float) $rows[1]['avg_rating'], 0.01);
    }

    /**
     * Physical isolation: all three tables should be empty when ZTD is disabled.
     */
    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM ttj_authors');
        $this->assertSame(0, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM ttj_books');
        $this->assertSame(0, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM ttj_reviews');
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
