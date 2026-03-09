<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a product review/rating system through ZTD shadow store (SQLite PDO).
 * Covers per-product AVG/COUNT star ratings, "was this helpful" voting,
 * filtering by rating range, reviewer activity stats, and physical isolation.
 * @spec SPEC-10.2.108
 */
class SqliteReviewRatingTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_rr_products (
                id INTEGER PRIMARY KEY,
                name TEXT,
                category TEXT,
                price REAL
            )',
            'CREATE TABLE sl_rr_reviews (
                id INTEGER PRIMARY KEY,
                product_id INTEGER,
                user_id INTEGER,
                rating INTEGER,
                title TEXT,
                body TEXT,
                created_at TEXT
            )',
            'CREATE TABLE sl_rr_review_votes (
                id INTEGER PRIMARY KEY,
                review_id INTEGER,
                user_id INTEGER,
                helpful INTEGER
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_rr_review_votes', 'sl_rr_reviews', 'sl_rr_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Products
        $this->pdo->exec("INSERT INTO sl_rr_products VALUES (1, 'Laptop', 'electronics', 999.99)");
        $this->pdo->exec("INSERT INTO sl_rr_products VALUES (2, 'Headphones', 'electronics', 149.99)");
        $this->pdo->exec("INSERT INTO sl_rr_products VALUES (3, 'Mouse', 'accessories', 49.99)");

        // Reviews (8 total)
        $this->pdo->exec("INSERT INTO sl_rr_reviews VALUES (1, 1, 1, 5, 'Amazing laptop', 'Best purchase ever', '2026-01-15')");
        $this->pdo->exec("INSERT INTO sl_rr_reviews VALUES (2, 1, 2, 4, 'Good value', 'Works well for the price', '2026-01-20')");
        $this->pdo->exec("INSERT INTO sl_rr_reviews VALUES (3, 1, 3, 2, 'Disappointing', 'Battery life is poor', '2026-02-01')");
        $this->pdo->exec("INSERT INTO sl_rr_reviews VALUES (4, 2, 1, 5, 'Perfect sound', 'Crystal clear audio', '2026-01-18')");
        $this->pdo->exec("INSERT INTO sl_rr_reviews VALUES (5, 2, 4, 3, 'Decent', 'Okay for the price', '2026-02-05')");
        $this->pdo->exec("INSERT INTO sl_rr_reviews VALUES (6, 2, 5, 4, 'Great headphones', 'Comfortable fit', '2026-02-10')");
        $this->pdo->exec("INSERT INTO sl_rr_reviews VALUES (7, 3, 2, 5, 'Best mouse ever', 'Precise and ergonomic', '2026-01-25')");
        $this->pdo->exec("INSERT INTO sl_rr_reviews VALUES (8, 3, 6, 1, 'Broke quickly', 'Stopped working after a week', '2026-02-15')");

        // Review votes (~10)
        $this->pdo->exec("INSERT INTO sl_rr_review_votes VALUES (1, 1, 4, 1)");
        $this->pdo->exec("INSERT INTO sl_rr_review_votes VALUES (2, 1, 5, 1)");
        $this->pdo->exec("INSERT INTO sl_rr_review_votes VALUES (3, 2, 3, 1)");
        $this->pdo->exec("INSERT INTO sl_rr_review_votes VALUES (4, 2, 6, 0)");
        $this->pdo->exec("INSERT INTO sl_rr_review_votes VALUES (5, 3, 1, 0)");
        $this->pdo->exec("INSERT INTO sl_rr_review_votes VALUES (6, 4, 2, 1)");
        $this->pdo->exec("INSERT INTO sl_rr_review_votes VALUES (7, 4, 3, 1)");
        $this->pdo->exec("INSERT INTO sl_rr_review_votes VALUES (8, 4, 6, 1)");
        $this->pdo->exec("INSERT INTO sl_rr_review_votes VALUES (9, 7, 1, 1)");
        $this->pdo->exec("INSERT INTO sl_rr_review_votes VALUES (10, 8, 3, 0)");
    }

    /**
     * AVG(rating) and COUNT(*) per product, ordered by average descending.
     */
    public function testAverageRatingPerProduct(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name,
                    COUNT(r.id) AS review_count,
                    ROUND(AVG(r.rating), 2) AS avg_rating
             FROM sl_rr_products p
             JOIN sl_rr_reviews r ON r.product_id = p.id
             GROUP BY p.id, p.name
             ORDER BY avg_rating DESC"
        );

        $this->assertCount(3, $rows);

        // Headphones: (5+3+4)/3 = 4.00
        $this->assertSame('Headphones', $rows[0]['name']);
        $this->assertEquals(3, (int) $rows[0]['review_count']);
        $this->assertEqualsWithDelta(4.0, (float) $rows[0]['avg_rating'], 0.01);

        // Laptop: (5+4+2)/3 = 3.67
        $this->assertSame('Laptop', $rows[1]['name']);
        $this->assertEquals(3, (int) $rows[1]['review_count']);
        $this->assertEqualsWithDelta(3.67, (float) $rows[1]['avg_rating'], 0.01);

        // Mouse: (5+1)/2 = 3.00
        $this->assertSame('Mouse', $rows[2]['name']);
        $this->assertEquals(2, (int) $rows[2]['review_count']);
        $this->assertEqualsWithDelta(3.0, (float) $rows[2]['avg_rating'], 0.01);
    }

    /**
     * Prepared statement filtering reviews by rating range (BETWEEN ? AND ?).
     */
    public function testFilterByRatingRange(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT r.id, r.title, r.rating
             FROM sl_rr_reviews r
             WHERE r.rating BETWEEN ? AND ?
             ORDER BY r.rating DESC, r.id",
            [4, 5]
        );

        // Ratings 4-5: reviews 1(5), 4(5), 7(5), 2(4), 6(4)
        $this->assertCount(5, $rows);
        $this->assertEquals(5, (int) $rows[0]['rating']);
        $this->assertEquals(4, (int) $rows[3]['rating']);
    }

    /**
     * Most helpful reviews: JOIN reviews + votes, SUM(helpful), ORDER BY helpfulness DESC.
     */
    public function testMostHelpfulReviews(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.id AS review_id,
                    r.title,
                    r.rating,
                    COALESCE(SUM(v.helpful), 0) AS helpful_count
             FROM sl_rr_reviews r
             LEFT JOIN sl_rr_review_votes v ON v.review_id = r.id
             GROUP BY r.id, r.title, r.rating
             ORDER BY helpful_count DESC, r.id"
        );

        $this->assertCount(8, $rows);

        // Review 4 (Perfect sound): 3 helpful votes
        $this->assertEquals(4, (int) $rows[0]['review_id']);
        $this->assertEquals(3, (int) $rows[0]['helpful_count']);

        // Review 1 (Amazing laptop): 2 helpful votes
        $this->assertEquals(1, (int) $rows[1]['review_id']);
        $this->assertEquals(2, (int) $rows[1]['helpful_count']);
    }

    /**
     * Star distribution using CASE WHEN for 5-star through 1-star counts.
     */
    public function testReviewDistribution(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                COUNT(CASE WHEN r.rating = 5 THEN 1 END) AS five_star,
                COUNT(CASE WHEN r.rating = 4 THEN 1 END) AS four_star,
                COUNT(CASE WHEN r.rating = 3 THEN 1 END) AS three_star,
                COUNT(CASE WHEN r.rating = 2 THEN 1 END) AS two_star,
                COUNT(CASE WHEN r.rating = 1 THEN 1 END) AS one_star,
                COUNT(*) AS total
             FROM sl_rr_reviews r"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(3, (int) $rows[0]['five_star']);
        $this->assertEquals(2, (int) $rows[0]['four_star']);
        $this->assertEquals(1, (int) $rows[0]['three_star']);
        $this->assertEquals(1, (int) $rows[0]['two_star']);
        $this->assertEquals(1, (int) $rows[0]['one_star']);
        $this->assertEquals(8, (int) $rows[0]['total']);
    }

    /**
     * INSERT a new review and verify the product average updates.
     */
    public function testAddReviewAndVerify(): void
    {
        // Add a 3-star review for Laptop
        $this->pdo->exec("INSERT INTO sl_rr_reviews VALUES (9, 1, 6, 3, 'Its okay', 'Average laptop', '2026-03-01')");

        $rows = $this->ztdQuery(
            "SELECT COUNT(r.id) AS review_count,
                    ROUND(AVG(r.rating), 2) AS avg_rating
             FROM sl_rr_reviews r
             WHERE r.product_id = 1"
        );

        // Laptop: (5+4+2+3)/4 = 3.50
        $this->assertEquals(4, (int) $rows[0]['review_count']);
        $this->assertEqualsWithDelta(3.50, (float) $rows[0]['avg_rating'], 0.01);
    }

    /**
     * INSERT a vote and verify helpful count changes.
     */
    public function testVoteOnReviewAndVerify(): void
    {
        // Vote review 7 as helpful
        $this->pdo->exec("INSERT INTO sl_rr_review_votes VALUES (11, 7, 5, 1)");

        $rows = $this->ztdQuery(
            "SELECT COALESCE(SUM(v.helpful), 0) AS helpful_count
             FROM sl_rr_review_votes v
             WHERE v.review_id = 7"
        );

        // Review 7 had 1 helpful vote, now has 2
        $this->assertEquals(2, (int) $rows[0]['helpful_count']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_rr_reviews VALUES (9, 1, 6, 3, 'Its okay', 'Average laptop', '2026-03-01')");
        $this->pdo->exec("INSERT INTO sl_rr_review_votes VALUES (11, 9, 1, 1)");

        // ZTD sees changes
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_rr_reviews");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_rr_review_votes");
        $this->assertEquals(11, (int) $rows[0]['cnt']);

        // Physical tables untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_rr_reviews")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
