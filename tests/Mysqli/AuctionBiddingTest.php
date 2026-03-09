<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests an auction bidding system workflow through ZTD shadow store (MySQLi).
 * Covers MAX subquery for highest bid, bid placement with price update,
 * bid history, auction summary aggregation, closing auctions, and physical isolation.
 * @spec SPEC-10.2.85
 */
class AuctionBiddingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_ab_auctions (
                id INT PRIMARY KEY,
                title VARCHAR(255),
                start_price DECIMAL(10,2),
                current_price DECIMAL(10,2),
                end_time DATETIME,
                status VARCHAR(20)
            )',
            'CREATE TABLE mi_ab_bids (
                id INT PRIMARY KEY,
                auction_id INT,
                bidder_name VARCHAR(100),
                bid_amount DECIMAL(10,2),
                bid_time DATETIME
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_ab_bids', 'mi_ab_auctions'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 auctions
        $this->mysqli->query("INSERT INTO mi_ab_auctions VALUES (1, 'Vintage Watch', 100.00, 250.00, '2026-04-01 18:00:00', 'active')");
        $this->mysqli->query("INSERT INTO mi_ab_auctions VALUES (2, 'Rare Painting', 500.00, 750.00, '2026-03-15 20:00:00', 'active')");
        $this->mysqli->query("INSERT INTO mi_ab_auctions VALUES (3, 'Signed Book', 20.00, 20.00, '2026-05-01 12:00:00', 'active')");

        // 5 bids across auctions
        $this->mysqli->query("INSERT INTO mi_ab_bids VALUES (1, 1, 'Alice', 150.00, '2026-03-01 10:00:00')");
        $this->mysqli->query("INSERT INTO mi_ab_bids VALUES (2, 1, 'Bob', 200.00, '2026-03-02 11:00:00')");
        $this->mysqli->query("INSERT INTO mi_ab_bids VALUES (3, 1, 'Charlie', 250.00, '2026-03-03 12:00:00')");
        $this->mysqli->query("INSERT INTO mi_ab_bids VALUES (4, 2, 'Diana', 650.00, '2026-03-05 14:00:00')");
        $this->mysqli->query("INSERT INTO mi_ab_bids VALUES (5, 2, 'Eve', 750.00, '2026-03-06 15:00:00')");
    }

    /**
     * Place a bid higher than current price and update the auction's current_price.
     */
    public function testPlaceBid(): void
    {
        // Place a new bid on auction 1
        $this->mysqli->query("INSERT INTO mi_ab_bids VALUES (6, 1, 'Frank', 300.00, '2026-03-09 10:00:00')");
        $this->mysqli->query("UPDATE mi_ab_auctions SET current_price = 300.00 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT current_price FROM mi_ab_auctions WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(300.00, (float) $rows[0]['current_price'], 0.01);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_ab_bids WHERE auction_id = 1");
        $this->assertEquals(4, (int) $rows[0]['cnt']);
    }

    /**
     * Get the highest bid for an auction using MAX with a prepared statement.
     */
    public function testGetHighestBid(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT MAX(bid_amount) AS highest_bid FROM mi_ab_bids WHERE auction_id = ?",
            [1]
        );

        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(250.00, (float) $rows[0]['highest_bid'], 0.01);
    }

    /**
     * Bid history for an auction: JOIN bids with auctions, ordered by bid_amount DESC.
     */
    public function testBidHistory(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT b.bidder_name, b.bid_amount, b.bid_time, a.title
             FROM mi_ab_bids b
             JOIN mi_ab_auctions a ON a.id = b.auction_id
             WHERE b.auction_id = ?
             ORDER BY b.bid_amount DESC",
            [1]
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Charlie', $rows[0]['bidder_name']);
        $this->assertEqualsWithDelta(250.00, (float) $rows[0]['bid_amount'], 0.01);
        $this->assertSame('Bob', $rows[1]['bidder_name']);
        $this->assertEqualsWithDelta(200.00, (float) $rows[1]['bid_amount'], 0.01);
        $this->assertSame('Alice', $rows[2]['bidder_name']);
        $this->assertEqualsWithDelta(150.00, (float) $rows[2]['bid_amount'], 0.01);
    }

    /**
     * Auction summary: COUNT bids, MAX and MIN bid_amount per auction via JOIN.
     */
    public function testAuctionSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.title,
                    COUNT(b.id) AS bid_count,
                    MAX(b.bid_amount) AS max_bid,
                    MIN(b.bid_amount) AS min_bid
             FROM mi_ab_auctions a
             LEFT JOIN mi_ab_bids b ON b.auction_id = a.id
             GROUP BY a.id, a.title
             ORDER BY a.id"
        );

        $this->assertCount(3, $rows);

        // Vintage Watch: 3 bids, max 250, min 150
        $this->assertSame('Vintage Watch', $rows[0]['title']);
        $this->assertEquals(3, (int) $rows[0]['bid_count']);
        $this->assertEqualsWithDelta(250.00, (float) $rows[0]['max_bid'], 0.01);
        $this->assertEqualsWithDelta(150.00, (float) $rows[0]['min_bid'], 0.01);

        // Rare Painting: 2 bids, max 750, min 650
        $this->assertSame('Rare Painting', $rows[1]['title']);
        $this->assertEquals(2, (int) $rows[1]['bid_count']);
        $this->assertEqualsWithDelta(750.00, (float) $rows[1]['max_bid'], 0.01);
        $this->assertEqualsWithDelta(650.00, (float) $rows[1]['min_bid'], 0.01);

        // Signed Book: 0 bids
        $this->assertSame('Signed Book', $rows[2]['title']);
        $this->assertEquals(0, (int) $rows[2]['bid_count']);
    }

    /**
     * Close an auction and determine the winner via MAX(bid_amount) JOIN.
     */
    public function testCloseAuction(): void
    {
        $this->mysqli->query("UPDATE mi_ab_auctions SET status = 'closed' WHERE id = 1");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        // Verify status changed
        $rows = $this->ztdQuery("SELECT status FROM mi_ab_auctions WHERE id = 1");
        $this->assertSame('closed', $rows[0]['status']);

        // Determine winner: bidder with MAX bid_amount for this auction
        $rows = $this->ztdQuery(
            "SELECT b.bidder_name, b.bid_amount
             FROM mi_ab_bids b
             WHERE b.auction_id = 1
               AND b.bid_amount = (SELECT MAX(bid_amount) FROM mi_ab_bids WHERE auction_id = 1)"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Charlie', $rows[0]['bidder_name']);
        $this->assertEqualsWithDelta(250.00, (float) $rows[0]['bid_amount'], 0.01);
    }

    /**
     * Active bidder stats: COUNT(DISTINCT bidder_name) per auction, HAVING > 1.
     */
    public function testActiveBidderStats(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.title, COUNT(DISTINCT b.bidder_name) AS unique_bidders
             FROM mi_ab_auctions a
             JOIN mi_ab_bids b ON b.auction_id = a.id
             GROUP BY a.id, a.title
             HAVING COUNT(DISTINCT b.bidder_name) > 1
             ORDER BY unique_bidders DESC"
        );

        // Vintage Watch: 3 unique bidders; Rare Painting: 2 unique bidders
        $this->assertCount(2, $rows);
        $this->assertSame('Vintage Watch', $rows[0]['title']);
        $this->assertEquals(3, (int) $rows[0]['unique_bidders']);
        $this->assertSame('Rare Painting', $rows[1]['title']);
        $this->assertEquals(2, (int) $rows[1]['unique_bidders']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_ab_bids VALUES (6, 3, 'Zara', 50.00, '2026-03-09 09:00:00')");
        $this->mysqli->query("UPDATE mi_ab_auctions SET status = 'closed' WHERE id = 2");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_ab_bids");
        $this->assertSame(6, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT status FROM mi_ab_auctions WHERE id = 2");
        $this->assertSame('closed', $rows[0]['status']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ab_bids');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
