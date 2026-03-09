<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests Customer RFM (Recency, Frequency, Monetary) segmentation — exercises
 * NTILE window function, multiple aggregations (COUNT, SUM, MAX), and
 * CASE-based composite scoring (MySQL PDO).
 * @spec SPEC-10.2.125
 */
class MysqlCustomerRfmSegmentationTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_rfm_customers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                signup_date VARCHAR(255)
            )',
            'CREATE TABLE mp_rfm_orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                customer_id INT,
                order_date VARCHAR(255),
                amount DECIMAL(10,2)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_rfm_orders', 'mp_rfm_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Customers
        $this->pdo->exec("INSERT INTO mp_rfm_customers VALUES (1, 'alice',   '2025-01-15')");
        $this->pdo->exec("INSERT INTO mp_rfm_customers VALUES (2, 'bob',     '2025-03-20')");
        $this->pdo->exec("INSERT INTO mp_rfm_customers VALUES (3, 'charlie', '2025-06-01')");
        $this->pdo->exec("INSERT INTO mp_rfm_customers VALUES (4, 'diana',   '2025-02-10')");
        $this->pdo->exec("INSERT INTO mp_rfm_customers VALUES (5, 'eve',     '2025-08-05')");
        $this->pdo->exec("INSERT INTO mp_rfm_customers VALUES (6, 'frank',   '2025-04-12')");
        $this->pdo->exec("INSERT INTO mp_rfm_customers VALUES (7, 'grace',   '2025-05-25')");
        $this->pdo->exec("INSERT INTO mp_rfm_customers VALUES (8, 'hank',    '2025-07-01')");

        // Orders
        $this->pdo->exec("INSERT INTO mp_rfm_orders VALUES (1,  1, '2025-10-01', 150.00)");
        $this->pdo->exec("INSERT INTO mp_rfm_orders VALUES (2,  1, '2025-09-15', 200.00)");
        $this->pdo->exec("INSERT INTO mp_rfm_orders VALUES (3,  1, '2025-08-01', 75.00)");
        $this->pdo->exec("INSERT INTO mp_rfm_orders VALUES (4,  1, '2025-07-10', 300.00)");
        $this->pdo->exec("INSERT INTO mp_rfm_orders VALUES (5,  1, '2025-06-20', 50.00)");
        $this->pdo->exec("INSERT INTO mp_rfm_orders VALUES (6,  2, '2025-10-05', 500.00)");
        $this->pdo->exec("INSERT INTO mp_rfm_orders VALUES (7,  2, '2025-08-12', 450.00)");
        $this->pdo->exec("INSERT INTO mp_rfm_orders VALUES (8,  2, '2025-05-30', 350.00)");
        $this->pdo->exec("INSERT INTO mp_rfm_orders VALUES (9,  3, '2025-09-20', 80.00)");
        $this->pdo->exec("INSERT INTO mp_rfm_orders VALUES (10, 3, '2025-07-15', 60.00)");
        $this->pdo->exec("INSERT INTO mp_rfm_orders VALUES (11, 4, '2025-04-01', 1200.00)");
        $this->pdo->exec("INSERT INTO mp_rfm_orders VALUES (12, 5, '2025-03-10', 30.00)");
        $this->pdo->exec("INSERT INTO mp_rfm_orders VALUES (13, 6, '2025-10-08', 120.00)");
        $this->pdo->exec("INSERT INTO mp_rfm_orders VALUES (14, 6, '2025-09-25', 180.00)");
        $this->pdo->exec("INSERT INTO mp_rfm_orders VALUES (15, 6, '2025-09-01', 90.00)");
        $this->pdo->exec("INSERT INTO mp_rfm_orders VALUES (16, 7, '2025-08-20', 250.00)");
        $this->pdo->exec("INSERT INTO mp_rfm_orders VALUES (17, 7, '2025-06-10', 200.00)");
        $this->pdo->exec("INSERT INTO mp_rfm_orders VALUES (18, 8, '2025-05-15', 100.00)");
    }

    /**
     * Recency ranking: customers ordered by their most recent order date.
     */
    public function testRecencyRanking(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.id, c.name, MAX(o.order_date) AS last_order
             FROM mp_rfm_customers c
             JOIN mp_rfm_orders o ON o.customer_id = c.id
             GROUP BY c.id, c.name
             ORDER BY last_order DESC"
        );

        $this->assertCount(8, $rows);

        // Frank is most recent (2025-10-08)
        $this->assertSame('frank', $rows[0]['name']);
        $this->assertSame('2025-10-08', $rows[0]['last_order']);

        // Bob is second (2025-10-05)
        $this->assertSame('bob', $rows[1]['name']);
        $this->assertSame('2025-10-05', $rows[1]['last_order']);

        // Eve is last (2025-03-10)
        $this->assertSame('eve', $rows[7]['name']);
        $this->assertSame('2025-03-10', $rows[7]['last_order']);
    }

    /**
     * Frequency ranking: customers ordered by number of orders.
     */
    public function testFrequencyRanking(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.id, c.name, COUNT(o.id) AS order_count
             FROM mp_rfm_customers c
             JOIN mp_rfm_orders o ON o.customer_id = c.id
             GROUP BY c.id, c.name
             ORDER BY order_count DESC, c.name"
        );

        $this->assertCount(8, $rows);

        // Alice: 5 orders
        $this->assertSame('alice', $rows[0]['name']);
        $this->assertEquals(5, (int) $rows[0]['order_count']);

        // Bob and Frank: 3 orders each
        $this->assertSame('bob', $rows[1]['name']);
        $this->assertEquals(3, (int) $rows[1]['order_count']);
        $this->assertSame('frank', $rows[2]['name']);
        $this->assertEquals(3, (int) $rows[2]['order_count']);

        // Charlie and Grace: 2 orders each
        $this->assertSame('charlie', $rows[3]['name']);
        $this->assertEquals(2, (int) $rows[3]['order_count']);
        $this->assertSame('grace', $rows[4]['name']);
        $this->assertEquals(2, (int) $rows[4]['order_count']);

        // Diana, Eve, Hank: 1 order each
        $this->assertSame('diana', $rows[5]['name']);
        $this->assertEquals(1, (int) $rows[5]['order_count']);
        $this->assertSame('eve', $rows[6]['name']);
        $this->assertEquals(1, (int) $rows[6]['order_count']);
        $this->assertSame('hank', $rows[7]['name']);
        $this->assertEquals(1, (int) $rows[7]['order_count']);
    }

    /**
     * Monetary ranking: customers ordered by total spending.
     */
    public function testMonetaryRanking(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.id, c.name, SUM(o.amount) AS total_spent
             FROM mp_rfm_customers c
             JOIN mp_rfm_orders o ON o.customer_id = c.id
             GROUP BY c.id, c.name
             ORDER BY total_spent DESC"
        );

        $this->assertCount(8, $rows);

        // Bob: 500+450+350 = 1300
        $this->assertSame('bob', $rows[0]['name']);
        $this->assertEqualsWithDelta(1300.00, (float) $rows[0]['total_spent'], 0.01);

        // Diana: 1200
        $this->assertSame('diana', $rows[1]['name']);
        $this->assertEqualsWithDelta(1200.00, (float) $rows[1]['total_spent'], 0.01);

        // Alice: 150+200+75+300+50 = 775
        $this->assertSame('alice', $rows[2]['name']);
        $this->assertEqualsWithDelta(775.00, (float) $rows[2]['total_spent'], 0.01);
    }

    /**
     * RFM scoring using NTILE(4) window function to assign quartile scores.
     * With 8 rows and NTILE(4), each group gets exactly 2 rows.
     */
    public function testRfmScoring(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, order_count, total_spent, last_order,
                    NTILE(4) OVER (ORDER BY last_order DESC) AS recency_score,
                    NTILE(4) OVER (ORDER BY order_count DESC) AS frequency_score,
                    NTILE(4) OVER (ORDER BY total_spent DESC) AS monetary_score
             FROM (
                 SELECT c.id, c.name,
                        COUNT(o.id) AS order_count,
                        SUM(o.amount) AS total_spent,
                        MAX(o.order_date) AS last_order
                 FROM mp_rfm_customers c
                 JOIN mp_rfm_orders o ON o.customer_id = c.id
                 GROUP BY c.id, c.name
             ) customer_metrics
             ORDER BY name"
        );

        $this->assertCount(8, $rows);

        // alice: last_order=2025-10-01 (3rd most recent -> recency_score=2),
        //        order_count=5 (highest -> frequency_score=1),
        //        total_spent=775 (3rd highest -> monetary_score=2)
        $this->assertSame('alice', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['recency_score']);
        $this->assertEquals(1, (int) $rows[0]['frequency_score']);
        $this->assertEquals(2, (int) $rows[0]['monetary_score']);

        // bob: last_order=2025-10-05 (2nd most recent -> recency_score=1),
        //      order_count=3 (2nd/3rd highest -> frequency_score=2),
        //      total_spent=1300 (highest -> monetary_score=1)
        $this->assertSame('bob', $rows[1]['name']);
        $this->assertEquals(1, (int) $rows[1]['recency_score']);
        $this->assertEquals(2, (int) $rows[1]['frequency_score']);
        $this->assertEquals(1, (int) $rows[1]['monetary_score']);

        // eve: last_order=2025-03-10 (oldest -> recency_score=4),
        //      order_count=1 (lowest tier -> frequency_score=4),
        //      total_spent=30 (lowest -> monetary_score=4)
        $this->assertSame('eve', $rows[4]['name']);
        $this->assertEquals(4, (int) $rows[4]['recency_score']);
        $this->assertEquals(4, (int) $rows[4]['frequency_score']);
        $this->assertEquals(4, (int) $rows[4]['monetary_score']);
    }

    /**
     * Identify high-value customers with above-average total spending using HAVING
     * with a correlated subquery.
     * Average: (775+1300+140+1200+30+390+450+100)/8 = 4385/8 = 548.125
     * Above average: bob (1300), diana (1200), alice (775).
     */
    public function testHighValueCustomers(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name, SUM(o.amount) AS total_spent
             FROM mp_rfm_customers c
             JOIN mp_rfm_orders o ON o.customer_id = c.id
             GROUP BY c.id, c.name
             HAVING SUM(o.amount) > (
                 SELECT AVG(customer_total) FROM (
                     SELECT SUM(amount) AS customer_total
                     FROM mp_rfm_orders
                     GROUP BY customer_id
                 ) avg_table
             )
             ORDER BY total_spent DESC"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('bob', $rows[0]['name']);
        $this->assertEqualsWithDelta(1300.00, (float) $rows[0]['total_spent'], 0.01);

        $this->assertSame('diana', $rows[1]['name']);
        $this->assertEqualsWithDelta(1200.00, (float) $rows[1]['total_spent'], 0.01);

        $this->assertSame('alice', $rows[2]['name']);
        $this->assertEqualsWithDelta(775.00, (float) $rows[2]['total_spent'], 0.01);
    }

    /**
     * Average order value per customer.
     * Diana has highest avg (1200.00), Bob second (433.33).
     */
    public function testAvgOrderValue(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name,
                    COUNT(o.id) AS orders,
                    SUM(o.amount) AS total,
                    ROUND(SUM(o.amount) / COUNT(o.id), 2) AS avg_order_value
             FROM mp_rfm_customers c
             JOIN mp_rfm_orders o ON o.customer_id = c.id
             GROUP BY c.id, c.name
             ORDER BY avg_order_value DESC"
        );

        $this->assertCount(8, $rows);

        // Diana: 1200/1 = 1200.00
        $this->assertSame('diana', $rows[0]['name']);
        $this->assertEqualsWithDelta(1200.00, (float) $rows[0]['avg_order_value'], 0.01);

        // Bob: 1300/3 = 433.33
        $this->assertSame('bob', $rows[1]['name']);
        $this->assertEqualsWithDelta(433.33, (float) $rows[1]['avg_order_value'], 0.01);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_rfm_customers VALUES (9, 'ivan', '2025-09-01')");
        $this->pdo->exec("INSERT INTO mp_rfm_orders VALUES (19, 9, '2025-10-10', 999.99)");

        // ZTD sees changes
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_rfm_customers");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_rfm_orders");
        $this->assertEquals(19, (int) $rows[0]['cnt']);

        // Physical tables untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_rfm_customers")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
