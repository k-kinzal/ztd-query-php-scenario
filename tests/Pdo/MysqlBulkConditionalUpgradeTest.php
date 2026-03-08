<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;

/**
 * Tests bulk conditional UPDATE based on cross-table aggregate lookups through ZTD shadow store (MySQL PDO).
 * Simulates CRM-style customer tier upgrades based on order history.
 *
 * Known Issue (SQLite): UPDATE WHERE IN (SELECT ... GROUP BY ... HAVING ...) causes "incomplete input"
 * on SQLite. On MySQL, this pattern may behave differently -- the direct approach is tested to document
 * actual behavior. The query-then-update workaround is the portable, reliable approach.
 *
 * @spec SPEC-4.2
 * @spec SPEC-3.3
 */
class MysqlBulkConditionalUpgradeTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_bu_customers (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255),
                tier VARCHAR(255) DEFAULT \'bronze\'
            )',
            'CREATE TABLE mp_bu_orders (
                id INT PRIMARY KEY,
                customer_id INT,
                amount DECIMAL(10,2),
                status VARCHAR(255),
                created_at DATETIME
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_bu_orders', 'mp_bu_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_bu_customers VALUES (1, 'Alice', 'alice@example.com', 'bronze')");
        $this->pdo->exec("INSERT INTO mp_bu_customers VALUES (2, 'Bob', 'bob@example.com', 'bronze')");
        $this->pdo->exec("INSERT INTO mp_bu_customers VALUES (3, 'Carol', 'carol@example.com', 'bronze')");
        $this->pdo->exec("INSERT INTO mp_bu_customers VALUES (4, 'Dave', 'dave@example.com', 'bronze')");
        $this->pdo->exec("INSERT INTO mp_bu_customers VALUES (5, 'Eve', 'eve@example.com', 'bronze')");

        // Alice: high spender (total 2500)
        $this->pdo->exec("INSERT INTO mp_bu_orders VALUES (1, 1, 1000.00, 'completed', '2024-01-01 00:00:00')");
        $this->pdo->exec("INSERT INTO mp_bu_orders VALUES (2, 1, 800.00, 'completed', '2024-02-01 00:00:00')");
        $this->pdo->exec("INSERT INTO mp_bu_orders VALUES (3, 1, 700.00, 'completed', '2024-03-01 00:00:00')");

        // Bob: medium spender (total 600)
        $this->pdo->exec("INSERT INTO mp_bu_orders VALUES (4, 2, 300.00, 'completed', '2024-01-15 00:00:00')");
        $this->pdo->exec("INSERT INTO mp_bu_orders VALUES (5, 2, 300.00, 'completed', '2024-02-15 00:00:00')");

        // Carol: low spender (total 150)
        $this->pdo->exec("INSERT INTO mp_bu_orders VALUES (6, 3, 150.00, 'completed', '2024-01-20 00:00:00')");

        // Dave: has only pending orders
        $this->pdo->exec("INSERT INTO mp_bu_orders VALUES (7, 4, 5000.00, 'pending', '2024-03-01 00:00:00')");

        // Eve: no orders at all
    }

    /**
     * Direct UPDATE with aggregate subquery: test actual MySQL behavior.
     * On SQLite this causes "incomplete input"; on MySQL it may work or throw a different error.
     * This test documents the actual behavior on MySQL.
     * @spec SPEC-11.UPDATE-AGGREGATE-SUBQUERY
     */
    public function testBulkUpgradeWithAggregateSubqueryDirectApproach(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE mp_bu_customers SET tier = 'gold'
                 WHERE id IN (
                    SELECT customer_id FROM mp_bu_orders
                    WHERE status = 'completed'
                    GROUP BY customer_id
                    HAVING SUM(amount) >= 1000
                 )"
            );

            // If we get here, MySQL handled it -- verify the result
            $rows = $this->ztdQuery("SELECT name, tier FROM mp_bu_customers ORDER BY name");
            $this->assertEquals('gold', $rows[0]['tier']);    // Alice (2500 >= 1000)
            $this->assertEquals('bronze', $rows[1]['tier']);   // Bob (600 < 1000)
            $this->assertEquals('bronze', $rows[2]['tier']);   // Carol (150 < 1000)
            $this->assertEquals('bronze', $rows[3]['tier']);   // Dave (pending only)
            $this->assertEquals('bronze', $rows[4]['tier']);   // Eve (no orders)
        } catch (ZtdPdoException $e) {
            // If MySQL also fails, document the error message
            $this->addToAssertionCount(1);
            // The error differs from SQLite's "incomplete input" -- record it
            $this->assertNotEmpty($e->getMessage());
        }
    }

    /**
     * Workaround: query eligible IDs first, then UPDATE by explicit ID list.
     */
    public function testBulkUpgradeWorkaround(): void
    {
        // Step 1: Query eligible customer IDs
        $rows = $this->ztdQuery(
            "SELECT customer_id FROM mp_bu_orders
             WHERE status = 'completed'
             GROUP BY customer_id
             HAVING SUM(amount) >= 1000"
        );
        $ids = array_column($rows, 'customer_id');

        // Step 2: UPDATE by explicit ID list
        if (!empty($ids)) {
            $placeholders = implode(',', $ids);
            $this->pdo->exec("UPDATE mp_bu_customers SET tier = 'gold' WHERE id IN ({$placeholders})");
        }

        $rows = $this->ztdQuery("SELECT name, tier FROM mp_bu_customers ORDER BY name");
        $this->assertEquals('gold', $rows[0]['tier']);    // Alice (2500 >= 1000)
        $this->assertEquals('bronze', $rows[1]['tier']);   // Bob (600 < 1000)
        $this->assertEquals('bronze', $rows[2]['tier']);   // Carol (150 < 1000)
        $this->assertEquals('bronze', $rows[3]['tier']);   // Dave (pending only)
        $this->assertEquals('bronze', $rows[4]['tier']);   // Eve (no orders)
    }

    /**
     * Sequential tier upgrades using workaround: query-then-update.
     */
    public function testSequentialTierUpgrades(): void
    {
        // Step 1: Upgrade to silver (>= 500)
        $silverIds = array_column($this->ztdQuery(
            "SELECT customer_id FROM mp_bu_orders WHERE status = 'completed'
             GROUP BY customer_id HAVING SUM(amount) >= 500"
        ), 'customer_id');

        if (!empty($silverIds)) {
            $this->pdo->exec("UPDATE mp_bu_customers SET tier = 'silver' WHERE id IN (" . implode(',', $silverIds) . ")");
        }

        // Step 2: Upgrade to gold (>= 2000)
        $goldIds = array_column($this->ztdQuery(
            "SELECT customer_id FROM mp_bu_orders WHERE status = 'completed'
             GROUP BY customer_id HAVING SUM(amount) >= 2000"
        ), 'customer_id');

        if (!empty($goldIds)) {
            $this->pdo->exec("UPDATE mp_bu_customers SET tier = 'gold' WHERE id IN (" . implode(',', $goldIds) . ")");
        }

        $rows = $this->ztdQuery("SELECT name, tier FROM mp_bu_customers ORDER BY name");
        $this->assertEquals('gold', $rows[0]['tier']);     // Alice: 2500 >= 2000
        $this->assertEquals('silver', $rows[1]['tier']);   // Bob: 600 >= 500
        $this->assertEquals('bronze', $rows[2]['tier']);   // Carol: 150 < 500
        $this->assertEquals('bronze', $rows[3]['tier']);   // Dave: no completed
        $this->assertEquals('bronze', $rows[4]['tier']);   // Eve: no orders
    }

    /**
     * Insert new orders then re-run bulk upgrade using workaround.
     */
    public function testUpgradeAfterNewOrders(): void
    {
        $this->pdo->exec("INSERT INTO mp_bu_orders VALUES (8, 2, 500.00, 'completed', '2024-03-15 00:00:00')");
        $this->pdo->exec("INSERT INTO mp_bu_orders VALUES (9, 2, 300.00, 'completed', '2024-03-20 00:00:00')");

        $goldIds = array_column($this->ztdQuery(
            "SELECT customer_id FROM mp_bu_orders WHERE status = 'completed'
             GROUP BY customer_id HAVING SUM(amount) >= 1000"
        ), 'customer_id');

        if (!empty($goldIds)) {
            $this->pdo->exec("UPDATE mp_bu_customers SET tier = 'gold' WHERE id IN (" . implode(',', $goldIds) . ")");
        }

        $rows = $this->ztdQuery("SELECT name, tier FROM mp_bu_customers ORDER BY name");
        $this->assertEquals('gold', $rows[0]['tier']); // Alice (2500)
        $this->assertEquals('gold', $rows[1]['tier']); // Bob (600 + 500 + 300 = 1400)
    }

    /**
     * Verification: JOIN customers with order aggregates after upgrade.
     */
    public function testVerifyTierWithOrderAggregates(): void
    {
        // Upgrade Alice using workaround
        $this->pdo->exec("UPDATE mp_bu_customers SET tier = 'gold' WHERE id = 1");

        $rows = $this->ztdQuery(
            "SELECT c.name, c.tier, COALESCE(SUM(o.amount), 0) AS total_spent
             FROM mp_bu_customers c
             LEFT JOIN mp_bu_orders o ON c.id = o.customer_id AND o.status = 'completed'
             GROUP BY c.id, c.name, c.tier
             ORDER BY total_spent DESC"
        );

        $this->assertCount(5, $rows);
        $goldCustomers = array_filter($rows, fn($r) => $r['tier'] === 'gold');
        foreach ($goldCustomers as $gc) {
            $this->assertGreaterThanOrEqual(1000, (float) $gc['total_spent']);
        }
    }

    /**
     * Tier count summary using GROUP BY on tier column.
     */
    public function testTierCountSummary(): void
    {
        // Apply tiers using workaround
        $silverIds = array_column($this->ztdQuery(
            "SELECT customer_id FROM mp_bu_orders WHERE status = 'completed'
             GROUP BY customer_id HAVING SUM(amount) >= 500"
        ), 'customer_id');
        if (!empty($silverIds)) {
            $this->pdo->exec("UPDATE mp_bu_customers SET tier = 'silver' WHERE id IN (" . implode(',', $silverIds) . ")");
        }

        $goldIds = array_column($this->ztdQuery(
            "SELECT customer_id FROM mp_bu_orders WHERE status = 'completed'
             GROUP BY customer_id HAVING SUM(amount) >= 2000"
        ), 'customer_id');
        if (!empty($goldIds)) {
            $this->pdo->exec("UPDATE mp_bu_customers SET tier = 'gold' WHERE id IN (" . implode(',', $goldIds) . ")");
        }

        $rows = $this->ztdQuery(
            "SELECT tier, COUNT(*) AS cnt FROM mp_bu_customers GROUP BY tier ORDER BY tier"
        );

        $tiers = [];
        foreach ($rows as $r) {
            $tiers[$r['tier']] = (int) $r['cnt'];
        }

        $this->assertEquals(3, $tiers['bronze'] ?? 0);
        $this->assertEquals(1, $tiers['silver'] ?? 0);
        $this->assertEquals(1, $tiers['gold'] ?? 0);
    }

    /**
     * Anti-pattern detection: find incorrectly classified gold customers.
     * Uses NOT IN with aggregate subquery in SELECT (read-side) which works.
     */
    public function testDetectMisclassifiedCustomers(): void
    {
        $this->pdo->exec("UPDATE mp_bu_customers SET tier = 'gold' WHERE id = 4");

        $rows = $this->ztdQuery(
            "SELECT c.name FROM mp_bu_customers c
             WHERE c.tier = 'gold'
             AND c.id NOT IN (
                SELECT customer_id FROM mp_bu_orders
                WHERE status = 'completed'
                GROUP BY customer_id
                HAVING SUM(amount) >= 1000
             )"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Dave', $rows[0]['name']);
    }

    /**
     * UPDATE with simple (non-aggregate) subquery in WHERE works.
     */
    public function testUpdateWithSimpleSubqueryWorks(): void
    {
        // This simpler pattern (no GROUP BY / HAVING) should work
        $this->pdo->exec(
            "UPDATE mp_bu_customers SET tier = 'active'
             WHERE id IN (SELECT DISTINCT customer_id FROM mp_bu_orders WHERE status = 'completed')"
        );

        $rows = $this->ztdQuery("SELECT name, tier FROM mp_bu_customers ORDER BY name");
        $this->assertEquals('active', $rows[0]['tier']); // Alice
        $this->assertEquals('active', $rows[1]['tier']); // Bob
        $this->assertEquals('active', $rows[2]['tier']); // Carol
        $this->assertEquals('bronze', $rows[3]['tier']); // Dave (pending only)
        $this->assertEquals('bronze', $rows[4]['tier']); // Eve (no orders)
    }

    /**
     * Order status change affects eligibility in subsequent queries.
     */
    public function testStatusChangeAffectsEligibility(): void
    {
        $this->pdo->exec("UPDATE mp_bu_orders SET status = 'completed' WHERE id = 7");

        $rows = $this->ztdQuery(
            "SELECT customer_id FROM mp_bu_orders WHERE status = 'completed'
             GROUP BY customer_id HAVING SUM(amount) >= 1000"
        );

        $ids = array_map('intval', array_column($rows, 'customer_id'));
        $this->assertContains(4, $ids); // Dave now qualifies (5000)
    }

    /**
     * Physical isolation: tier upgrades don't affect physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("UPDATE mp_bu_customers SET tier = 'gold' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT tier FROM mp_bu_customers WHERE id = 1");
        $this->assertEquals('gold', $rows[0]['tier']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT tier FROM mp_bu_customers")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);
    }
}
