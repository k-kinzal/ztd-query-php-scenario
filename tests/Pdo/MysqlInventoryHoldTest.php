<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests an inventory hold/reservation with expiry workflow through ZTD shadow store (MySQL PDO).
 * Covers time-windowed reservation, available-stock calculation, hold-to-purchase conversion,
 * expiry cleanup, and physical isolation.
 * @spec SPEC-10.2.111
 */
class MysqlInventoryHoldTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_ih_products (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                stock INT
            )',
            'CREATE TABLE mp_ih_holds (
                id INT AUTO_INCREMENT PRIMARY KEY,
                product_id INT,
                user_id INT,
                quantity INT,
                status VARCHAR(20) DEFAULT \'held\',
                created_at DATE,
                expires_at DATE
            )',
            'CREATE TABLE mp_ih_purchases (
                id INT AUTO_INCREMENT PRIMARY KEY,
                hold_id INT,
                product_id INT,
                user_id INT,
                quantity INT,
                purchased_at DATE
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_ih_purchases', 'mp_ih_holds', 'mp_ih_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 products
        $this->pdo->exec("INSERT INTO mp_ih_products VALUES (1, 'Widget', 10)");
        $this->pdo->exec("INSERT INTO mp_ih_products VALUES (2, 'Gadget', 5)");
        $this->pdo->exec("INSERT INTO mp_ih_products VALUES (3, 'Gizmo', 20)");

        // 5 holds
        $this->pdo->exec("INSERT INTO mp_ih_holds VALUES (1, 1, 1, 2, 'held', '2026-03-08', '2026-03-10')");
        $this->pdo->exec("INSERT INTO mp_ih_holds VALUES (2, 1, 2, 3, 'held', '2026-03-07', '2026-03-08')");
        $this->pdo->exec("INSERT INTO mp_ih_holds VALUES (3, 2, 3, 2, 'held', '2026-03-08', '2026-03-10')");
        $this->pdo->exec("INSERT INTO mp_ih_holds VALUES (4, 1, 4, 1, 'confirmed', '2026-03-07', '2026-03-10')");
        $this->pdo->exec("INSERT INTO mp_ih_holds VALUES (5, 3, 5, 5, 'held', '2026-03-08', '2026-03-10')");

        // 1 purchase from confirmed hold 4
        $this->pdo->exec("INSERT INTO mp_ih_purchases VALUES (1, 4, 1, 4, 1, '2026-03-09')");
    }

    /**
     * Available stock = physical stock minus active held quantity.
     * Active hold: status='held' AND expires_at >= '2026-03-09'.
     */
    public function testAvailableStock(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name,
                    p.stock,
                    p.stock - COALESCE(SUM(CASE WHEN h.status = 'held' AND h.expires_at >= '2026-03-09' THEN h.quantity ELSE 0 END), 0) AS available
             FROM mp_ih_products p
             LEFT JOIN mp_ih_holds h ON h.product_id = p.id
             GROUP BY p.id, p.name, p.stock
             ORDER BY p.id"
        );

        $this->assertCount(3, $rows);

        // Widget: stock 10, active held = 2 (hold 1), hold 2 expired, hold 4 confirmed => available = 8
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertEquals(10, (int) $rows[0]['stock']);
        $this->assertEquals(8, (int) $rows[0]['available']);

        // Gadget: stock 5, active held = 2 (hold 3) => available = 3
        $this->assertSame('Gadget', $rows[1]['name']);
        $this->assertEquals(5, (int) $rows[1]['stock']);
        $this->assertEquals(3, (int) $rows[1]['available']);

        // Gizmo: stock 20, active held = 5 (hold 5) => available = 15
        $this->assertSame('Gizmo', $rows[2]['name']);
        $this->assertEquals(20, (int) $rows[2]['stock']);
        $this->assertEquals(15, (int) $rows[2]['available']);
    }

    /**
     * Active holds per product: COUNT and SUM of quantity where status='held' AND expires_at >= today.
     */
    public function testActiveHoldsPerProduct(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name,
                    COUNT(h.id) AS hold_count,
                    SUM(h.quantity) AS total_held
             FROM mp_ih_products p
             JOIN mp_ih_holds h ON h.product_id = p.id
             WHERE h.status = 'held' AND h.expires_at >= '2026-03-09'
             GROUP BY p.id, p.name
             ORDER BY p.id"
        );

        $this->assertCount(3, $rows);

        // Widget: 1 active hold (hold 1, qty 2)
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertEquals(1, (int) $rows[0]['hold_count']);
        $this->assertEquals(2, (int) $rows[0]['total_held']);

        // Gadget: 1 active hold (hold 3, qty 2)
        $this->assertSame('Gadget', $rows[1]['name']);
        $this->assertEquals(1, (int) $rows[1]['hold_count']);
        $this->assertEquals(2, (int) $rows[1]['total_held']);

        // Gizmo: 1 active hold (hold 5, qty 5)
        $this->assertSame('Gizmo', $rows[2]['name']);
        $this->assertEquals(1, (int) $rows[2]['hold_count']);
        $this->assertEquals(5, (int) $rows[2]['total_held']);
    }

    /**
     * Expired holds: status='held' AND expires_at < today.
     */
    public function testExpiredHolds(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT h.id, h.product_id, h.user_id, h.quantity, h.expires_at
             FROM mp_ih_holds h
             WHERE h.status = 'held' AND h.expires_at < ?
             ORDER BY h.id",
            ['2026-03-09']
        );

        // Only hold 2 is expired (Widget, user 2, qty 3, expires 2026-03-08)
        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['id']);
        $this->assertEquals(1, (int) $rows[0]['product_id']);
        $this->assertEquals(2, (int) $rows[0]['user_id']);
        $this->assertEquals(3, (int) $rows[0]['quantity']);
    }

    /**
     * Confirm a hold: UPDATE status to 'confirmed', INSERT purchase, verify both.
     */
    public function testConfirmHold(): void
    {
        // Confirm hold 1 (Widget, user 1, qty 2)
        $this->pdo->exec("UPDATE mp_ih_holds SET status = 'confirmed' WHERE id = 1");
        $this->pdo->exec("INSERT INTO mp_ih_purchases VALUES (2, 1, 1, 1, 2, '2026-03-09')");

        // Verify hold status changed
        $rows = $this->ztdQuery("SELECT status FROM mp_ih_holds WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('confirmed', $rows[0]['status']);

        // Verify purchase recorded
        $rows = $this->ztdQuery("SELECT hold_id, product_id, user_id, quantity FROM mp_ih_purchases WHERE id = 2");
        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['hold_id']);
        $this->assertEquals(1, (int) $rows[0]['product_id']);
        $this->assertEquals(1, (int) $rows[0]['user_id']);
        $this->assertEquals(2, (int) $rows[0]['quantity']);

        // Total purchases now 2
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_ih_purchases");
        $this->assertEquals(2, (int) $rows[0]['cnt']);
    }

    /**
     * Cleanup expired holds: UPDATE expired holds to status='expired', verify stock recalculation.
     */
    public function testCleanupExpiredHolds(): void
    {
        // Mark expired holds
        $this->pdo->exec("UPDATE mp_ih_holds SET status = 'expired' WHERE status = 'held' AND expires_at < '2026-03-09'");

        // Verify hold 2 is now 'expired'
        $rows = $this->ztdQuery("SELECT status FROM mp_ih_holds WHERE id = 2");
        $this->assertSame('expired', $rows[0]['status']);

        // Available stock for Widget should remain 8 (hold 2 was already expired by date,
        // now also by status; only hold 1 is active held)
        $rows = $this->ztdQuery(
            "SELECT p.stock - COALESCE(SUM(CASE WHEN h.status = 'held' AND h.expires_at >= '2026-03-09' THEN h.quantity ELSE 0 END), 0) AS available
             FROM mp_ih_products p
             LEFT JOIN mp_ih_holds h ON h.product_id = p.id
             WHERE p.id = 1
             GROUP BY p.id, p.stock"
        );
        $this->assertEquals(8, (int) $rows[0]['available']);
    }

    /**
     * Full hold-to-purchase workflow: create hold, confirm it, verify purchase and available stock.
     */
    public function testHoldToPurchaseConversion(): void
    {
        // Step 1: Create a new hold on Gizmo for user 6, qty 3
        $this->pdo->exec("INSERT INTO mp_ih_holds VALUES (6, 3, 6, 3, 'held', '2026-03-09', '2026-03-10')");

        // Verify hold exists
        $rows = $this->ztdQuery("SELECT status, quantity FROM mp_ih_holds WHERE id = 6");
        $this->assertCount(1, $rows);
        $this->assertSame('held', $rows[0]['status']);
        $this->assertEquals(3, (int) $rows[0]['quantity']);

        // Step 2: Confirm the hold
        $this->pdo->exec("UPDATE mp_ih_holds SET status = 'confirmed' WHERE id = 6");

        // Step 3: Insert purchase
        $this->pdo->exec("INSERT INTO mp_ih_purchases VALUES (2, 6, 3, 6, 3, '2026-03-09')");

        // Verify purchase
        $rows = $this->ztdQuery("SELECT hold_id, quantity FROM mp_ih_purchases WHERE hold_id = 6");
        $this->assertCount(1, $rows);
        $this->assertEquals(6, (int) $rows[0]['hold_id']);
        $this->assertEquals(3, (int) $rows[0]['quantity']);

        // Step 4: Verify available stock for Gizmo (hold 5 still active with qty 5, hold 6 confirmed)
        $rows = $this->ztdQuery(
            "SELECT p.stock - COALESCE(SUM(CASE WHEN h.status = 'held' AND h.expires_at >= '2026-03-09' THEN h.quantity ELSE 0 END), 0) AS available
             FROM mp_ih_products p
             LEFT JOIN mp_ih_holds h ON h.product_id = p.id
             WHERE p.id = 3
             GROUP BY p.id, p.stock"
        );
        $this->assertEquals(15, (int) $rows[0]['available']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_ih_holds VALUES (6, 1, 6, 1, 'held', '2026-03-09', '2026-03-10')");
        $this->pdo->exec("UPDATE mp_ih_holds SET status = 'confirmed' WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_ih_holds");
        $this->assertSame(6, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT status FROM mp_ih_holds WHERE id = 1");
        $this->assertSame('confirmed', $rows[0]['status']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_ih_holds")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
