<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a warranty claim workflow through ZTD shadow store (SQLite PDO).
 * Covers claim filing, warranty validity via date arithmetic, state-machine
 * transitions with guards, status reporting, and physical isolation.
 * @spec SPEC-10.2.81
 */
class SqliteWarrantyClaimTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_wc_products (
                id INTEGER PRIMARY KEY,
                name TEXT,
                warranty_months INTEGER
            )',
            'CREATE TABLE sl_wc_purchases (
                id INTEGER PRIMARY KEY,
                product_id INTEGER,
                customer_name TEXT,
                purchase_date TEXT
            )',
            'CREATE TABLE sl_wc_claims (
                id INTEGER PRIMARY KEY,
                purchase_id INTEGER,
                description TEXT,
                status TEXT,
                filed_date TEXT,
                resolved_date TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_wc_claims', 'sl_wc_purchases', 'sl_wc_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_wc_products VALUES (1, 'Laptop Pro', 24)");
        $this->pdo->exec("INSERT INTO sl_wc_products VALUES (2, 'Wireless Mouse', 6)");
        $this->pdo->exec("INSERT INTO sl_wc_products VALUES (3, 'Monitor Ultra', 36)");

        $this->pdo->exec("INSERT INTO sl_wc_purchases VALUES (1, 1, 'Alice', '2025-06-15 10:00:00')");
        $this->pdo->exec("INSERT INTO sl_wc_purchases VALUES (2, 2, 'Bob', '2025-01-10 14:00:00')");
        $this->pdo->exec("INSERT INTO sl_wc_purchases VALUES (3, 3, 'Charlie', '2024-01-01 09:00:00')");
        $this->pdo->exec("INSERT INTO sl_wc_purchases VALUES (4, 1, 'Diana', '2026-01-20 11:00:00')");

        $this->pdo->exec("INSERT INTO sl_wc_claims VALUES (1, 1, 'Screen flickering', 'filed', '2026-03-01 09:00:00', NULL)");
        $this->pdo->exec("INSERT INTO sl_wc_claims VALUES (2, 3, 'Dead pixels', 'filed', '2026-03-02 10:00:00', NULL)");
        $this->pdo->exec("INSERT INTO sl_wc_claims VALUES (3, 4, 'Keyboard issue', 'filed', '2026-03-03 11:00:00', NULL)");
    }

    /**
     * INSERT a claim, verify via JOIN with purchase and product.
     */
    public function testFileWarrantyClaim(): void
    {
        $this->pdo->exec("INSERT INTO sl_wc_claims VALUES (4, 2, 'Scroll wheel broken', 'filed', '2026-03-05 09:00:00', NULL)");

        $rows = $this->ztdQuery(
            "SELECT cl.id AS claim_id, cl.description, cl.status,
                    pu.customer_name, pr.name AS product_name
             FROM sl_wc_claims cl
             JOIN sl_wc_purchases pu ON pu.id = cl.purchase_id
             JOIN sl_wc_products pr ON pr.id = pu.product_id
             WHERE cl.id = 4"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Scroll wheel broken', $rows[0]['description']);
        $this->assertSame('filed', $rows[0]['status']);
        $this->assertSame('Bob', $rows[0]['customer_name']);
        $this->assertSame('Wireless Mouse', $rows[0]['product_name']);
    }

    /**
     * Check warranty validity using date() to compare filed_date vs purchase_date + warranty_months.
     */
    public function testCheckWarrantyValidity(): void
    {
        $rows = $this->ztdQuery(
            "SELECT cl.id AS claim_id,
                    pu.customer_name,
                    pr.name AS product_name,
                    CASE
                        WHEN cl.filed_date <= date(pu.purchase_date, '+' || pr.warranty_months || ' months')
                        THEN 'valid'
                        ELSE 'expired'
                    END AS warranty_status
             FROM sl_wc_claims cl
             JOIN sl_wc_purchases pu ON pu.id = cl.purchase_id
             JOIN sl_wc_products pr ON pr.id = pu.product_id
             ORDER BY cl.id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('valid', $rows[0]['warranty_status']);
        $this->assertSame('valid', $rows[1]['warranty_status']);
        $this->assertSame('valid', $rows[2]['warranty_status']);
    }

    /**
     * Approve a claim with a state guard: only claims with status='filed' can be approved.
     */
    public function testApproveClaimWithGuard(): void
    {
        $affected = $this->pdo->exec("UPDATE sl_wc_claims SET status = 'approved' WHERE id = 1 AND status = 'filed'");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT status FROM sl_wc_claims WHERE id = 1");
        $this->assertSame('approved', $rows[0]['status']);

        // Trying to approve again should affect 0 rows
        $affected = $this->pdo->exec("UPDATE sl_wc_claims SET status = 'approved' WHERE id = 1 AND status = 'filed'");
        $this->assertSame(0, $affected);
    }

    /**
     * Reject a claim: UPDATE status='rejected', set resolved_date.
     */
    public function testRejectInvalidClaim(): void
    {
        $affected = $this->pdo->exec("UPDATE sl_wc_claims SET status = 'rejected', resolved_date = '2026-03-06 15:00:00' WHERE id = 2 AND status = 'filed'");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT status, resolved_date FROM sl_wc_claims WHERE id = 2");
        $this->assertSame('rejected', $rows[0]['status']);
        $this->assertNotNull($rows[0]['resolved_date']);
    }

    /**
     * Claim status report: COUNT by status grouped by product.
     */
    public function testClaimStatusReport(): void
    {
        $this->pdo->exec("UPDATE sl_wc_claims SET status = 'approved' WHERE id = 1");
        $this->pdo->exec("UPDATE sl_wc_claims SET status = 'rejected', resolved_date = '2026-03-06 15:00:00' WHERE id = 2");

        $rows = $this->ztdQuery(
            "SELECT pr.name AS product_name,
                    COUNT(*) AS total_claims,
                    SUM(CASE WHEN cl.status = 'filed' THEN 1 ELSE 0 END) AS filed_count,
                    SUM(CASE WHEN cl.status = 'approved' THEN 1 ELSE 0 END) AS approved_count,
                    SUM(CASE WHEN cl.status = 'rejected' THEN 1 ELSE 0 END) AS rejected_count
             FROM sl_wc_claims cl
             JOIN sl_wc_purchases pu ON pu.id = cl.purchase_id
             JOIN sl_wc_products pr ON pr.id = pu.product_id
             GROUP BY pr.id, pr.name
             ORDER BY pr.name"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Laptop Pro', $rows[0]['product_name']);
        $this->assertEquals(2, (int) $rows[0]['total_claims']);
        $this->assertEquals(1, (int) $rows[0]['filed_count']);
        $this->assertEquals(1, (int) $rows[0]['approved_count']);
        $this->assertSame('Monitor Ultra', $rows[1]['product_name']);
        $this->assertEquals(1, (int) $rows[1]['total_claims']);
        $this->assertEquals(1, (int) $rows[1]['rejected_count']);
    }

    /**
     * Resolve a claim: UPDATE status='resolved', set resolved_date, verify full lifecycle.
     */
    public function testResolveClaim(): void
    {
        $affected = $this->pdo->exec("UPDATE sl_wc_claims SET status = 'approved' WHERE id = 3 AND status = 'filed'");
        $this->assertSame(1, $affected);

        $affected = $this->pdo->exec("UPDATE sl_wc_claims SET status = 'resolved', resolved_date = '2026-03-07 16:00:00' WHERE id = 3 AND status = 'approved'");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT status, filed_date, resolved_date FROM sl_wc_claims WHERE id = 3");
        $this->assertSame('resolved', $rows[0]['status']);
        $this->assertNotNull($rows[0]['filed_date']);
        $this->assertNotNull($rows[0]['resolved_date']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_wc_claims VALUES (4, 2, 'Button stuck', 'filed', '2026-03-08 09:00:00', NULL)");
        $this->pdo->exec("UPDATE sl_wc_claims SET status = 'approved' WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_wc_claims");
        $this->assertEquals(4, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_wc_claims')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
