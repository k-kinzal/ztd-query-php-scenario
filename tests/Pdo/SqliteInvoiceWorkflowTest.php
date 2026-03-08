<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests an invoice/billing workflow through ZTD shadow store (SQLite PDO).
 * Covers line item calculations, discount application, status transitions,
 * multi-table aggregations, and physical isolation of shadow mutations.
 * @spec SPEC-4.1
 */
class SqliteInvoiceWorkflowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_iw_customers (
                id INTEGER PRIMARY KEY,
                name TEXT,
                tier TEXT
            )',
            'CREATE TABLE sl_iw_invoices (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                invoice_date TEXT,
                status TEXT,
                discount_pct REAL
            )',
            'CREATE TABLE sl_iw_line_items (
                id INTEGER PRIMARY KEY,
                invoice_id INTEGER,
                description TEXT,
                quantity INTEGER,
                unit_price REAL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_iw_customers', 'sl_iw_invoices', 'sl_iw_line_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 customers with different tiers
        $this->pdo->exec("INSERT INTO sl_iw_customers VALUES (1, 'Acme Corp', 'standard')");
        $this->pdo->exec("INSERT INTO sl_iw_customers VALUES (2, 'Globex Inc', 'premium')");
        $this->pdo->exec("INSERT INTO sl_iw_customers VALUES (3, 'Initech LLC', 'vip')");

        // 3 invoices across customers
        $this->pdo->exec("INSERT INTO sl_iw_invoices VALUES (1, 1, '2026-01-15', 'draft', 0.0)");
        $this->pdo->exec("INSERT INTO sl_iw_invoices VALUES (2, 2, '2026-02-10', 'sent', 0.0)");
        $this->pdo->exec("INSERT INTO sl_iw_invoices VALUES (3, 3, '2026-03-01', 'paid', 5.0)");

        // 8 line items across invoices
        // Invoice 1: 3 items
        $this->pdo->exec("INSERT INTO sl_iw_line_items VALUES (1, 1, 'Consulting hours', 10, 150.00)");
        $this->pdo->exec("INSERT INTO sl_iw_line_items VALUES (2, 1, 'Software license', 2, 500.00)");
        $this->pdo->exec("INSERT INTO sl_iw_line_items VALUES (3, 1, 'Support plan', 1, 200.00)");

        // Invoice 2: 3 items
        $this->pdo->exec("INSERT INTO sl_iw_line_items VALUES (4, 2, 'Hardware setup', 5, 80.00)");
        $this->pdo->exec("INSERT INTO sl_iw_line_items VALUES (5, 2, 'Network cable', 20, 12.50)");
        $this->pdo->exec("INSERT INTO sl_iw_line_items VALUES (6, 2, 'Installation fee', 1, 350.00)");

        // Invoice 3: 2 items
        $this->pdo->exec("INSERT INTO sl_iw_line_items VALUES (7, 3, 'Annual subscription', 1, 1200.00)");
        $this->pdo->exec("INSERT INTO sl_iw_line_items VALUES (8, 3, 'Premium add-on', 3, 99.99)");
    }

    /**
     * SELECT with quantity * unit_price calculation per line item.
     */
    public function testLineItemSubtotals(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, description, quantity, unit_price,
                    quantity * unit_price AS subtotal
             FROM sl_iw_line_items
             ORDER BY id"
        );

        $this->assertCount(8, $rows);

        // Invoice 1 items
        $this->assertEqualsWithDelta(1500.00, (float) $rows[0]['subtotal'], 0.01); // 10 * 150
        $this->assertEqualsWithDelta(1000.00, (float) $rows[1]['subtotal'], 0.01); // 2 * 500
        $this->assertEqualsWithDelta(200.00, (float) $rows[2]['subtotal'], 0.01);  // 1 * 200

        // Invoice 2 items
        $this->assertEqualsWithDelta(400.00, (float) $rows[3]['subtotal'], 0.01);  // 5 * 80
        $this->assertEqualsWithDelta(250.00, (float) $rows[4]['subtotal'], 0.01);  // 20 * 12.50
        $this->assertEqualsWithDelta(350.00, (float) $rows[5]['subtotal'], 0.01);  // 1 * 350

        // Invoice 3 items
        $this->assertEqualsWithDelta(1200.00, (float) $rows[6]['subtotal'], 0.01); // 1 * 1200
        $this->assertEqualsWithDelta(299.97, (float) $rows[7]['subtotal'], 0.01);  // 3 * 99.99
    }

    /**
     * SUM(quantity * unit_price) per invoice using JOIN + GROUP BY.
     */
    public function testInvoiceTotalWithJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT i.id AS invoice_id, i.invoice_date,
                    SUM(li.quantity * li.unit_price) AS total
             FROM sl_iw_invoices i
             JOIN sl_iw_line_items li ON li.invoice_id = i.id
             GROUP BY i.id, i.invoice_date
             ORDER BY i.id"
        );

        $this->assertCount(3, $rows);

        // Invoice 1: 1500 + 1000 + 200 = 2700
        $this->assertSame(1, (int) $rows[0]['invoice_id']);
        $this->assertEqualsWithDelta(2700.00, (float) $rows[0]['total'], 0.01);

        // Invoice 2: 400 + 250 + 350 = 1000
        $this->assertSame(2, (int) $rows[1]['invoice_id']);
        $this->assertEqualsWithDelta(1000.00, (float) $rows[1]['total'], 0.01);

        // Invoice 3: 1200 + 299.97 = 1499.97
        $this->assertSame(3, (int) $rows[2]['invoice_id']);
        $this->assertEqualsWithDelta(1499.97, (float) $rows[2]['total'], 0.01);
    }

    /**
     * UPDATE discount_pct, then query total with discount applied.
     */
    public function testApplyDiscountAndRecalculate(): void
    {
        // Apply 10% discount to invoice 1
        $this->pdo->exec("UPDATE sl_iw_invoices SET discount_pct = 10.0 WHERE id = 1");

        // Verify discount was saved
        $rows = $this->ztdQuery("SELECT discount_pct FROM sl_iw_invoices WHERE id = 1");
        $this->assertEqualsWithDelta(10.0, (float) $rows[0]['discount_pct'], 0.01);

        // Calculate total with discount applied
        $rows = $this->ztdQuery(
            "SELECT i.id,
                    SUM(li.quantity * li.unit_price) AS subtotal,
                    i.discount_pct,
                    SUM(li.quantity * li.unit_price) * (1.0 - i.discount_pct / 100.0) AS discounted_total
             FROM sl_iw_invoices i
             JOIN sl_iw_line_items li ON li.invoice_id = i.id
             WHERE i.id = 1
             GROUP BY i.id, i.discount_pct"
        );

        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(2700.00, (float) $rows[0]['subtotal'], 0.01);
        $this->assertEqualsWithDelta(10.0, (float) $rows[0]['discount_pct'], 0.01);
        // 2700 * (1 - 0.10) = 2430
        $this->assertEqualsWithDelta(2430.00, (float) $rows[0]['discounted_total'], 0.01);

        // Also verify invoice 3 still has its original 5% discount
        $rows = $this->ztdQuery(
            "SELECT i.id,
                    SUM(li.quantity * li.unit_price) * (1.0 - i.discount_pct / 100.0) AS discounted_total
             FROM sl_iw_invoices i
             JOIN sl_iw_line_items li ON li.invoice_id = i.id
             WHERE i.id = 3
             GROUP BY i.id, i.discount_pct"
        );

        // 1499.97 * (1 - 0.05) = 1424.9715
        $this->assertEqualsWithDelta(1424.97, (float) $rows[0]['discounted_total'], 0.01);
    }

    /**
     * UPDATE status from draft -> sent -> paid, verifying each step.
     */
    public function testInvoiceStatusTransition(): void
    {
        // Invoice 1 starts as 'draft'
        $rows = $this->ztdQuery("SELECT status FROM sl_iw_invoices WHERE id = 1");
        $this->assertSame('draft', $rows[0]['status']);

        // Transition: draft -> sent
        $affected = $this->pdo->exec("UPDATE sl_iw_invoices SET status = 'sent' WHERE id = 1 AND status = 'draft'");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT status FROM sl_iw_invoices WHERE id = 1");
        $this->assertSame('sent', $rows[0]['status']);

        // Transition: sent -> paid
        $affected = $this->pdo->exec("UPDATE sl_iw_invoices SET status = 'paid' WHERE id = 1 AND status = 'sent'");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT status FROM sl_iw_invoices WHERE id = 1");
        $this->assertSame('paid', $rows[0]['status']);

        // Attempting invalid transition: paid -> draft should match zero rows
        $affected = $this->pdo->exec("UPDATE sl_iw_invoices SET status = 'draft' WHERE id = 1 AND status = 'sent'");
        $this->assertSame(0, $affected);

        // Status remains 'paid'
        $rows = $this->ztdQuery("SELECT status FROM sl_iw_invoices WHERE id = 1");
        $this->assertSame('paid', $rows[0]['status']);
    }

    /**
     * 3-table JOIN, GROUP BY customer, SUM totals.
     */
    public function testCustomerSpendingSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.id, c.name, c.tier,
                    COUNT(DISTINCT i.id) AS invoice_count,
                    SUM(li.quantity * li.unit_price) AS total_spending
             FROM sl_iw_customers c
             JOIN sl_iw_invoices i ON i.customer_id = c.id
             JOIN sl_iw_line_items li ON li.invoice_id = i.id
             GROUP BY c.id, c.name, c.tier
             ORDER BY total_spending DESC"
        );

        $this->assertCount(3, $rows);

        // Acme Corp: invoice 1 = 2700
        $this->assertSame('Acme Corp', $rows[0]['name']);
        $this->assertSame('standard', $rows[0]['tier']);
        $this->assertSame(1, (int) $rows[0]['invoice_count']);
        $this->assertEqualsWithDelta(2700.00, (float) $rows[0]['total_spending'], 0.01);

        // Initech LLC: invoice 3 = 1499.97
        $this->assertSame('Initech LLC', $rows[1]['name']);
        $this->assertSame('vip', $rows[1]['tier']);
        $this->assertSame(1, (int) $rows[1]['invoice_count']);
        $this->assertEqualsWithDelta(1499.97, (float) $rows[1]['total_spending'], 0.01);

        // Globex Inc: invoice 2 = 1000
        $this->assertSame('Globex Inc', $rows[2]['name']);
        $this->assertSame('premium', $rows[2]['tier']);
        $this->assertSame(1, (int) $rows[2]['invoice_count']);
        $this->assertEqualsWithDelta(1000.00, (float) $rows[2]['total_spending'], 0.01);
    }

    /**
     * INSERT new line item, verify invoice total changes.
     */
    public function testAddLineItemAndRecalculate(): void
    {
        // Original total for invoice 2: 400 + 250 + 350 = 1000
        $rows = $this->ztdQuery(
            "SELECT SUM(quantity * unit_price) AS total
             FROM sl_iw_line_items WHERE invoice_id = 2"
        );
        $this->assertEqualsWithDelta(1000.00, (float) $rows[0]['total'], 0.01);

        // Add a new line item to invoice 2
        $this->pdo->exec(
            "INSERT INTO sl_iw_line_items VALUES (9, 2, 'Emergency support', 2, 175.00)"
        );

        // Verify new item exists
        $rows = $this->ztdQuery(
            "SELECT description, quantity, unit_price
             FROM sl_iw_line_items WHERE id = 9"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Emergency support', $rows[0]['description']);
        $this->assertSame(2, (int) $rows[0]['quantity']);
        $this->assertEqualsWithDelta(175.00, (float) $rows[0]['unit_price'], 0.01);

        // Verify invoice total increased: 1000 + (2 * 175) = 1350
        $rows = $this->ztdQuery(
            "SELECT SUM(quantity * unit_price) AS total
             FROM sl_iw_line_items WHERE invoice_id = 2"
        );
        $this->assertEqualsWithDelta(1350.00, (float) $rows[0]['total'], 0.01);

        // Verify line count increased
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM sl_iw_line_items WHERE invoice_id = 2"
        );
        $this->assertSame(4, (int) $rows[0]['cnt']);
    }

    /**
     * DELETE a line item, verify total recalculates.
     */
    public function testDeleteLineItemAndVerify(): void
    {
        // Original total for invoice 1: 1500 + 1000 + 200 = 2700
        $rows = $this->ztdQuery(
            "SELECT SUM(quantity * unit_price) AS total
             FROM sl_iw_line_items WHERE invoice_id = 1"
        );
        $this->assertEqualsWithDelta(2700.00, (float) $rows[0]['total'], 0.01);

        // Delete 'Support plan' (id=3, subtotal=200)
        $affected = $this->pdo->exec("DELETE FROM sl_iw_line_items WHERE id = 3");
        $this->assertSame(1, $affected);

        // Verify item is gone
        $rows = $this->ztdQuery("SELECT id FROM sl_iw_line_items WHERE id = 3");
        $this->assertCount(0, $rows);

        // Verify new total: 1500 + 1000 = 2500
        $rows = $this->ztdQuery(
            "SELECT SUM(quantity * unit_price) AS total
             FROM sl_iw_line_items WHERE invoice_id = 1"
        );
        $this->assertEqualsWithDelta(2500.00, (float) $rows[0]['total'], 0.01);

        // Verify line count decreased
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM sl_iw_line_items WHERE invoice_id = 1"
        );
        $this->assertSame(2, (int) $rows[0]['cnt']);
    }

    /**
     * Prepared statement to look up invoice details by customer_id.
     */
    public function testPreparedInvoiceLookup(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT i.id AS invoice_id, i.invoice_date, i.status,
                    COUNT(li.id) AS line_count,
                    SUM(li.quantity * li.unit_price) AS total
             FROM sl_iw_invoices i
             JOIN sl_iw_line_items li ON li.invoice_id = i.id
             WHERE i.customer_id = ?
             GROUP BY i.id, i.invoice_date, i.status
             ORDER BY i.id"
        );

        // Lookup for customer 1 (Acme Corp)
        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['invoice_id']);
        $this->assertSame('draft', $rows[0]['status']);
        $this->assertSame(3, (int) $rows[0]['line_count']);
        $this->assertEqualsWithDelta(2700.00, (float) $rows[0]['total'], 0.01);

        // Re-execute for customer 2 (Globex Inc)
        $stmt->execute([2]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(2, (int) $rows[0]['invoice_id']);
        $this->assertSame('sent', $rows[0]['status']);
        $this->assertSame(3, (int) $rows[0]['line_count']);
        $this->assertEqualsWithDelta(1000.00, (float) $rows[0]['total'], 0.01);

        // Re-execute for customer 3 (Initech LLC)
        $stmt->execute([3]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(3, (int) $rows[0]['invoice_id']);
        $this->assertSame('paid', $rows[0]['status']);
        $this->assertSame(2, (int) $rows[0]['line_count']);
        $this->assertEqualsWithDelta(1499.97, (float) $rows[0]['total'], 0.01);
    }

    /**
     * HAVING clause to find invoices above a threshold.
     */
    public function testHighValueInvoiceFilter(): void
    {
        $rows = $this->ztdQuery(
            "SELECT i.id AS invoice_id,
                    SUM(li.quantity * li.unit_price) AS total
             FROM sl_iw_invoices i
             JOIN sl_iw_line_items li ON li.invoice_id = i.id
             GROUP BY i.id
             HAVING SUM(li.quantity * li.unit_price) > 1200.00
             ORDER BY total DESC"
        );

        $this->assertCount(2, $rows);

        // Invoice 1: 2700
        $this->assertSame(1, (int) $rows[0]['invoice_id']);
        $this->assertEqualsWithDelta(2700.00, (float) $rows[0]['total'], 0.01);

        // Invoice 3: 1499.97
        $this->assertSame(3, (int) $rows[1]['invoice_id']);
        $this->assertEqualsWithDelta(1499.97, (float) $rows[1]['total'], 0.01);

        // Invoice 2 (total=1000) should not appear
        $invoiceIds = array_column($rows, 'invoice_id');
        $this->assertNotContains('2', $invoiceIds);
    }

    /**
     * Verify shadow changes don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Mutate data through ZTD
        $this->pdo->exec("INSERT INTO sl_iw_customers VALUES (4, 'NewCo', 'standard')");
        $this->pdo->exec("UPDATE sl_iw_invoices SET status = 'paid' WHERE id = 1");
        $this->pdo->exec("DELETE FROM sl_iw_line_items WHERE id = 1");
        $this->pdo->exec("INSERT INTO sl_iw_invoices VALUES (4, 4, '2026-04-01', 'draft', 0.0)");

        // Verify mutations visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_iw_customers");
        $this->assertSame(4, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT status FROM sl_iw_invoices WHERE id = 1");
        $this->assertSame('paid', $rows[0]['status']);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_iw_line_items");
        $this->assertSame(7, (int) $rows[0]['cnt']); // 8 - 1 deleted

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_iw_invoices");
        $this->assertSame(4, (int) $rows[0]['cnt']); // 3 + 1 inserted

        // Disable ZTD and verify physical tables are untouched
        $this->pdo->disableZtd();

        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_iw_customers")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']); // Physical table has no seed data

        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_iw_invoices")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);

        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_iw_line_items")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
