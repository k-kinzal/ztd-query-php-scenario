<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests an invoice/billing workflow through ZTD shadow store (PostgreSQL PDO).
 * Covers line item calculations, discount application, status transitions,
 * multi-table aggregations, and physical isolation of shadow mutations.
 * @spec SPEC-4.1
 */
class PostgresInvoiceWorkflowTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_iw_customers (
                id INTEGER PRIMARY KEY,
                name TEXT,
                tier TEXT
            )',
            'CREATE TABLE pg_iw_invoices (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                invoice_date DATE,
                status TEXT,
                discount_pct NUMERIC(5,2)
            )',
            'CREATE TABLE pg_iw_line_items (
                id INTEGER PRIMARY KEY,
                invoice_id INTEGER,
                description TEXT,
                quantity INTEGER,
                unit_price NUMERIC(10,2)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_iw_line_items', 'pg_iw_invoices', 'pg_iw_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_iw_customers VALUES (1, 'Acme Corp', 'standard')");
        $this->pdo->exec("INSERT INTO pg_iw_customers VALUES (2, 'Globex Inc', 'premium')");
        $this->pdo->exec("INSERT INTO pg_iw_customers VALUES (3, 'Initech LLC', 'vip')");

        $this->pdo->exec("INSERT INTO pg_iw_invoices VALUES (1, 1, '2026-01-15', 'draft', 0.00)");
        $this->pdo->exec("INSERT INTO pg_iw_invoices VALUES (2, 2, '2026-02-10', 'sent', 0.00)");
        $this->pdo->exec("INSERT INTO pg_iw_invoices VALUES (3, 3, '2026-03-01', 'paid', 5.00)");

        $this->pdo->exec("INSERT INTO pg_iw_line_items VALUES (1, 1, 'Consulting hours', 10, 150.00)");
        $this->pdo->exec("INSERT INTO pg_iw_line_items VALUES (2, 1, 'Software license', 2, 500.00)");
        $this->pdo->exec("INSERT INTO pg_iw_line_items VALUES (3, 1, 'Support plan', 1, 200.00)");
        $this->pdo->exec("INSERT INTO pg_iw_line_items VALUES (4, 2, 'Hardware setup', 5, 80.00)");
        $this->pdo->exec("INSERT INTO pg_iw_line_items VALUES (5, 2, 'Network cable', 20, 12.50)");
        $this->pdo->exec("INSERT INTO pg_iw_line_items VALUES (6, 2, 'Installation fee', 1, 350.00)");
        $this->pdo->exec("INSERT INTO pg_iw_line_items VALUES (7, 3, 'Annual subscription', 1, 1200.00)");
        $this->pdo->exec("INSERT INTO pg_iw_line_items VALUES (8, 3, 'Premium add-on', 3, 99.99)");
    }

    public function testLineItemSubtotals(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, description, quantity, unit_price,
                    quantity * unit_price AS subtotal
             FROM pg_iw_line_items
             ORDER BY id"
        );

        $this->assertCount(8, $rows);
        $this->assertEqualsWithDelta(1500.00, (float) $rows[0]['subtotal'], 0.01);
        $this->assertEqualsWithDelta(1000.00, (float) $rows[1]['subtotal'], 0.01);
        $this->assertEqualsWithDelta(200.00, (float) $rows[2]['subtotal'], 0.01);
        $this->assertEqualsWithDelta(400.00, (float) $rows[3]['subtotal'], 0.01);
        $this->assertEqualsWithDelta(250.00, (float) $rows[4]['subtotal'], 0.01);
        $this->assertEqualsWithDelta(350.00, (float) $rows[5]['subtotal'], 0.01);
        $this->assertEqualsWithDelta(1200.00, (float) $rows[6]['subtotal'], 0.01);
        $this->assertEqualsWithDelta(299.97, (float) $rows[7]['subtotal'], 0.01);
    }

    public function testInvoiceTotalWithJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT i.id AS invoice_id, i.invoice_date,
                    SUM(li.quantity * li.unit_price) AS total
             FROM pg_iw_invoices i
             JOIN pg_iw_line_items li ON li.invoice_id = i.id
             GROUP BY i.id, i.invoice_date
             ORDER BY i.id"
        );

        $this->assertCount(3, $rows);
        $this->assertEqualsWithDelta(2700.00, (float) $rows[0]['total'], 0.01);
        $this->assertEqualsWithDelta(1000.00, (float) $rows[1]['total'], 0.01);
        $this->assertEqualsWithDelta(1499.97, (float) $rows[2]['total'], 0.01);
    }

    public function testApplyDiscountAndRecalculate(): void
    {
        $this->pdo->exec("UPDATE pg_iw_invoices SET discount_pct = 10.00 WHERE id = 1");

        $rows = $this->ztdQuery(
            "SELECT i.id,
                    SUM(li.quantity * li.unit_price) AS subtotal,
                    i.discount_pct,
                    SUM(li.quantity * li.unit_price) * (1.0 - i.discount_pct / 100.0) AS discounted_total
             FROM pg_iw_invoices i
             JOIN pg_iw_line_items li ON li.invoice_id = i.id
             WHERE i.id = 1
             GROUP BY i.id, i.discount_pct"
        );

        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(2700.00, (float) $rows[0]['subtotal'], 0.01);
        $this->assertEqualsWithDelta(2430.00, (float) $rows[0]['discounted_total'], 0.01);
    }

    public function testInvoiceStatusTransition(): void
    {
        $rows = $this->ztdQuery("SELECT status FROM pg_iw_invoices WHERE id = 1");
        $this->assertSame('draft', $rows[0]['status']);

        $affected = $this->pdo->exec("UPDATE pg_iw_invoices SET status = 'sent' WHERE id = 1 AND status = 'draft'");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT status FROM pg_iw_invoices WHERE id = 1");
        $this->assertSame('sent', $rows[0]['status']);

        $affected = $this->pdo->exec("UPDATE pg_iw_invoices SET status = 'paid' WHERE id = 1 AND status = 'sent'");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT status FROM pg_iw_invoices WHERE id = 1");
        $this->assertSame('paid', $rows[0]['status']);

        $affected = $this->pdo->exec("UPDATE pg_iw_invoices SET status = 'draft' WHERE id = 1 AND status = 'sent'");
        $this->assertSame(0, $affected);

        $rows = $this->ztdQuery("SELECT status FROM pg_iw_invoices WHERE id = 1");
        $this->assertSame('paid', $rows[0]['status']);
    }

    public function testCustomerSpendingSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.id, c.name, c.tier,
                    COUNT(DISTINCT i.id) AS invoice_count,
                    SUM(li.quantity * li.unit_price) AS total_spending
             FROM pg_iw_customers c
             JOIN pg_iw_invoices i ON i.customer_id = c.id
             JOIN pg_iw_line_items li ON li.invoice_id = i.id
             GROUP BY c.id, c.name, c.tier
             ORDER BY total_spending DESC"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Acme Corp', $rows[0]['name']);
        $this->assertEqualsWithDelta(2700.00, (float) $rows[0]['total_spending'], 0.01);
        $this->assertSame('Initech LLC', $rows[1]['name']);
        $this->assertEqualsWithDelta(1499.97, (float) $rows[1]['total_spending'], 0.01);
        $this->assertSame('Globex Inc', $rows[2]['name']);
        $this->assertEqualsWithDelta(1000.00, (float) $rows[2]['total_spending'], 0.01);
    }

    public function testAddLineItemAndRecalculate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT SUM(quantity * unit_price) AS total
             FROM pg_iw_line_items WHERE invoice_id = 2"
        );
        $this->assertEqualsWithDelta(1000.00, (float) $rows[0]['total'], 0.01);

        $this->pdo->exec(
            "INSERT INTO pg_iw_line_items VALUES (9, 2, 'Emergency support', 2, 175.00)"
        );

        $rows = $this->ztdQuery(
            "SELECT SUM(quantity * unit_price) AS total
             FROM pg_iw_line_items WHERE invoice_id = 2"
        );
        $this->assertEqualsWithDelta(1350.00, (float) $rows[0]['total'], 0.01);
    }

    public function testPreparedInvoiceLookup(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT i.id AS invoice_id, i.status,
                    COUNT(li.id) AS line_count,
                    SUM(li.quantity * li.unit_price) AS total
             FROM pg_iw_invoices i
             JOIN pg_iw_line_items li ON li.invoice_id = i.id
             WHERE i.customer_id = ?
             GROUP BY i.id, i.status
             ORDER BY i.id",
            [1]
        );

        $this->assertCount(1, $rows);
        $this->assertSame('draft', $rows[0]['status']);
        $this->assertEquals(3, (int) $rows[0]['line_count']);
        $this->assertEqualsWithDelta(2700.00, (float) $rows[0]['total'], 0.01);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_iw_customers VALUES (4, 'NewCo', 'standard')");
        $this->pdo->exec("UPDATE pg_iw_invoices SET status = 'paid' WHERE id = 1");
        $this->pdo->exec("DELETE FROM pg_iw_line_items WHERE id = 1");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_iw_customers");
        $this->assertSame(4, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();

        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_iw_customers")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
