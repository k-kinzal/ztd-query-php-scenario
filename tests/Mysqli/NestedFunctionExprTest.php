<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests nested function expressions, subquery in BETWEEN, and JOIN rate conversion (MySQLi).
 * SQL patterns exercised: COALESCE(NULLIF()), nested function chains in SELECT/WHERE,
 * subquery in BETWEEN, JOIN with rate table, mixed query/prepare in same session,
 * scalar subquery balance calculation.
 * @spec SPEC-10.2.174
 */
class NestedFunctionExprTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_nfe_vendor (
                id INT PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                country VARCHAR(10) NOT NULL,
                notes TEXT
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_nfe_invoice (
                id INT PRIMARY KEY,
                vendor_id INT NOT NULL,
                amount DECIMAL(12,2) NOT NULL,
                currency VARCHAR(3) NOT NULL DEFAULT \'USD\',
                issued_date DATE NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT \'pending\'
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_nfe_payment (
                id INT PRIMARY KEY,
                invoice_id INT NOT NULL,
                paid_amount DECIMAL(12,2) NOT NULL,
                paid_date DATE NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_nfe_rate (
                from_currency VARCHAR(3) NOT NULL,
                to_currency VARCHAR(3) NOT NULL,
                rate DECIMAL(10,4) NOT NULL,
                PRIMARY KEY (from_currency, to_currency)
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_nfe_payment', 'mi_nfe_invoice', 'mi_nfe_rate', 'mi_nfe_vendor'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_nfe_vendor VALUES (1, 'Acme Corp', 'US', 'Primary supplier')");
        $this->mysqli->query("INSERT INTO mi_nfe_vendor VALUES (2, 'EuroTech', 'DE', '')");
        $this->mysqli->query("INSERT INTO mi_nfe_vendor VALUES (3, 'AsiaLogistics', 'JP', NULL)");
        $this->mysqli->query("INSERT INTO mi_nfe_vendor VALUES (4, 'NullNotes Inc', 'US', '   ')");

        $this->mysqli->query("INSERT INTO mi_nfe_invoice VALUES (1, 1, 1000.00, 'USD', '2025-01-15', 'paid')");
        $this->mysqli->query("INSERT INTO mi_nfe_invoice VALUES (2, 1, 2500.00, 'USD', '2025-02-20', 'partial')");
        $this->mysqli->query("INSERT INTO mi_nfe_invoice VALUES (3, 2, 1800.00, 'EUR', '2025-03-10', 'pending')");
        $this->mysqli->query("INSERT INTO mi_nfe_invoice VALUES (4, 2, 750.00, 'EUR', '2025-04-05', 'paid')");
        $this->mysqli->query("INSERT INTO mi_nfe_invoice VALUES (5, 3, 500000.00, 'JPY', '2025-05-01', 'pending')");

        $this->mysqli->query("INSERT INTO mi_nfe_payment VALUES (1, 1, 1000.00, '2025-02-01')");
        $this->mysqli->query("INSERT INTO mi_nfe_payment VALUES (2, 2, 1500.00, '2025-03-01')");
        $this->mysqli->query("INSERT INTO mi_nfe_payment VALUES (3, 4, 750.00, '2025-04-20')");

        $this->mysqli->query("INSERT INTO mi_nfe_rate VALUES ('EUR', 'USD', 1.08)");
        $this->mysqli->query("INSERT INTO mi_nfe_rate VALUES ('JPY', 'USD', 0.0067)");
        $this->mysqli->query("INSERT INTO mi_nfe_rate VALUES ('USD', 'USD', 1.00)");
    }

    /**
     * COALESCE(NULLIF(notes, ''), 'No notes') — nested function handling.
     */
    public function testCoalesceNullif(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name,
                    COALESCE(NULLIF(notes, ''), 'No notes') AS display_notes
             FROM mi_nfe_vendor
             ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Primary supplier', $rows[0]['display_notes']);
        $this->assertSame('No notes', $rows[1]['display_notes']);
        $this->assertSame('No notes', $rows[2]['display_notes']);
        $this->assertSame('   ', $rows[3]['display_notes']);
    }

    /**
     * COALESCE(NULLIF()) with TRIM — strip whitespace then fallback.
     */
    public function testCoalesceNullifWithTrim(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name,
                    COALESCE(NULLIF(TRIM(notes), ''), 'No notes') AS clean_notes
             FROM mi_nfe_vendor
             ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Primary supplier', $rows[0]['clean_notes']);
        $this->assertSame('No notes', $rows[1]['clean_notes']);
        $this->assertSame('No notes', $rows[2]['clean_notes']);
        $this->assertSame('No notes', $rows[3]['clean_notes']);
    }

    /**
     * Subquery in BETWEEN: invoices issued between earliest and latest payment dates.
     */
    public function testSubqueryInBetween(): void
    {
        $rows = $this->ztdQuery(
            "SELECT i.id, i.amount, i.issued_date
             FROM mi_nfe_invoice i
             WHERE i.issued_date BETWEEN
                 (SELECT MIN(paid_date) FROM mi_nfe_payment) AND
                 (SELECT MAX(paid_date) FROM mi_nfe_payment)
             ORDER BY i.issued_date"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(2, (int) $rows[0]['id']);
        $this->assertEquals(3, (int) $rows[1]['id']);
        $this->assertEquals(4, (int) $rows[2]['id']);
    }

    /**
     * Scalar subquery balance: amount minus sum of payments.
     */
    public function testScalarSubqueryBalance(): void
    {
        $rows = $this->ztdQuery(
            "SELECT i.id,
                    i.amount,
                    COALESCE((SELECT SUM(p.paid_amount) FROM mi_nfe_payment p WHERE p.invoice_id = i.id), 0) AS total_paid,
                    i.amount - COALESCE((SELECT SUM(p.paid_amount) FROM mi_nfe_payment p WHERE p.invoice_id = i.id), 0) AS balance
             FROM mi_nfe_invoice i
             ORDER BY i.id"
        );

        $this->assertCount(5, $rows);
        $this->assertEqualsWithDelta(0.00, (float) $rows[0]['balance'], 0.01);
        $this->assertEqualsWithDelta(1000.00, (float) $rows[1]['balance'], 0.01);
        $this->assertEqualsWithDelta(1800.00, (float) $rows[2]['balance'], 0.01);
        $this->assertEqualsWithDelta(0.00, (float) $rows[3]['balance'], 0.01);
        $this->assertEqualsWithDelta(500000.00, (float) $rows[4]['balance'], 0.01);
    }

    /**
     * JOIN with rate table for currency conversion.
     */
    public function testJoinCurrencyConversion(): void
    {
        $rows = $this->ztdQuery(
            "SELECT i.id,
                    v.name AS vendor,
                    i.amount,
                    i.currency,
                    ROUND(i.amount * r.rate, 2) AS amount_usd
             FROM mi_nfe_invoice i
             JOIN mi_nfe_vendor v ON v.id = i.vendor_id
             JOIN mi_nfe_rate r ON r.from_currency = i.currency AND r.to_currency = 'USD'
             ORDER BY i.id"
        );

        $this->assertCount(5, $rows);
        $this->assertEqualsWithDelta(1000.00, (float) $rows[0]['amount_usd'], 0.01);
        $this->assertEqualsWithDelta(2500.00, (float) $rows[1]['amount_usd'], 0.01);
        $this->assertEqualsWithDelta(1944.00, (float) $rows[2]['amount_usd'], 0.01);
        $this->assertEqualsWithDelta(810.00, (float) $rows[3]['amount_usd'], 0.01);
        $this->assertEqualsWithDelta(3350.00, (float) $rows[4]['amount_usd'], 0.01);
    }

    /**
     * UPDATE invoice status based on payment sum comparison.
     */
    public function testUpdateStatusBasedOnPaymentSum(): void
    {
        $this->ztdExec(
            "UPDATE mi_nfe_invoice SET status = 'paid'
             WHERE id IN (
                SELECT i.id
                FROM mi_nfe_invoice i
                JOIN (SELECT invoice_id, SUM(paid_amount) AS total_paid FROM mi_nfe_payment GROUP BY invoice_id) p
                ON p.invoice_id = i.id
                WHERE p.total_paid >= i.amount
             )"
        );

        $rows = $this->ztdQuery("SELECT id, status FROM mi_nfe_invoice ORDER BY id");
        $this->assertSame('paid', $rows[0]['status']);
        $this->assertSame('partial', $rows[1]['status']);
        $this->assertSame('pending', $rows[2]['status']);
        $this->assertSame('paid', $rows[3]['status']);
        $this->assertSame('pending', $rows[4]['status']);
    }

    /**
     * Prepared statement with ROUND and arithmetic.
     */
    public function testPreparedWithRoundArithmetic(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT i.id,
                    i.amount,
                    ROUND(i.amount * r.rate, 2) AS amount_usd
             FROM mi_nfe_invoice i
             JOIN mi_nfe_rate r ON r.from_currency = i.currency AND r.to_currency = 'USD'
             WHERE i.vendor_id = ?
             ORDER BY i.id",
            [2]
        );

        $this->assertCount(2, $rows);
        $this->assertEqualsWithDelta(1944.00, (float) $rows[0]['amount_usd'], 0.01);
        $this->assertEqualsWithDelta(810.00, (float) $rows[1]['amount_usd'], 0.01);
    }

    /**
     * Mixed query/prepare interleaving.
     */
    public function testMixedQueryPrepareInterleaving(): void
    {
        $this->mysqli->query("INSERT INTO mi_nfe_invoice VALUES (6, 3, 200000.00, 'JPY', '2025-06-01', 'pending')");

        $rows = $this->ztdPrepareAndExecute(
            "SELECT COUNT(*) AS cnt FROM mi_nfe_invoice WHERE vendor_id = ?",
            [3]
        );
        $this->assertEquals(2, (int) $rows[0]['cnt']);

        $this->ztdExec("UPDATE mi_nfe_invoice SET status = 'approved' WHERE id = 6");

        $rows = $this->ztdPrepareAndExecute(
            "SELECT status FROM mi_nfe_invoice WHERE id = ?",
            [6]
        );
        $this->assertCount(1, $rows);
        $this->assertSame('approved', $rows[0]['status']);
    }

    /**
     * Nested CASE with COALESCE: payment status label.
     */
    public function testNestedCaseWithCoalesce(): void
    {
        $rows = $this->ztdQuery(
            "SELECT i.id,
                    CASE
                        WHEN COALESCE((SELECT SUM(p.paid_amount) FROM mi_nfe_payment p WHERE p.invoice_id = i.id), 0) = 0
                            THEN 'unpaid'
                        WHEN COALESCE((SELECT SUM(p.paid_amount) FROM mi_nfe_payment p WHERE p.invoice_id = i.id), 0) >= i.amount
                            THEN 'fully paid'
                        ELSE 'partially paid'
                    END AS payment_label
             FROM mi_nfe_invoice i
             ORDER BY i.id"
        );

        $this->assertCount(5, $rows);
        $this->assertSame('fully paid', $rows[0]['payment_label']);
        $this->assertSame('partially paid', $rows[1]['payment_label']);
        $this->assertSame('unpaid', $rows[2]['payment_label']);
        $this->assertSame('fully paid', $rows[3]['payment_label']);
        $this->assertSame('unpaid', $rows[4]['payment_label']);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_nfe_invoice VALUES (6, 1, 999.00, 'USD', '2025-12-01', 'pending')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_nfe_invoice");
        $this->assertEquals(6, (int) $rows[0]['cnt']);

        $this->disableZtd();
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_nfe_invoice");
        $row = $result->fetch_assoc();
        $this->assertSame(0, (int) $row['cnt']);
    }
}
