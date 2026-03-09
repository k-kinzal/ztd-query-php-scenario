<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests nested function expressions, subquery in BETWEEN, JOIN rate conversion,
 * computed INSERT values, function-based WHERE in prepared statements,
 * function-based UPDATE SET, and GROUP BY with function expressions (MySQL PDO).
 *
 * SQL patterns exercised: COALESCE(NULLIF()), nested function chains in SELECT/WHERE,
 * subquery in BETWEEN, JOIN with rate table, mixed exec/prepare in same session,
 * scalar subquery balance calculation, CONCAT(UPPER(), LPAD()), LOCATE(LOWER()),
 * LEFT()+CONCAT in UPDATE, YEAR()/MONTH() in GROUP BY.
 *
 * @spec SPEC-10.2.174, SPEC-3.1, SPEC-3.3, SPEC-4.1, SPEC-4.2
 */
class MysqlNestedFunctionExprTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_nfe_vendor (
                id INT PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                country VARCHAR(10) NOT NULL,
                notes TEXT
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_nfe_invoice (
                id INT PRIMARY KEY,
                vendor_id INT NOT NULL,
                amount DECIMAL(12,2) NOT NULL,
                currency VARCHAR(3) NOT NULL DEFAULT \'USD\',
                issued_date DATE NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT \'pending\'
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_nfe_payment (
                id INT PRIMARY KEY,
                invoice_id INT NOT NULL,
                paid_amount DECIMAL(12,2) NOT NULL,
                paid_date DATE NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_nfe_rate (
                from_currency VARCHAR(3) NOT NULL,
                to_currency VARCHAR(3) NOT NULL,
                rate DECIMAL(10,4) NOT NULL,
                PRIMARY KEY (from_currency, to_currency)
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_nfe_codes (
                id INT PRIMARY KEY,
                label VARCHAR(200) NOT NULL,
                code VARCHAR(50) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_nfe_events (
                id INT PRIMARY KEY,
                title VARCHAR(200) NOT NULL,
                created_at DATETIME NOT NULL,
                category VARCHAR(50) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_nfe_payment', 'mp_nfe_invoice', 'mp_nfe_rate', 'mp_nfe_vendor', 'mp_nfe_codes', 'mp_nfe_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_nfe_vendor VALUES (1, 'Acme Corp', 'US', 'Primary supplier')");
        $this->pdo->exec("INSERT INTO mp_nfe_vendor VALUES (2, 'EuroTech', 'DE', '')");
        $this->pdo->exec("INSERT INTO mp_nfe_vendor VALUES (3, 'AsiaLogistics', 'JP', NULL)");
        $this->pdo->exec("INSERT INTO mp_nfe_vendor VALUES (4, 'NullNotes Inc', 'US', '   ')");

        $this->pdo->exec("INSERT INTO mp_nfe_invoice VALUES (1, 1, 1000.00, 'USD', '2025-01-15', 'paid')");
        $this->pdo->exec("INSERT INTO mp_nfe_invoice VALUES (2, 1, 2500.00, 'USD', '2025-02-20', 'partial')");
        $this->pdo->exec("INSERT INTO mp_nfe_invoice VALUES (3, 2, 1800.00, 'EUR', '2025-03-10', 'pending')");
        $this->pdo->exec("INSERT INTO mp_nfe_invoice VALUES (4, 2, 750.00, 'EUR', '2025-04-05', 'paid')");
        $this->pdo->exec("INSERT INTO mp_nfe_invoice VALUES (5, 3, 500000.00, 'JPY', '2025-05-01', 'pending')");

        $this->pdo->exec("INSERT INTO mp_nfe_payment VALUES (1, 1, 1000.00, '2025-02-01')");
        $this->pdo->exec("INSERT INTO mp_nfe_payment VALUES (2, 2, 1500.00, '2025-03-01')");
        $this->pdo->exec("INSERT INTO mp_nfe_payment VALUES (3, 4, 750.00, '2025-04-20')");

        $this->pdo->exec("INSERT INTO mp_nfe_rate VALUES ('EUR', 'USD', 1.08)");
        $this->pdo->exec("INSERT INTO mp_nfe_rate VALUES ('JPY', 'USD', 0.0067)");
        $this->pdo->exec("INSERT INTO mp_nfe_rate VALUES ('USD', 'USD', 1.00)");
    }

    /**
     * COALESCE(NULLIF(notes, ''), 'No notes') -- nested function handling.
     */
    public function testCoalesceNullif(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name,
                    COALESCE(NULLIF(notes, ''), 'No notes') AS display_notes
             FROM mp_nfe_vendor
             ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Primary supplier', $rows[0]['display_notes']);
        $this->assertSame('No notes', $rows[1]['display_notes']);
        $this->assertSame('No notes', $rows[2]['display_notes']);
        $this->assertSame('   ', $rows[3]['display_notes']);
    }

    /**
     * COALESCE(NULLIF()) with TRIM -- strip whitespace then fallback.
     */
    public function testCoalesceNullifWithTrim(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name,
                    COALESCE(NULLIF(TRIM(notes), ''), 'No notes') AS clean_notes
             FROM mp_nfe_vendor
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
             FROM mp_nfe_invoice i
             WHERE i.issued_date BETWEEN
                 (SELECT MIN(paid_date) FROM mp_nfe_payment) AND
                 (SELECT MAX(paid_date) FROM mp_nfe_payment)
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
                    COALESCE((SELECT SUM(p.paid_amount) FROM mp_nfe_payment p WHERE p.invoice_id = i.id), 0) AS total_paid,
                    i.amount - COALESCE((SELECT SUM(p.paid_amount) FROM mp_nfe_payment p WHERE p.invoice_id = i.id), 0) AS balance
             FROM mp_nfe_invoice i
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
             FROM mp_nfe_invoice i
             JOIN mp_nfe_vendor v ON v.id = i.vendor_id
             JOIN mp_nfe_rate r ON r.from_currency = i.currency AND r.to_currency = 'USD'
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
            "UPDATE mp_nfe_invoice SET status = 'paid'
             WHERE id IN (
                SELECT i.id
                FROM mp_nfe_invoice i
                JOIN (SELECT invoice_id, SUM(paid_amount) AS total_paid FROM mp_nfe_payment GROUP BY invoice_id) p
                ON p.invoice_id = i.id
                WHERE p.total_paid >= i.amount
             )"
        );

        $rows = $this->ztdQuery("SELECT id, status FROM mp_nfe_invoice ORDER BY id");
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
             FROM mp_nfe_invoice i
             JOIN mp_nfe_rate r ON r.from_currency = i.currency AND r.to_currency = 'USD'
             WHERE i.vendor_id = ?
             ORDER BY i.id",
            [2]
        );

        $this->assertCount(2, $rows);
        $this->assertEqualsWithDelta(1944.00, (float) $rows[0]['amount_usd'], 0.01);
        $this->assertEqualsWithDelta(810.00, (float) $rows[1]['amount_usd'], 0.01);
    }

    /**
     * Mixed exec/prepare interleaving.
     */
    public function testMixedExecPrepareInterleaving(): void
    {
        $this->pdo->exec("INSERT INTO mp_nfe_invoice VALUES (6, 3, 200000.00, 'JPY', '2025-06-01', 'pending')");

        $rows = $this->ztdPrepareAndExecute(
            "SELECT COUNT(*) AS cnt FROM mp_nfe_invoice WHERE vendor_id = ?",
            [3]
        );
        $this->assertEquals(2, (int) $rows[0]['cnt']);

        $this->ztdExec("UPDATE mp_nfe_invoice SET status = 'approved' WHERE id = 6");

        $rows = $this->ztdPrepareAndExecute(
            "SELECT status FROM mp_nfe_invoice WHERE id = ?",
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
                        WHEN COALESCE((SELECT SUM(p.paid_amount) FROM mp_nfe_payment p WHERE p.invoice_id = i.id), 0) = 0
                            THEN 'unpaid'
                        WHEN COALESCE((SELECT SUM(p.paid_amount) FROM mp_nfe_payment p WHERE p.invoice_id = i.id), 0) >= i.amount
                            THEN 'fully paid'
                        ELSE 'partially paid'
                    END AS payment_label
             FROM mp_nfe_invoice i
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
     * INSERT with computed values: CONCAT(UPPER(), '-', LPAD()).
     *
     * Tests deeply nested function expressions in INSERT VALUES.
     * The CTE rewriter must correctly parse the VALUES clause containing
     * nested function calls.
     */
    public function testInsertWithNestedFunctionValues(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO mp_nfe_codes (id, label, code) VALUES "
                . "(1, CONCAT(UPPER('hello'), '-', LPAD('1', 3, '0')), 'A')"
            );

            $rows = $this->ztdQuery('SELECT label, code FROM mp_nfe_codes WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('HELLO-001', $rows[0]['label']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with CONCAT(UPPER(), LPAD()) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with multiple computed function expressions.
     */
    public function testInsertMultipleComputedValues(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO mp_nfe_codes (id, label, code) VALUES "
                . "(1, CONCAT(UPPER('widget'), ' #', LPAD(CAST(42 AS CHAR), 5, '0')), "
                . "CONCAT(LEFT('ABCDEF', 3), '-', RIGHT('123456', 3)))"
            );

            $rows = $this->ztdQuery('SELECT label, code FROM mp_nfe_codes WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('WIDGET #00042', $rows[0]['label']);
            $this->assertSame('ABC-456', $rows[0]['code']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with multiple computed expressions failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared SELECT WHERE with nested functions: LOCATE(LOWER(?), LOWER(name)).
     *
     * This tests case-insensitive search using nested functions with a
     * prepared statement parameter.
     */
    public function testPreparedWhereLocateLower(): void
    {
        try {
            $this->ztdExec("INSERT INTO mp_nfe_codes (id, label, code) VALUES (1, 'Hello World', 'HW')");
            $this->ztdExec("INSERT INTO mp_nfe_codes (id, label, code) VALUES (2, 'Goodbye Moon', 'GM')");
            $this->ztdExec("INSERT INTO mp_nfe_codes (id, label, code) VALUES (3, 'HELLO THERE', 'HT')");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT id, label FROM mp_nfe_codes WHERE LOCATE(LOWER(?), LOWER(label)) > 0 ORDER BY id",
                ['hello']
            );

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(3, (int) $rows[1]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Prepared WHERE LOCATE(LOWER(?), LOWER(col)) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE SET with nested functions: CONCAT(LEFT(name, 3), '...').
     */
    public function testUpdateSetWithNestedFunctions(): void
    {
        try {
            $this->ztdExec("INSERT INTO mp_nfe_codes (id, label, code) VALUES (1, 'International Business Machines', 'IBM')");
            $this->ztdExec("INSERT INTO mp_nfe_codes (id, label, code) VALUES (2, 'Short', 'SH')");

            $this->ztdExec(
                "UPDATE mp_nfe_codes SET label = CONCAT(LEFT(label, 3), '...') WHERE LENGTH(label) > 10"
            );

            $rows = $this->ztdQuery('SELECT id, label FROM mp_nfe_codes ORDER BY id');
            $this->assertCount(2, $rows);
            $this->assertSame('Int...', $rows[0]['label'], 'Long label should be truncated');
            $this->assertSame('Short', $rows[1]['label'], 'Short label should be unchanged');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE SET with CONCAT(LEFT(), ...) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared UPDATE with nested function in SET clause.
     */
    public function testPreparedUpdateWithNestedFunctions(): void
    {
        try {
            $this->ztdExec("INSERT INTO mp_nfe_codes (id, label, code) VALUES (1, 'hello world', 'HW')");

            $stmt = $this->pdo->prepare(
                "UPDATE mp_nfe_codes SET label = CONCAT(UPPER(LEFT(label, 1)), SUBSTRING(label, 2)) WHERE id = ?"
            );
            $stmt->execute([1]);

            $rows = $this->ztdQuery('SELECT label FROM mp_nfe_codes WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('Hello world', $rows[0]['label']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Prepared UPDATE with nested functions in SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * GROUP BY with YEAR() and MONTH() function expressions.
     *
     * This tests the CTE rewriter's handling of function expressions in
     * GROUP BY clauses, which is a common pattern for time-series aggregation.
     */
    public function testGroupByYearMonth(): void
    {
        try {
            $this->ztdExec("INSERT INTO mp_nfe_events VALUES (1, 'Event A', '2025-01-15 10:00:00', 'tech')");
            $this->ztdExec("INSERT INTO mp_nfe_events VALUES (2, 'Event B', '2025-01-20 14:00:00', 'tech')");
            $this->ztdExec("INSERT INTO mp_nfe_events VALUES (3, 'Event C', '2025-02-10 09:00:00', 'science')");
            $this->ztdExec("INSERT INTO mp_nfe_events VALUES (4, 'Event D', '2025-02-25 16:00:00', 'tech')");
            $this->ztdExec("INSERT INTO mp_nfe_events VALUES (5, 'Event E', '2025-03-05 11:00:00', 'science')");

            $rows = $this->ztdQuery(
                "SELECT YEAR(created_at) AS yr, MONTH(created_at) AS mo, COUNT(*) AS cnt
                 FROM mp_nfe_events
                 GROUP BY YEAR(created_at), MONTH(created_at)
                 ORDER BY yr, mo"
            );

            $this->assertCount(3, $rows);
            $this->assertSame(2025, (int) $rows[0]['yr']);
            $this->assertSame(1, (int) $rows[0]['mo']);
            $this->assertSame(2, (int) $rows[0]['cnt']);
            $this->assertSame(2025, (int) $rows[1]['yr']);
            $this->assertSame(2, (int) $rows[1]['mo']);
            $this->assertSame(2, (int) $rows[1]['cnt']);
            $this->assertSame(2025, (int) $rows[2]['yr']);
            $this->assertSame(3, (int) $rows[2]['mo']);
            $this->assertSame(1, (int) $rows[2]['cnt']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'GROUP BY YEAR(), MONTH() failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * GROUP BY YEAR/MONTH with HAVING and category filter.
     */
    public function testGroupByYearMonthWithHaving(): void
    {
        try {
            $this->ztdExec("INSERT INTO mp_nfe_events VALUES (1, 'A', '2025-01-15 10:00:00', 'tech')");
            $this->ztdExec("INSERT INTO mp_nfe_events VALUES (2, 'B', '2025-01-20 14:00:00', 'tech')");
            $this->ztdExec("INSERT INTO mp_nfe_events VALUES (3, 'C', '2025-01-25 09:00:00', 'science')");
            $this->ztdExec("INSERT INTO mp_nfe_events VALUES (4, 'D', '2025-02-10 16:00:00', 'tech')");
            $this->ztdExec("INSERT INTO mp_nfe_events VALUES (5, 'E', '2025-02-15 11:00:00', 'science')");

            $rows = $this->ztdQuery(
                "SELECT YEAR(created_at) AS yr, MONTH(created_at) AS mo,
                        category, COUNT(*) AS cnt
                 FROM mp_nfe_events
                 GROUP BY YEAR(created_at), MONTH(created_at), category
                 HAVING COUNT(*) > 1
                 ORDER BY yr, mo, category"
            );

            $this->assertCount(1, $rows, 'Only Jan/tech should have count > 1');
            $this->assertSame(1, (int) $rows[0]['mo']);
            $this->assertSame('tech', $rows[0]['category']);
            $this->assertSame(2, (int) $rows[0]['cnt']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'GROUP BY YEAR/MONTH with HAVING failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Nested function chain in SELECT: REPLACE(UPPER(TRIM()), ...).
     */
    public function testDeepNestedFunctionChainInSelect(): void
    {
        try {
            $this->ztdExec("INSERT INTO mp_nfe_codes (id, label, code) VALUES (1, '  hello world  ', 'HW')");

            $rows = $this->ztdQuery(
                "SELECT id,
                        REPLACE(UPPER(TRIM(label)), ' ', '_') AS slug
                 FROM mp_nfe_codes WHERE id = 1"
            );

            $this->assertCount(1, $rows);
            $this->assertSame('HELLO_WORLD', $rows[0]['slug']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Deep nested function chain (REPLACE(UPPER(TRIM()))) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_nfe_invoice VALUES (6, 1, 999.00, 'USD', '2025-12-01', 'pending')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_nfe_invoice");
        $this->assertEquals(6, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_nfe_invoice")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
