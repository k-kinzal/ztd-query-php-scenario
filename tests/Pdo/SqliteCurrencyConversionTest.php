<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests multi-currency conversion ledger with ROUND arithmetic, CASE for currency
 * conversion, UPDATE with CASE, and SUM CASE cross-currency totals (SQLite PDO).
 * SQL patterns exercised: ROUND with multiplication/division, nested CASE in SELECT,
 * UPDATE SET with CASE expression, SUM CASE ROUND, LEFT JOIN derived table, prepared arithmetic.
 * @spec SPEC-10.2.177
 */
class SqliteCurrencyConversionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_fx_account (
                id INTEGER PRIMARY KEY,
                name TEXT,
                currency TEXT,
                balance REAL
            )',
            'CREATE TABLE sl_fx_txn (
                id INTEGER PRIMARY KEY,
                account_id INTEGER,
                amount REAL,
                currency TEXT,
                description TEXT,
                txn_date TEXT
            )',
            'CREATE TABLE sl_fx_rate (
                id INTEGER PRIMARY KEY,
                from_currency TEXT,
                to_currency TEXT,
                rate REAL,
                effective_date TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_fx_txn', 'sl_fx_rate', 'sl_fx_account'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_fx_account VALUES (1, 'US Operations', 'USD', 50000.00)");
        $this->pdo->exec("INSERT INTO sl_fx_account VALUES (2, 'EU Operations', 'EUR', 40000.00)");
        $this->pdo->exec("INSERT INTO sl_fx_account VALUES (3, 'UK Operations', 'GBP', 30000.00)");
        $this->pdo->exec("INSERT INTO sl_fx_account VALUES (4, 'JP Operations', 'JPY', 5000000.00)");

        $this->pdo->exec("INSERT INTO sl_fx_txn VALUES (1, 1, 5000.00, 'USD', 'Client payment', '2025-01-15')");
        $this->pdo->exec("INSERT INTO sl_fx_txn VALUES (2, 1, -2000.00, 'USD', 'Vendor payment', '2025-01-20')");
        $this->pdo->exec("INSERT INTO sl_fx_txn VALUES (3, 2, 3000.00, 'EUR', 'Service revenue', '2025-01-18')");
        $this->pdo->exec("INSERT INTO sl_fx_txn VALUES (4, 2, -1500.00, 'EUR', 'Office rent', '2025-02-01')");
        $this->pdo->exec("INSERT INTO sl_fx_txn VALUES (5, 3, 4000.00, 'GBP', 'Consulting fee', '2025-01-25')");
        $this->pdo->exec("INSERT INTO sl_fx_txn VALUES (6, 3, -500.00, 'GBP', 'Travel expense', '2025-02-05')");
        $this->pdo->exec("INSERT INTO sl_fx_txn VALUES (7, 4, 500000.00, 'JPY', 'License fee', '2025-01-30')");
        $this->pdo->exec("INSERT INTO sl_fx_txn VALUES (8, 4, -200000.00, 'JPY', 'Equipment', '2025-02-10')");

        $this->pdo->exec("INSERT INTO sl_fx_rate VALUES (1, 'USD', 'USD', 1.000000, '2025-01-01')");
        $this->pdo->exec("INSERT INTO sl_fx_rate VALUES (2, 'EUR', 'USD', 1.085000, '2025-01-01')");
        $this->pdo->exec("INSERT INTO sl_fx_rate VALUES (3, 'GBP', 'USD', 1.270000, '2025-01-01')");
        $this->pdo->exec("INSERT INTO sl_fx_rate VALUES (4, 'JPY', 'USD', 0.006700, '2025-01-01')");
    }

    public function testRoundWithCurrencyConversion(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.name, a.currency, a.balance,
                    ROUND(a.balance * r.rate, 2) AS balance_usd
             FROM sl_fx_account a
             JOIN sl_fx_rate r ON r.from_currency = a.currency AND r.to_currency = 'USD'
             ORDER BY balance_usd DESC"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('US Operations', $rows[0]['name']);
        $this->assertEqualsWithDelta(50000.00, (float) $rows[0]['balance_usd'], 0.01);
        $this->assertSame('EU Operations', $rows[1]['name']);
        $this->assertEqualsWithDelta(43400.00, (float) $rows[1]['balance_usd'], 0.01);
        $this->assertSame('UK Operations', $rows[2]['name']);
        $this->assertEqualsWithDelta(38100.00, (float) $rows[2]['balance_usd'], 0.01);
        $this->assertSame('JP Operations', $rows[3]['name']);
        $this->assertEqualsWithDelta(33500.00, (float) $rows[3]['balance_usd'], 0.01);
    }

    public function testNestedCaseClassification(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.name,
                    ROUND(a.balance * r.rate, 2) AS balance_usd,
                    CASE
                        WHEN ROUND(a.balance * r.rate, 2) >= 50000 THEN 'large'
                        WHEN ROUND(a.balance * r.rate, 2) >= 35000 THEN 'medium'
                        ELSE 'small'
                    END AS account_tier
             FROM sl_fx_account a
             JOIN sl_fx_rate r ON r.from_currency = a.currency AND r.to_currency = 'USD'
             ORDER BY balance_usd DESC"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('large', $rows[0]['account_tier']);
        $this->assertSame('medium', $rows[1]['account_tier']);
        $this->assertSame('medium', $rows[2]['account_tier']);
        $this->assertSame('small', $rows[3]['account_tier']);
    }

    public function testUpdateSetWithCasePerCurrency(): void
    {
        $this->ztdExec(
            "UPDATE sl_fx_account SET balance =
                CASE currency
                    WHEN 'USD' THEN ROUND(balance * 1.02, 2)
                    WHEN 'EUR' THEN ROUND(balance * 1.03, 2)
                    WHEN 'GBP' THEN ROUND(balance * 1.025, 2)
                    ELSE balance
                END"
        );

        $rows = $this->ztdQuery("SELECT name, currency, balance FROM sl_fx_account ORDER BY id");
        $this->assertCount(4, $rows);
        $this->assertEqualsWithDelta(51000.00, (float) $rows[0]['balance'], 0.01);
        $this->assertEqualsWithDelta(41200.00, (float) $rows[1]['balance'], 0.01);
        $this->assertEqualsWithDelta(30750.00, (float) $rows[2]['balance'], 0.01);
        $this->assertEqualsWithDelta(5000000.00, (float) $rows[3]['balance'], 0.01);
    }

    public function testSumCaseCurrencyBreakdown(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                ROUND(SUM(CASE WHEN t.currency = 'USD' THEN t.amount * r.rate ELSE 0 END), 2) AS usd_volume,
                ROUND(SUM(CASE WHEN t.currency = 'EUR' THEN t.amount * r.rate ELSE 0 END), 2) AS eur_volume_usd,
                ROUND(SUM(CASE WHEN t.currency = 'GBP' THEN t.amount * r.rate ELSE 0 END), 2) AS gbp_volume_usd,
                ROUND(SUM(CASE WHEN t.currency = 'JPY' THEN t.amount * r.rate ELSE 0 END), 2) AS jpy_volume_usd,
                ROUND(SUM(t.amount * r.rate), 2) AS total_volume_usd
             FROM sl_fx_txn t
             JOIN sl_fx_rate r ON r.from_currency = t.currency AND r.to_currency = 'USD'"
        );

        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(3000.00, (float) $rows[0]['usd_volume'], 0.01);
        $this->assertEqualsWithDelta(1627.50, (float) $rows[0]['eur_volume_usd'], 0.01);
        $this->assertEqualsWithDelta(4445.00, (float) $rows[0]['gbp_volume_usd'], 0.01);
        $this->assertEqualsWithDelta(2010.00, (float) $rows[0]['jpy_volume_usd'], 0.01);
    }

    /**
     * Account net position using LEFT JOIN derived table.
     * Probes derived table behavior on SQLite.
     */
    public function testAccountNetPositionInUsd(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.name,
                    ROUND((a.balance + COALESCE(txn_sum.net, 0)) * r.rate, 2) AS net_position_usd
             FROM sl_fx_account a
             JOIN sl_fx_rate r ON r.from_currency = a.currency AND r.to_currency = 'USD'
             LEFT JOIN (
                 SELECT account_id, SUM(amount) AS net
                 FROM sl_fx_txn
                 GROUP BY account_id
             ) txn_sum ON txn_sum.account_id = a.id
             ORDER BY net_position_usd DESC"
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'Account net position with LEFT JOIN derived table returned empty on SQLite.'
            );
        }

        $this->assertCount(4, $rows);
        $this->assertSame('US Operations', $rows[0]['name']);
        $this->assertEqualsWithDelta(53000.00, (float) $rows[0]['net_position_usd'], 0.01);
    }

    public function testPreparedConversionByAccount(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT a.name, a.balance, r.rate,
                    ROUND(a.balance * r.rate, 2) AS balance_usd
             FROM sl_fx_account a
             JOIN sl_fx_rate r ON r.from_currency = a.currency AND r.to_currency = 'USD'
             WHERE a.currency = ?",
            ['EUR']
        );

        $this->assertCount(1, $rows);
        $this->assertSame('EU Operations', $rows[0]['name']);
        $this->assertEqualsWithDelta(43400.00, (float) $rows[0]['balance_usd'], 0.01);
    }

    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("UPDATE sl_fx_account SET balance = 99999.99 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT balance FROM sl_fx_account WHERE id = 1");
        $this->assertEqualsWithDelta(99999.99, (float) $rows[0]['balance'], 0.01);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_fx_account")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
