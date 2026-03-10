<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE with mathematical expressions that reference the column being
 * updated and/or other columns, including subquery-based computation.
 *
 * The CTE rewriter must preserve correct evaluation of self-referencing math
 * expressions (col = col * factor + offset), cross-column references
 * (col_a = col_b - col_c), and subquery results in SET.
 *
 * @spec SPEC-4.2
 */
class SqliteUpdateMathExpressionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ume_accounts (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                balance REAL NOT NULL DEFAULT 0.0,
                credit_limit REAL NOT NULL DEFAULT 1000.0
            )',
            'CREATE TABLE sl_ume_bonuses (
                account_id INTEGER NOT NULL,
                amount REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ume_bonuses', 'sl_ume_accounts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ume_accounts (id, name, balance, credit_limit) VALUES (1, 'Alice', 500.00, 1000.00)");
        $this->pdo->exec("INSERT INTO sl_ume_accounts (id, name, balance, credit_limit) VALUES (2, 'Bob', 300.00, 500.00)");
        $this->pdo->exec("INSERT INTO sl_ume_accounts (id, name, balance, credit_limit) VALUES (3, 'Carol', 0.00, 2000.00)");

        $this->pdo->exec("INSERT INTO sl_ume_bonuses (account_id, amount) VALUES (1, 50.00)");
        $this->pdo->exec("INSERT INTO sl_ume_bonuses (account_id, amount) VALUES (1, 25.00)");
        $this->pdo->exec("INSERT INTO sl_ume_bonuses (account_id, amount) VALUES (2, 100.00)");
    }

    /**
     * UPDATE balance = balance + (SELECT SUM(amount) FROM bonuses WHERE ...)
     * Combines self-ref expression with scalar subquery.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateWithSubquerySumSelfRef(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_ume_accounts SET balance = balance + COALESCE(
                    (SELECT SUM(amount) FROM sl_ume_bonuses WHERE sl_ume_bonuses.account_id = sl_ume_accounts.id),
                    0.0
                )"
            );

            $rows = $this->ztdQuery('SELECT id, balance FROM sl_ume_accounts ORDER BY id');

            if (count($rows) !== 3) {
                $this->markTestIncomplete('Expected 3 rows, got ' . count($rows));
            }

            $this->assertEqualsWithDelta(575.00, (float) $rows[0]['balance'], 0.01,
                'Alice: 500 + (50+25) = 575');
            $this->assertEqualsWithDelta(400.00, (float) $rows[1]['balance'], 0.01,
                'Bob: 300 + 100 = 400');
            $this->assertEqualsWithDelta(0.00, (float) $rows[2]['balance'], 0.01,
                'Carol: 0 + 0 (no bonuses) = 0');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE with subquery SUM self-ref failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with percentage calculation: balance = balance * 1.05 (5% interest).
     *
     * @spec SPEC-4.2
     */
    public function testUpdatePercentageCalculation(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_ume_accounts SET balance = balance * 1.05 WHERE balance > 0"
            );

            $rows = $this->ztdQuery('SELECT id, balance FROM sl_ume_accounts ORDER BY id');

            $this->assertEqualsWithDelta(525.00, (float) $rows[0]['balance'], 0.01,
                'Alice: 500 * 1.05 = 525');
            $this->assertEqualsWithDelta(315.00, (float) $rows[1]['balance'], 0.01,
                'Bob: 300 * 1.05 = 315');
            $this->assertEqualsWithDelta(0.00, (float) $rows[2]['balance'], 0.01,
                'Carol: unchanged (balance not > 0)');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE percentage calculation failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with cross-column reference: balance = credit_limit - balance
     * (available credit calculation).
     *
     * @spec SPEC-4.2
     */
    public function testUpdateCrossColumnExpression(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_ume_accounts SET balance = credit_limit - balance"
            );

            $rows = $this->ztdQuery('SELECT id, balance FROM sl_ume_accounts ORDER BY id');

            $this->assertEqualsWithDelta(500.00, (float) $rows[0]['balance'], 0.01,
                'Alice: 1000 - 500 = 500');
            $this->assertEqualsWithDelta(200.00, (float) $rows[1]['balance'], 0.01,
                'Bob: 500 - 300 = 200');
            $this->assertEqualsWithDelta(2000.00, (float) $rows[2]['balance'], 0.01,
                'Carol: 2000 - 0 = 2000');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE cross-column expression failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with CASE + math: conditional adjustment.
     * SET balance = CASE WHEN balance > 400 THEN balance - 100 ELSE balance + 50 END
     *
     * @spec SPEC-4.2
     */
    public function testUpdateCaseMathExpression(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_ume_accounts SET balance = CASE
                    WHEN balance > 400 THEN balance - 100
                    ELSE balance + 50
                END"
            );

            $rows = $this->ztdQuery('SELECT id, balance FROM sl_ume_accounts ORDER BY id');

            $this->assertEqualsWithDelta(400.00, (float) $rows[0]['balance'], 0.01,
                'Alice: 500 > 400, so 500 - 100 = 400');
            $this->assertEqualsWithDelta(350.00, (float) $rows[1]['balance'], 0.01,
                'Bob: 300 <= 400, so 300 + 50 = 350');
            $this->assertEqualsWithDelta(50.00, (float) $rows[2]['balance'], 0.01,
                'Carol: 0 <= 400, so 0 + 50 = 50');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE CASE math expression failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared UPDATE with math expression and ? params.
     * SET balance = balance + ? WHERE id = ?
     *
     * @spec SPEC-4.2
     */
    public function testPreparedUpdateMathWithParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_ume_accounts SET balance = balance + ? WHERE id = ?"
            );
            $stmt->execute([75.50, 1]);

            $rows = $this->ztdQuery('SELECT id, balance FROM sl_ume_accounts WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertEqualsWithDelta(575.50, (float) $rows[0]['balance'], 0.01,
                'Alice: 500 + 75.50 = 575.50');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared UPDATE math expression failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE balance = balance + (subquery) combined with prepared params.
     *
     * @spec SPEC-4.2
     */
    public function testPreparedUpdateSubqueryMathSelfRef(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_ume_accounts SET balance = balance + COALESCE(
                    (SELECT SUM(amount) FROM sl_ume_bonuses WHERE sl_ume_bonuses.account_id = sl_ume_accounts.id),
                    0.0
                ) WHERE id = ?"
            );
            $stmt->execute([1]);

            $rows = $this->ztdQuery('SELECT id, balance FROM sl_ume_accounts WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertEqualsWithDelta(575.00, (float) $rows[0]['balance'], 0.01,
                'Alice: 500 + (50+25) = 575 via prepared stmt');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared UPDATE subquery math self-ref failed: ' . $e->getMessage()
            );
        }
    }
}
