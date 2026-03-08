<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Decimal precision: DECIMAL(10,2) for financial calculations.
 * E-commerce and accounting apps depend on exact decimal handling.
 * @spec SPEC-4.1, SPEC-4.2, SPEC-3.1
 */
class DecimalPrecisionTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_dp_ledger (id INT PRIMARY KEY, description VARCHAR(255), amount DECIMAL(10,2) NOT NULL, tax DECIMAL(10,4) NULL)';
    }

    protected function getTableNames(): array
    {
        return ['mi_dp_ledger'];
    }

    public function testInsertAndSelectDecimal(): void
    {
        $this->mysqli->query("INSERT INTO mi_dp_ledger (id, description, amount, tax) VALUES (1, 'Sale', 99.99, 7.9992)");

        $rows = $this->ztdQuery('SELECT amount, tax FROM mi_dp_ledger WHERE id = 1');
        $this->assertSame(99.99, (float) $rows[0]['amount']);
        $this->assertSame(7.9992, (float) $rows[0]['tax']);
    }

    public function testDecimalArithmeticInUpdate(): void
    {
        $this->mysqli->query("INSERT INTO mi_dp_ledger (id, description, amount, tax) VALUES (1, 'Item', 100.50, NULL)");

        $this->mysqli->query("UPDATE mi_dp_ledger SET amount = amount + 0.75 WHERE id = 1");

        $rows = $this->ztdQuery('SELECT amount FROM mi_dp_ledger WHERE id = 1');
        $this->assertSame(101.25, (float) $rows[0]['amount']);
    }

    public function testDecimalSumAggregation(): void
    {
        $this->mysqli->query("INSERT INTO mi_dp_ledger (id, description, amount, tax) VALUES (1, 'A', 10.10, NULL)");
        $this->mysqli->query("INSERT INTO mi_dp_ledger (id, description, amount, tax) VALUES (2, 'B', 20.20, NULL)");
        $this->mysqli->query("INSERT INTO mi_dp_ledger (id, description, amount, tax) VALUES (3, 'C', 30.30, NULL)");

        $rows = $this->ztdQuery('SELECT SUM(amount) AS total FROM mi_dp_ledger');
        $this->assertEqualsWithDelta(60.60, (float) $rows[0]['total'], 0.01);
    }

    public function testDecimalComparisonInWhere(): void
    {
        $this->mysqli->query("INSERT INTO mi_dp_ledger (id, description, amount, tax) VALUES (1, 'Small', 9.99, NULL)");
        $this->mysqli->query("INSERT INTO mi_dp_ledger (id, description, amount, tax) VALUES (2, 'Medium', 49.99, NULL)");
        $this->mysqli->query("INSERT INTO mi_dp_ledger (id, description, amount, tax) VALUES (3, 'Large', 199.99, NULL)");

        $rows = $this->ztdQuery('SELECT id FROM mi_dp_ledger WHERE amount >= 49.99 ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame(2, (int) $rows[0]['id']);
        $this->assertSame(3, (int) $rows[1]['id']);
    }

    public function testDecimalWithPreparedStatement(): void
    {
        $this->mysqli->query("INSERT INTO mi_dp_ledger (id, description, amount, tax) VALUES (1, 'Item', 100.00, NULL)");

        $rows = $this->ztdPrepareAndExecute(
            'SELECT id FROM mi_dp_ledger WHERE amount = ?',
            [100.00]
        );
        $this->assertCount(1, $rows);
    }

    public function testNegativeDecimal(): void
    {
        $this->mysqli->query("INSERT INTO mi_dp_ledger (id, description, amount, tax) VALUES (1, 'Refund', -50.25, NULL)");

        $rows = $this->ztdQuery('SELECT amount FROM mi_dp_ledger WHERE id = 1');
        $this->assertSame(-50.25, (float) $rows[0]['amount']);
    }

    public function testDecimalRoundingInExpression(): void
    {
        $this->mysqli->query("INSERT INTO mi_dp_ledger (id, description, amount, tax) VALUES (1, 'Item', 100.00, NULL)");

        $rows = $this->ztdQuery('SELECT ROUND(amount * 0.075, 2) AS tax_amount FROM mi_dp_ledger WHERE id = 1');
        $this->assertSame(7.50, (float) $rows[0]['tax_amount']);
    }

    public function testMultipleDecimalUpdates(): void
    {
        $this->mysqli->query("INSERT INTO mi_dp_ledger (id, description, amount, tax) VALUES (1, 'Balance', 1000.00, NULL)");

        // Simulate multiple transactions
        $this->mysqli->query("UPDATE mi_dp_ledger SET amount = amount - 150.75 WHERE id = 1");
        $this->mysqli->query("UPDATE mi_dp_ledger SET amount = amount - 299.99 WHERE id = 1");
        $this->mysqli->query("UPDATE mi_dp_ledger SET amount = amount + 50.50 WHERE id = 1");

        $rows = $this->ztdQuery('SELECT amount FROM mi_dp_ledger WHERE id = 1');
        $this->assertEqualsWithDelta(599.76, (float) $rows[0]['amount'], 0.01);
    }
}
