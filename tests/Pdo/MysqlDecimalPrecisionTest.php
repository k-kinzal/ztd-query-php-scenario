<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Decimal precision: DECIMAL(10,2) for financial calculations.
 * @spec SPEC-4.1, SPEC-4.2, SPEC-3.1
 */
class MysqlDecimalPrecisionTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_dp_ledger (id INT PRIMARY KEY, description VARCHAR(255), amount DECIMAL(10,2) NOT NULL, tax DECIMAL(10,4) NULL)';
    }

    protected function getTableNames(): array
    {
        return ['mp_dp_ledger'];
    }

    public function testInsertAndSelectDecimal(): void
    {
        $this->pdo->exec("INSERT INTO mp_dp_ledger (id, description, amount, tax) VALUES (1, 'Sale', 99.99, 7.9992)");

        $rows = $this->ztdQuery('SELECT amount, tax FROM mp_dp_ledger WHERE id = 1');
        $this->assertSame(99.99, (float) $rows[0]['amount']);
        $this->assertSame(7.9992, (float) $rows[0]['tax']);
    }

    public function testDecimalArithmeticInUpdate(): void
    {
        $this->pdo->exec("INSERT INTO mp_dp_ledger (id, description, amount, tax) VALUES (1, 'Item', 100.50, NULL)");

        $this->pdo->exec("UPDATE mp_dp_ledger SET amount = amount + 0.75 WHERE id = 1");

        $rows = $this->ztdQuery('SELECT amount FROM mp_dp_ledger WHERE id = 1');
        $this->assertSame(101.25, (float) $rows[0]['amount']);
    }

    public function testDecimalSumAggregation(): void
    {
        $this->pdo->exec("INSERT INTO mp_dp_ledger (id, description, amount, tax) VALUES (1, 'A', 10.10, NULL)");
        $this->pdo->exec("INSERT INTO mp_dp_ledger (id, description, amount, tax) VALUES (2, 'B', 20.20, NULL)");
        $this->pdo->exec("INSERT INTO mp_dp_ledger (id, description, amount, tax) VALUES (3, 'C', 30.30, NULL)");

        $rows = $this->ztdQuery('SELECT SUM(amount) AS total FROM mp_dp_ledger');
        $this->assertEqualsWithDelta(60.60, (float) $rows[0]['total'], 0.01);
    }

    public function testDecimalComparisonInWhere(): void
    {
        $this->pdo->exec("INSERT INTO mp_dp_ledger (id, description, amount, tax) VALUES (1, 'Small', 9.99, NULL)");
        $this->pdo->exec("INSERT INTO mp_dp_ledger (id, description, amount, tax) VALUES (2, 'Medium', 49.99, NULL)");
        $this->pdo->exec("INSERT INTO mp_dp_ledger (id, description, amount, tax) VALUES (3, 'Large', 199.99, NULL)");

        $rows = $this->ztdQuery('SELECT id FROM mp_dp_ledger WHERE amount >= 49.99 ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame(2, (int) $rows[0]['id']);
    }

    public function testNegativeDecimal(): void
    {
        $this->pdo->exec("INSERT INTO mp_dp_ledger (id, description, amount, tax) VALUES (1, 'Refund', -50.25, NULL)");

        $rows = $this->ztdQuery('SELECT amount FROM mp_dp_ledger WHERE id = 1');
        $this->assertSame(-50.25, (float) $rows[0]['amount']);
    }

    public function testPreparedDecimalBinding(): void
    {
        $this->pdo->exec("INSERT INTO mp_dp_ledger (id, description, amount, tax) VALUES (1, 'Item', 100.00, NULL)");

        $rows = $this->ztdPrepareAndExecute('SELECT id FROM mp_dp_ledger WHERE amount = ?', [100.00]);
        $this->assertCount(1, $rows);
    }

    public function testMultipleDecimalUpdates(): void
    {
        $this->pdo->exec("INSERT INTO mp_dp_ledger (id, description, amount, tax) VALUES (1, 'Balance', 1000.00, NULL)");

        $this->pdo->exec("UPDATE mp_dp_ledger SET amount = amount - 150.75 WHERE id = 1");
        $this->pdo->exec("UPDATE mp_dp_ledger SET amount = amount - 299.99 WHERE id = 1");
        $this->pdo->exec("UPDATE mp_dp_ledger SET amount = amount + 50.50 WHERE id = 1");

        $rows = $this->ztdQuery('SELECT amount FROM mp_dp_ledger WHERE id = 1');
        $this->assertEqualsWithDelta(599.76, (float) $rows[0]['amount'], 0.01);
    }
}
