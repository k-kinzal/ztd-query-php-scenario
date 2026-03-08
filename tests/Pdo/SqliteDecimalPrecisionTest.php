<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Decimal precision: REAL type for financial calculations.
 * SQLite uses REAL (IEEE 754 double) — precision differs from MySQL/PostgreSQL DECIMAL.
 * @spec SPEC-4.1, SPEC-4.2, SPEC-3.1
 */
class SqliteDecimalPrecisionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE dp_ledger (id INTEGER PRIMARY KEY, description TEXT, amount REAL NOT NULL, tax REAL NULL)';
    }

    protected function getTableNames(): array
    {
        return ['dp_ledger'];
    }

    public function testInsertAndSelectDecimal(): void
    {
        $this->pdo->exec("INSERT INTO dp_ledger (id, description, amount, tax) VALUES (1, 'Sale', 99.99, 7.9992)");

        $rows = $this->ztdQuery('SELECT amount, tax FROM dp_ledger WHERE id = 1');
        $this->assertEqualsWithDelta(99.99, (float) $rows[0]['amount'], 0.001);
        $this->assertEqualsWithDelta(7.9992, (float) $rows[0]['tax'], 0.0001);
    }

    public function testDecimalArithmeticInUpdate(): void
    {
        $this->pdo->exec("INSERT INTO dp_ledger (id, description, amount, tax) VALUES (1, 'Item', 100.50, NULL)");

        $this->pdo->exec("UPDATE dp_ledger SET amount = amount + 0.75 WHERE id = 1");

        $rows = $this->ztdQuery('SELECT amount FROM dp_ledger WHERE id = 1');
        $this->assertEqualsWithDelta(101.25, (float) $rows[0]['amount'], 0.01);
    }

    public function testDecimalSumAggregation(): void
    {
        $this->pdo->exec("INSERT INTO dp_ledger (id, description, amount, tax) VALUES (1, 'A', 10.10, NULL)");
        $this->pdo->exec("INSERT INTO dp_ledger (id, description, amount, tax) VALUES (2, 'B', 20.20, NULL)");
        $this->pdo->exec("INSERT INTO dp_ledger (id, description, amount, tax) VALUES (3, 'C', 30.30, NULL)");

        $rows = $this->ztdQuery('SELECT SUM(amount) AS total FROM dp_ledger');
        $this->assertEqualsWithDelta(60.60, (float) $rows[0]['total'], 0.01);
    }

    public function testDecimalComparisonInWhere(): void
    {
        $this->pdo->exec("INSERT INTO dp_ledger (id, description, amount, tax) VALUES (1, 'Small', 9.99, NULL)");
        $this->pdo->exec("INSERT INTO dp_ledger (id, description, amount, tax) VALUES (2, 'Medium', 49.99, NULL)");
        $this->pdo->exec("INSERT INTO dp_ledger (id, description, amount, tax) VALUES (3, 'Large', 199.99, NULL)");

        $rows = $this->ztdQuery('SELECT id FROM dp_ledger WHERE amount >= 49.99 ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame(2, (int) $rows[0]['id']);
    }

    public function testNegativeDecimal(): void
    {
        $this->pdo->exec("INSERT INTO dp_ledger (id, description, amount, tax) VALUES (1, 'Refund', -50.25, NULL)");

        $rows = $this->ztdQuery('SELECT amount FROM dp_ledger WHERE id = 1');
        $this->assertEqualsWithDelta(-50.25, (float) $rows[0]['amount'], 0.01);
    }

    public function testPreparedDecimalBinding(): void
    {
        $this->pdo->exec("INSERT INTO dp_ledger (id, description, amount, tax) VALUES (1, 'Item', 100.00, NULL)");

        $rows = $this->ztdPrepareAndExecute('SELECT id FROM dp_ledger WHERE amount = ?', [100.00]);
        $this->assertCount(1, $rows);
    }

    public function testMultipleDecimalUpdates(): void
    {
        $this->pdo->exec("INSERT INTO dp_ledger (id, description, amount, tax) VALUES (1, 'Balance', 1000.00, NULL)");

        $this->pdo->exec("UPDATE dp_ledger SET amount = amount - 150.75 WHERE id = 1");
        $this->pdo->exec("UPDATE dp_ledger SET amount = amount - 299.99 WHERE id = 1");
        $this->pdo->exec("UPDATE dp_ledger SET amount = amount + 50.50 WHERE id = 1");

        $rows = $this->ztdQuery('SELECT amount FROM dp_ledger WHERE id = 1');
        $this->assertEqualsWithDelta(599.76, (float) $rows[0]['amount'], 0.01);
    }
}
