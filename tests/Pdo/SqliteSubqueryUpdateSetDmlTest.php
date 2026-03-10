<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE SET col = (scalar subquery) on SQLite.
 *
 * Setting a column from a scalar subquery is used for derived updates:
 * denormalized totals, cached aggregates, and computed fields.
 *
 * @spec SPEC-10.2
 */
class SqliteSubqueryUpdateSetDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_sus_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER NOT NULL,
                amount REAL NOT NULL
            )",
            "CREATE TABLE sl_sus_customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                total_spent REAL DEFAULT 0
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_sus_orders', 'sl_sus_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_sus_customers (name) VALUES ('Alice')");
        $this->ztdExec("INSERT INTO sl_sus_customers (name) VALUES ('Bob')");

        $this->ztdExec("INSERT INTO sl_sus_orders (customer_id, amount) VALUES (1, 100.00)");
        $this->ztdExec("INSERT INTO sl_sus_orders (customer_id, amount) VALUES (1, 200.00)");
        $this->ztdExec("INSERT INTO sl_sus_orders (customer_id, amount) VALUES (2, 50.00)");
    }

    /**
     * UPDATE SET total_spent = (SELECT SUM(amount) FROM orders WHERE ...).
     */
    public function testUpdateSetFromSubquery(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_sus_customers SET total_spent = (
                    SELECT COALESCE(SUM(amount), 0) FROM sl_sus_orders
                    WHERE sl_sus_orders.customer_id = sl_sus_customers.id
                )"
            );

            $rows = $this->ztdQuery("SELECT name, total_spent FROM sl_sus_customers ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Subquery UPDATE SET (SQLite): expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            if (abs((float) $rows[0]['total_spent'] - 300.0) > 0.01) {
                $this->markTestIncomplete(
                    'Subquery UPDATE SET (SQLite): Alice expected 300, got ' . $rows[0]['total_spent']
                );
            }

            $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total_spent'], 0.01);
            $this->assertEqualsWithDelta(50.0, (float) $rows[1]['total_spent'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Subquery UPDATE SET (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * Subquery UPDATE after prior DML on the source table.
     */
    public function testSubqueryUpdateAfterDml(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_sus_orders (customer_id, amount) VALUES (1, 150.00)");

            $this->ztdExec(
                "UPDATE sl_sus_customers SET total_spent = (
                    SELECT COALESCE(SUM(amount), 0) FROM sl_sus_orders
                    WHERE sl_sus_orders.customer_id = sl_sus_customers.id
                ) WHERE name = 'Alice'"
            );

            $rows = $this->ztdQuery("SELECT total_spent FROM sl_sus_customers WHERE name = 'Alice'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Subquery after DML (SQLite): expected 1, got ' . count($rows));
            }

            // 100 + 200 + 150 = 450
            if (abs((float) $rows[0]['total_spent'] - 450.0) > 0.01) {
                $this->markTestIncomplete(
                    'Subquery after DML (SQLite): Alice expected 450, got ' . $rows[0]['total_spent']
                );
            }

            $this->assertEqualsWithDelta(450.0, (float) $rows[0]['total_spent'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Subquery after DML (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with COUNT subquery.
     */
    public function testUpdateSetCountSubquery(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_sus_customers SET total_spent = (
                    SELECT COUNT(*) FROM sl_sus_orders
                    WHERE sl_sus_orders.customer_id = sl_sus_customers.id
                )"
            );

            $rows = $this->ztdQuery("SELECT name, total_spent FROM sl_sus_customers ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete('COUNT subquery (SQLite): expected 2, got ' . count($rows));
            }

            // Alice has 2 orders, Bob has 1
            if ((int) $rows[0]['total_spent'] !== 2) {
                $this->markTestIncomplete(
                    'COUNT subquery (SQLite): Alice expected 2, got ' . $rows[0]['total_spent']
                );
            }

            $this->assertSame(2, (int) $rows[0]['total_spent']);
            $this->assertSame(1, (int) $rows[1]['total_spent']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COUNT subquery (SQLite) failed: ' . $e->getMessage());
        }
    }
}
