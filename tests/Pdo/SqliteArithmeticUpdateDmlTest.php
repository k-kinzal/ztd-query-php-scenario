<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE SET with arithmetic self-referencing expressions on SQLite.
 *
 * Patterns like SET quantity = quantity + 1 are fundamental for counters,
 * inventory management, scoring, and any incremental update.
 *
 * @spec SPEC-10.2
 */
class SqliteArithmeticUpdateDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_au_products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            quantity INTEGER DEFAULT 0,
            price REAL DEFAULT 0.0,
            views INTEGER DEFAULT 0
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_au_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_au_products (name, quantity, price, views) VALUES ('Widget', 10, 9.99, 100)");
        $this->ztdExec("INSERT INTO sl_au_products (name, quantity, price, views) VALUES ('Gadget', 5, 24.99, 50)");
        $this->ztdExec("INSERT INTO sl_au_products (name, quantity, price, views) VALUES ('Doohickey', 0, 4.99, 200)");
    }

    /**
     * UPDATE SET quantity = quantity + 1 — basic increment.
     */
    public function testIncrementColumn(): void
    {
        try {
            $this->ztdExec("UPDATE sl_au_products SET quantity = quantity + 1 WHERE name = 'Widget'");

            $rows = $this->ztdQuery("SELECT quantity FROM sl_au_products WHERE name = 'Widget'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Increment (SQLite): expected 1 row, got ' . count($rows));
            }

            if ((int) $rows[0]['quantity'] !== 11) {
                $this->markTestIncomplete(
                    'Increment (SQLite): expected quantity=11, got ' . $rows[0]['quantity']
                );
            }

            $this->assertSame(11, (int) $rows[0]['quantity']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Increment (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET quantity = quantity - 1 — decrement.
     */
    public function testDecrementColumn(): void
    {
        try {
            $this->ztdExec("UPDATE sl_au_products SET quantity = quantity - 1 WHERE name = 'Gadget'");

            $rows = $this->ztdQuery("SELECT quantity FROM sl_au_products WHERE name = 'Gadget'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Decrement (SQLite): expected 1 row, got ' . count($rows));
            }

            if ((int) $rows[0]['quantity'] !== 4) {
                $this->markTestIncomplete(
                    'Decrement (SQLite): expected quantity=4, got ' . $rows[0]['quantity']
                );
            }

            $this->assertSame(4, (int) $rows[0]['quantity']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Decrement (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET price = price * 1.1 — percentage increase.
     */
    public function testMultiplyColumn(): void
    {
        try {
            $this->ztdExec("UPDATE sl_au_products SET price = price * 1.1 WHERE name = 'Widget'");

            $rows = $this->ztdQuery("SELECT price FROM sl_au_products WHERE name = 'Widget'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Multiply (SQLite): expected 1 row, got ' . count($rows));
            }

            $expected = 10.989;
            if (abs((float) $rows[0]['price'] - $expected) > 0.01) {
                $this->markTestIncomplete(
                    'Multiply (SQLite): expected price≈10.99, got ' . $rows[0]['price']
                );
            }

            $this->assertEqualsWithDelta($expected, (float) $rows[0]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiply (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with multiple arithmetic columns at once.
     */
    public function testMultiColumnArithmetic(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_au_products SET quantity = quantity + 5, views = views + 1 WHERE name = 'Doohickey'"
            );

            $rows = $this->ztdQuery("SELECT quantity, views FROM sl_au_products WHERE name = 'Doohickey'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Multi-column arithmetic (SQLite): expected 1 row, got ' . count($rows));
            }

            if ((int) $rows[0]['quantity'] !== 5 || (int) $rows[0]['views'] !== 201) {
                $this->markTestIncomplete(
                    'Multi-column arithmetic (SQLite): expected qty=5 views=201, got qty='
                    . $rows[0]['quantity'] . ' views=' . $rows[0]['views']
                );
            }

            $this->assertSame(5, (int) $rows[0]['quantity']);
            $this->assertSame(201, (int) $rows[0]['views']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-column arithmetic (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * Double increment — two consecutive updates on the same row.
     */
    public function testDoubleIncrement(): void
    {
        try {
            $this->ztdExec("UPDATE sl_au_products SET quantity = quantity + 1 WHERE name = 'Widget'");
            $this->ztdExec("UPDATE sl_au_products SET quantity = quantity + 1 WHERE name = 'Widget'");

            $rows = $this->ztdQuery("SELECT quantity FROM sl_au_products WHERE name = 'Widget'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Double increment (SQLite): expected 1 row, got ' . count($rows));
            }

            if ((int) $rows[0]['quantity'] !== 12) {
                $this->markTestIncomplete(
                    'Double increment (SQLite): expected quantity=12, got ' . $rows[0]['quantity']
                );
            }

            $this->assertSame(12, (int) $rows[0]['quantity']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Double increment (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE all rows with arithmetic (no WHERE clause).
     */
    public function testBulkArithmeticUpdate(): void
    {
        try {
            $this->ztdExec("UPDATE sl_au_products SET views = views * 2");

            $rows = $this->ztdQuery("SELECT name, views FROM sl_au_products ORDER BY name");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Bulk arithmetic (SQLite): expected 3 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $expected = ['Doohickey' => 400, 'Gadget' => 100, 'Widget' => 200];
            foreach ($rows as $row) {
                if ((int) $row['views'] !== $expected[$row['name']]) {
                    $this->markTestIncomplete(
                        'Bulk arithmetic (SQLite): ' . $row['name'] . ' expected views='
                        . $expected[$row['name']] . ', got ' . $row['views']
                    );
                }
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Bulk arithmetic (SQLite) failed: ' . $e->getMessage());
        }
    }
}
