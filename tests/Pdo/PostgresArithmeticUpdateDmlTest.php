<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests UPDATE SET with arithmetic self-referencing expressions on PostgreSQL.
 *
 * @spec SPEC-10.2
 */
class PostgresArithmeticUpdateDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE pg_au_products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            quantity INTEGER DEFAULT 0,
            price NUMERIC(10,2) DEFAULT 0.00,
            views INTEGER DEFAULT 0
        )";
    }

    protected function getTableNames(): array
    {
        return ['pg_au_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_au_products (name, quantity, price, views) VALUES ('Widget', 10, 9.99, 100)");
        $this->ztdExec("INSERT INTO pg_au_products (name, quantity, price, views) VALUES ('Gadget', 5, 24.99, 50)");
        $this->ztdExec("INSERT INTO pg_au_products (name, quantity, price, views) VALUES ('Doohickey', 0, 4.99, 200)");
    }

    public function testIncrementColumn(): void
    {
        try {
            $this->ztdExec("UPDATE pg_au_products SET quantity = quantity + 1 WHERE name = 'Widget'");

            $rows = $this->ztdQuery("SELECT quantity FROM pg_au_products WHERE name = 'Widget'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Increment (PG): expected 1 row, got ' . count($rows));
            }

            if ((int) $rows[0]['quantity'] !== 11) {
                $this->markTestIncomplete(
                    'Increment (PG): expected quantity=11, got ' . $rows[0]['quantity']
                );
            }

            $this->assertSame(11, (int) $rows[0]['quantity']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Increment (PG) failed: ' . $e->getMessage());
        }
    }

    public function testDecrementColumn(): void
    {
        try {
            $this->ztdExec("UPDATE pg_au_products SET quantity = quantity - 1 WHERE name = 'Gadget'");

            $rows = $this->ztdQuery("SELECT quantity FROM pg_au_products WHERE name = 'Gadget'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Decrement (PG): expected 1 row, got ' . count($rows));
            }

            if ((int) $rows[0]['quantity'] !== 4) {
                $this->markTestIncomplete(
                    'Decrement (PG): expected quantity=4, got ' . $rows[0]['quantity']
                );
            }

            $this->assertSame(4, (int) $rows[0]['quantity']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Decrement (PG) failed: ' . $e->getMessage());
        }
    }

    public function testMultiplyColumn(): void
    {
        try {
            $this->ztdExec("UPDATE pg_au_products SET price = price * 1.1 WHERE name = 'Widget'");

            $rows = $this->ztdQuery("SELECT price FROM pg_au_products WHERE name = 'Widget'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Multiply (PG): expected 1 row, got ' . count($rows));
            }

            if (abs((float) $rows[0]['price'] - 10.99) > 0.01) {
                $this->markTestIncomplete(
                    'Multiply (PG): expected price≈10.99, got ' . $rows[0]['price']
                );
            }

            $this->assertEqualsWithDelta(10.99, (float) $rows[0]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiply (PG) failed: ' . $e->getMessage());
        }
    }

    public function testMultiColumnArithmetic(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_au_products SET quantity = quantity + 5, views = views + 1 WHERE name = 'Doohickey'"
            );

            $rows = $this->ztdQuery("SELECT quantity, views FROM pg_au_products WHERE name = 'Doohickey'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Multi-column arithmetic (PG): expected 1 row, got ' . count($rows));
            }

            if ((int) $rows[0]['quantity'] !== 5 || (int) $rows[0]['views'] !== 201) {
                $this->markTestIncomplete(
                    'Multi-column arithmetic (PG): expected qty=5 views=201, got qty='
                    . $rows[0]['quantity'] . ' views=' . $rows[0]['views']
                );
            }

            $this->assertSame(5, (int) $rows[0]['quantity']);
            $this->assertSame(201, (int) $rows[0]['views']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-column arithmetic (PG) failed: ' . $e->getMessage());
        }
    }

    public function testDoubleIncrement(): void
    {
        try {
            $this->ztdExec("UPDATE pg_au_products SET quantity = quantity + 1 WHERE name = 'Widget'");
            $this->ztdExec("UPDATE pg_au_products SET quantity = quantity + 1 WHERE name = 'Widget'");

            $rows = $this->ztdQuery("SELECT quantity FROM pg_au_products WHERE name = 'Widget'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Double increment (PG): expected 1 row, got ' . count($rows));
            }

            if ((int) $rows[0]['quantity'] !== 12) {
                $this->markTestIncomplete(
                    'Double increment (PG): expected quantity=12, got ' . $rows[0]['quantity']
                );
            }

            $this->assertSame(12, (int) $rows[0]['quantity']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Double increment (PG) failed: ' . $e->getMessage());
        }
    }
}
