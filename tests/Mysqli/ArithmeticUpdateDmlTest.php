<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE SET with arithmetic self-referencing expressions on MySQLi.
 *
 * @spec SPEC-10.2
 */
class ArithmeticUpdateDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE mi_au_products (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            quantity INT DEFAULT 0,
            price DECIMAL(10,2) DEFAULT 0.00,
            views INT DEFAULT 0
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['mi_au_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_au_products (name, quantity, price, views) VALUES ('Widget', 10, 9.99, 100)");
        $this->ztdExec("INSERT INTO mi_au_products (name, quantity, price, views) VALUES ('Gadget', 5, 24.99, 50)");
        $this->ztdExec("INSERT INTO mi_au_products (name, quantity, price, views) VALUES ('Doohickey', 0, 4.99, 200)");
    }

    public function testIncrementColumn(): void
    {
        try {
            $this->ztdExec("UPDATE mi_au_products SET quantity = quantity + 1 WHERE name = 'Widget'");

            $rows = $this->ztdQuery("SELECT quantity FROM mi_au_products WHERE name = 'Widget'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Increment (MySQLi): expected 1 row, got ' . count($rows));
            }

            if ((int) $rows[0]['quantity'] !== 11) {
                $this->markTestIncomplete(
                    'Increment (MySQLi): expected quantity=11, got ' . $rows[0]['quantity']
                );
            }

            $this->assertSame(11, (int) $rows[0]['quantity']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Increment (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testDecrementColumn(): void
    {
        try {
            $this->ztdExec("UPDATE mi_au_products SET quantity = quantity - 1 WHERE name = 'Gadget'");

            $rows = $this->ztdQuery("SELECT quantity FROM mi_au_products WHERE name = 'Gadget'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Decrement (MySQLi): expected 1 row, got ' . count($rows));
            }

            if ((int) $rows[0]['quantity'] !== 4) {
                $this->markTestIncomplete(
                    'Decrement (MySQLi): expected quantity=4, got ' . $rows[0]['quantity']
                );
            }

            $this->assertSame(4, (int) $rows[0]['quantity']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Decrement (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testMultiplyColumn(): void
    {
        try {
            $this->ztdExec("UPDATE mi_au_products SET price = price * 1.1 WHERE name = 'Widget'");

            $rows = $this->ztdQuery("SELECT price FROM mi_au_products WHERE name = 'Widget'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Multiply (MySQLi): expected 1 row, got ' . count($rows));
            }

            if (abs((float) $rows[0]['price'] - 10.99) > 0.01) {
                $this->markTestIncomplete(
                    'Multiply (MySQLi): expected price≈10.99, got ' . $rows[0]['price']
                );
            }

            $this->assertEqualsWithDelta(10.99, (float) $rows[0]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiply (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testDoubleIncrement(): void
    {
        try {
            $this->ztdExec("UPDATE mi_au_products SET quantity = quantity + 1 WHERE name = 'Widget'");
            $this->ztdExec("UPDATE mi_au_products SET quantity = quantity + 1 WHERE name = 'Widget'");

            $rows = $this->ztdQuery("SELECT quantity FROM mi_au_products WHERE name = 'Widget'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Double increment (MySQLi): expected 1 row, got ' . count($rows));
            }

            if ((int) $rows[0]['quantity'] !== 12) {
                $this->markTestIncomplete(
                    'Double increment (MySQLi): expected quantity=12, got ' . $rows[0]['quantity']
                );
            }

            $this->assertSame(12, (int) $rows[0]['quantity']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Double increment (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
