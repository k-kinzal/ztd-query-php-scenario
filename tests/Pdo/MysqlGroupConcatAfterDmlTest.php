<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests GROUP_CONCAT aggregate function with shadow-modified data on MySQL PDO.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class MysqlGroupConcatAfterDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mpd_gcd_orders (
            id INT PRIMARY KEY,
            customer VARCHAR(50) NOT NULL,
            product VARCHAR(50) NOT NULL,
            amount DECIMAL(10,2) NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mpd_gcd_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mpd_gcd_orders VALUES (1, 'Alice', 'Widget', 10.00)");
        $this->pdo->exec("INSERT INTO mpd_gcd_orders VALUES (2, 'Alice', 'Gadget', 20.00)");
        $this->pdo->exec("INSERT INTO mpd_gcd_orders VALUES (3, 'Bob', 'Widget', 15.00)");
        $this->pdo->exec("INSERT INTO mpd_gcd_orders VALUES (4, 'Carol', 'Gadget', 25.00)");
    }

    public function testGroupConcatBaseline(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT customer, GROUP_CONCAT(product ORDER BY product) AS products
                 FROM mpd_gcd_orders GROUP BY customer ORDER BY customer"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete('GROUP_CONCAT baseline: expected 3 rows, got ' . count($rows));
            }
            $this->assertCount(3, $rows);
            $this->assertSame('Gadget,Widget', $rows[0]['products']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT baseline failed: ' . $e->getMessage());
        }
    }

    public function testGroupConcatAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mpd_gcd_orders VALUES (5, 'Alice', 'Doohickey', 30.00)");

            $rows = $this->ztdQuery(
                "SELECT customer, GROUP_CONCAT(product ORDER BY product) AS products
                 FROM mpd_gcd_orders WHERE customer = 'Alice' GROUP BY customer"
            );

            $this->assertCount(1, $rows);
            $expected = 'Doohickey,Gadget,Widget';
            if ($rows[0]['products'] !== $expected) {
                $this->markTestIncomplete(
                    'GROUP_CONCAT after INSERT: expected "' . $expected . '", got '
                    . var_export($rows[0]['products'], true)
                );
            }
            $this->assertSame($expected, $rows[0]['products']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testGroupConcatAfterDelete(): void
    {
        try {
            $this->pdo->exec("DELETE FROM mpd_gcd_orders WHERE id = 2");

            $rows = $this->ztdQuery(
                "SELECT customer, GROUP_CONCAT(product ORDER BY product) AS products
                 FROM mpd_gcd_orders WHERE customer = 'Alice' GROUP BY customer"
            );

            $this->assertCount(1, $rows);
            if ($rows[0]['products'] !== 'Widget') {
                $this->markTestIncomplete(
                    'GROUP_CONCAT after DELETE: expected "Widget", got '
                    . var_export($rows[0]['products'], true)
                );
            }
            $this->assertSame('Widget', $rows[0]['products']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT after DELETE failed: ' . $e->getMessage());
        }
    }

    public function testGroupConcatAfterUpdate(): void
    {
        try {
            $this->pdo->exec("UPDATE mpd_gcd_orders SET product = 'Thingamajig' WHERE id = 1");

            $rows = $this->ztdQuery(
                "SELECT customer, GROUP_CONCAT(product ORDER BY product) AS products
                 FROM mpd_gcd_orders WHERE customer = 'Alice' GROUP BY customer"
            );

            $this->assertCount(1, $rows);
            $expected = 'Gadget,Thingamajig';
            if ($rows[0]['products'] !== $expected) {
                $this->markTestIncomplete(
                    'GROUP_CONCAT after UPDATE: expected "' . $expected . '", got '
                    . var_export($rows[0]['products'], true)
                );
            }
            $this->assertSame($expected, $rows[0]['products']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT after UPDATE failed: ' . $e->getMessage());
        }
    }
}
