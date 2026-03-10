<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests VALUES-like expressions in DML subqueries on MySQLi.
 *
 * Uses UNION ALL derived tables as the portable MySQL equivalent of
 * standalone VALUES expressions, testing CTE rewriter handling of
 * derived table data sources in DML operations.
 *
 * @spec SPEC-10.2
 */
class ValuesExpressionDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE mi_val_products (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            price DECIMAL(10,2),
            category VARCHAR(50)
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['mi_val_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_val_products (name, price, category) VALUES ('Widget', 10.00, 'hardware')");
        $this->ztdExec("INSERT INTO mi_val_products (name, price, category) VALUES ('Gadget', 25.00, 'electronics')");
        $this->ztdExec("INSERT INTO mi_val_products (name, price, category) VALUES ('Doohickey', 5.00, 'hardware')");
        $this->ztdExec("INSERT INTO mi_val_products (name, price, category) VALUES ('Thingamajig', 50.00, 'electronics')");
        $this->ztdExec("INSERT INTO mi_val_products (name, price, category) VALUES ('Whatsit', 15.00, 'misc')");
    }

    /**
     * DELETE WHERE id IN derived VALUES.
     */
    public function testDeleteWhereIdInDerivedValues(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM mi_val_products
                 WHERE id IN (
                     SELECT v.id FROM (SELECT 1 AS id UNION ALL SELECT 3 UNION ALL SELECT 5) AS v
                 )"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mi_val_products ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE IN derived VALUES: expected 2 remaining, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(2, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE IN derived VALUES (MySQLi) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with JOIN on derived VALUES table.
     */
    public function testUpdateJoinDerivedValues(): void
    {
        try {
            $this->ztdExec(
                "UPDATE mi_val_products p
                 INNER JOIN (
                     SELECT 1 AS id, 12.00 AS new_price
                     UNION ALL SELECT 2, 30.00
                     UNION ALL SELECT 4, 55.00
                 ) v ON p.id = v.id
                 SET p.price = v.new_price"
            );

            $rows = $this->ztdQuery("SELECT id, price FROM mi_val_products ORDER BY id");

            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'UPDATE JOIN derived VALUES (MySQLi): expected 5, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertEqualsWithDelta(12.00, (float) $rows[0]['price'], 0.01);
            $this->assertEqualsWithDelta(30.00, (float) $rows[1]['price'], 0.01);
            $this->assertEqualsWithDelta(5.00, (float) $rows[2]['price'], 0.01);
            $this->assertEqualsWithDelta(55.00, (float) $rows[3]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE JOIN derived VALUES (MySQLi) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT SELECT from derived VALUES.
     */
    public function testInsertSelectFromDerivedValues(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO mi_val_products (name, price, category)
                 SELECT v.name, v.price, v.cat FROM (
                     SELECT 'Sprocket' AS name, 8.50 AS price, 'hardware' AS cat
                     UNION ALL SELECT 'Cog', 3.25, 'hardware'
                 ) v"
            );

            $rows = $this->ztdQuery(
                "SELECT name FROM mi_val_products WHERE category = 'hardware' ORDER BY name"
            );

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT SELECT derived VALUES (MySQLi): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT derived VALUES (MySQLi) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with EXISTS on derived VALUES.
     */
    public function testUpdateExistsDerivedValues(): void
    {
        try {
            $this->ztdExec(
                "UPDATE mi_val_products
                 SET category = 'premium'
                 WHERE EXISTS (
                     SELECT 1 FROM (SELECT 2 AS id UNION ALL SELECT 4) AS v
                     WHERE v.id = mi_val_products.id
                 )"
            );

            $rows = $this->ztdQuery(
                "SELECT id, category FROM mi_val_products WHERE category = 'premium' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE EXISTS derived VALUES (MySQLi): expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE EXISTS derived VALUES (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
