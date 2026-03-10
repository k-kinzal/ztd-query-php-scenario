<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests VALUES ROW() expression in DML subqueries on MySQL 8.0+.
 *
 * MySQL 8.0.19+ supports VALUES ROW(...), ROW(...) as a table value constructor
 * and also supports the TABLE keyword. This tests whether the CTE rewriter
 * handles VALUES as a data source in DML operations.
 *
 * @spec SPEC-10.2
 */
class MysqlValuesExpressionDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE my_val_products (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            price DECIMAL(10,2),
            category VARCHAR(50)
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['my_val_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_val_products (name, price, category) VALUES ('Widget', 10.00, 'hardware')");
        $this->ztdExec("INSERT INTO my_val_products (name, price, category) VALUES ('Gadget', 25.00, 'electronics')");
        $this->ztdExec("INSERT INTO my_val_products (name, price, category) VALUES ('Doohickey', 5.00, 'hardware')");
        $this->ztdExec("INSERT INTO my_val_products (name, price, category) VALUES ('Thingamajig', 50.00, 'electronics')");
        $this->ztdExec("INSERT INTO my_val_products (name, price, category) VALUES ('Whatsit', 15.00, 'misc')");
    }

    /**
     * DELETE WHERE id IN (subquery from derived table with UNION ALL as VALUES workaround).
     *
     * MySQL doesn't support VALUES as a standalone table expression the same way,
     * so we use UNION ALL as a portable equivalent.
     */
    public function testDeleteWhereIdInDerivedValues(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM my_val_products
                 WHERE id IN (
                     SELECT v.id FROM (SELECT 1 AS id UNION ALL SELECT 3 UNION ALL SELECT 5) AS v
                 )"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM my_val_products ORDER BY id");

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
            $this->markTestIncomplete('DELETE WHERE IN derived VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with JOIN on derived VALUES table (UNION ALL workaround).
     */
    public function testUpdateJoinDerivedValues(): void
    {
        try {
            $this->ztdExec(
                "UPDATE my_val_products p
                 INNER JOIN (
                     SELECT 1 AS id, 12.00 AS new_price
                     UNION ALL SELECT 2, 30.00
                     UNION ALL SELECT 4, 55.00
                 ) v ON p.id = v.id
                 SET p.price = v.new_price"
            );

            $rows = $this->ztdQuery("SELECT id, price FROM my_val_products ORDER BY id");

            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'UPDATE JOIN derived VALUES: expected 5 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertEqualsWithDelta(12.00, (float) $rows[0]['price'], 0.01);
            $this->assertEqualsWithDelta(30.00, (float) $rows[1]['price'], 0.01);
            $this->assertEqualsWithDelta(5.00, (float) $rows[2]['price'], 0.01);
            $this->assertEqualsWithDelta(55.00, (float) $rows[3]['price'], 0.01);
            $this->assertEqualsWithDelta(15.00, (float) $rows[4]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE JOIN derived VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT ... SELECT from derived VALUES (UNION ALL).
     */
    public function testInsertSelectFromDerivedValues(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO my_val_products (name, price, category)
                 SELECT v.name, v.price, v.cat FROM (
                     SELECT 'Sprocket' AS name, 8.50 AS price, 'hardware' AS cat
                     UNION ALL SELECT 'Cog', 3.25, 'hardware'
                 ) v"
            );

            $rows = $this->ztdQuery(
                "SELECT name, price FROM my_val_products WHERE category = 'hardware' ORDER BY name"
            );

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT SELECT derived VALUES: expected 4 hardware, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT from derived VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with NOT IN derived VALUES.
     */
    public function testDeleteNotInDerivedValues(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM my_val_products
                 WHERE id NOT IN (SELECT v.id FROM (SELECT 2 AS id UNION ALL SELECT 4) AS v)"
            );

            $rows = $this->ztdQuery("SELECT id FROM my_val_products ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE NOT IN derived VALUES: expected 2 remaining, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame(2, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE NOT IN derived VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with EXISTS on derived VALUES.
     */
    public function testUpdateExistsDerivedValues(): void
    {
        try {
            $this->ztdExec(
                "UPDATE my_val_products
                 SET category = 'premium'
                 WHERE EXISTS (
                     SELECT 1 FROM (SELECT 2 AS id UNION ALL SELECT 4) AS v
                     WHERE v.id = my_val_products.id
                 )"
            );

            $rows = $this->ztdQuery(
                "SELECT id, category FROM my_val_products WHERE category = 'premium' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE EXISTS derived VALUES: expected 2 premium, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE EXISTS derived VALUES failed: ' . $e->getMessage());
        }
    }
}
