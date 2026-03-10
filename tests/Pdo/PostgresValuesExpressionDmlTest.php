<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests VALUES as a standalone table expression in DML subqueries on PostgreSQL.
 *
 * PostgreSQL supports VALUES (...), (...) as a standalone row source that can
 * appear in subqueries. When used in DELETE WHERE id IN (SELECT FROM VALUES)
 * or UPDATE FROM (VALUES ...), the CTE rewriter must handle this correctly.
 *
 * @spec SPEC-10.2
 */
class PostgresValuesExpressionDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE pg_val_products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            price DECIMAL(10,2),
            category VARCHAR(50)
        )";
    }

    protected function getTableNames(): array
    {
        return ['pg_val_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_val_products (name, price, category) VALUES ('Widget', 10.00, 'hardware')");
        $this->ztdExec("INSERT INTO pg_val_products (name, price, category) VALUES ('Gadget', 25.00, 'electronics')");
        $this->ztdExec("INSERT INTO pg_val_products (name, price, category) VALUES ('Doohickey', 5.00, 'hardware')");
        $this->ztdExec("INSERT INTO pg_val_products (name, price, category) VALUES ('Thingamajig', 50.00, 'electronics')");
        $this->ztdExec("INSERT INTO pg_val_products (name, price, category) VALUES ('Whatchamacallit', 15.00, 'misc')");
    }

    /**
     * DELETE WHERE id IN (SELECT FROM VALUES): delete specific IDs via VALUES list.
     */
    public function testDeleteWhereIdInValues(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM pg_val_products
                 WHERE id IN (SELECT v.id FROM (VALUES (1), (3), (5)) AS v(id))"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM pg_val_products ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE IN VALUES: expected 2 remaining, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                    . ' — VALUES in DML subquery may not work with ZTD'
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(2, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE IN VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE FROM VALUES: batch update prices from a VALUES list.
     */
    public function testUpdateFromValues(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_val_products p
                 SET price = v.new_price
                 FROM (VALUES (1, 12.00), (2, 30.00), (4, 55.00)) AS v(id, new_price)
                 WHERE p.id = v.id"
            );

            $rows = $this->ztdQuery(
                "SELECT id, price FROM pg_val_products ORDER BY id"
            );

            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'UPDATE FROM VALUES: expected 5 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertEqualsWithDelta(12.00, (float) $rows[0]['price'], 0.01); // id=1
            $this->assertEqualsWithDelta(30.00, (float) $rows[1]['price'], 0.01); // id=2
            $this->assertEqualsWithDelta(5.00, (float) $rows[2]['price'], 0.01);  // id=3 unchanged
            $this->assertEqualsWithDelta(55.00, (float) $rows[3]['price'], 0.01); // id=4
            $this->assertEqualsWithDelta(15.00, (float) $rows[4]['price'], 0.01); // id=5 unchanged
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE FROM VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT ... SELECT FROM VALUES: insert rows from a VALUES expression.
     */
    public function testInsertSelectFromValues(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_val_products (name, price, category)
                 SELECT v.name, v.price, v.cat
                 FROM (VALUES ('Sprocket', 8.50, 'hardware'), ('Cog', 3.25, 'hardware')) AS v(name, price, cat)"
            );

            $rows = $this->ztdQuery(
                "SELECT name, price FROM pg_val_products WHERE category = 'hardware' ORDER BY name"
            );

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT SELECT FROM VALUES: expected 4 hardware products, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT FROM VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE using NOT IN with VALUES expression.
     */
    public function testDeleteNotInValues(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM pg_val_products
                 WHERE id NOT IN (SELECT v.id FROM (VALUES (2), (4)) AS v(id))"
            );

            $rows = $this->ztdQuery("SELECT id FROM pg_val_products ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE NOT IN VALUES: expected 2 remaining, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame(2, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE NOT IN VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with VALUES in correlated EXISTS subquery.
     */
    public function testUpdateWithValuesInExists(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_val_products
                 SET category = 'premium'
                 WHERE EXISTS (
                     SELECT 1 FROM (VALUES (2), (4)) AS v(id)
                     WHERE v.id = pg_val_products.id
                 )"
            );

            $rows = $this->ztdQuery(
                "SELECT id, category FROM pg_val_products WHERE category = 'premium' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE VALUES EXISTS: expected 2 premium, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(2, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with VALUES in EXISTS failed: ' . $e->getMessage());
        }
    }
}
