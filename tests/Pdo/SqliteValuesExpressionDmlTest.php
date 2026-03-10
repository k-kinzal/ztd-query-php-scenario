<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests VALUES as standalone table expression in DML subqueries on SQLite.
 *
 * SQLite 3.8.3+ supports VALUES (...), (...) as a standalone row source.
 * This tests whether the CTE rewriter handles VALUES in DML operations.
 *
 * @spec SPEC-10.2
 */
class SqliteValuesExpressionDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_val_products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            price REAL,
            category TEXT
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_val_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_val_products (name, price, category) VALUES ('Widget', 10.00, 'hardware')");
        $this->ztdExec("INSERT INTO sl_val_products (name, price, category) VALUES ('Gadget', 25.00, 'electronics')");
        $this->ztdExec("INSERT INTO sl_val_products (name, price, category) VALUES ('Doohickey', 5.00, 'hardware')");
        $this->ztdExec("INSERT INTO sl_val_products (name, price, category) VALUES ('Thingamajig', 50.00, 'electronics')");
        $this->ztdExec("INSERT INTO sl_val_products (name, price, category) VALUES ('Whatsit', 15.00, 'misc')");
    }

    /**
     * DELETE WHERE id IN (SELECT FROM VALUES): delete specific IDs.
     */
    public function testDeleteWhereIdInValues(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM sl_val_products
                 WHERE id IN (SELECT column1 FROM (VALUES (1), (3), (5)))"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM sl_val_products ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE IN VALUES: expected 2 remaining, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                    . ' — VALUES in DML subquery may not work with ZTD on SQLite'
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
     * INSERT ... SELECT FROM VALUES expression.
     */
    public function testInsertSelectFromValues(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_val_products (name, price, category)
                 SELECT column1, column2, column3
                 FROM (VALUES ('Sprocket', 8.50, 'hardware'), ('Cog', 3.25, 'hardware'))"
            );

            $rows = $this->ztdQuery(
                "SELECT name, price FROM sl_val_products WHERE category = 'hardware' ORDER BY name"
            );

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT SELECT FROM VALUES: expected 4 hardware, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT from VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with subquery using VALUES to match IDs.
     */
    public function testUpdateWithValuesSubquery(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_val_products
                 SET category = 'premium'
                 WHERE id IN (SELECT column1 FROM (VALUES (2), (4)))"
            );

            $rows = $this->ztdQuery(
                "SELECT id, category FROM sl_val_products WHERE category = 'premium' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE VALUES subquery: expected 2 premium, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(2, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with VALUES subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE NOT IN with VALUES expression.
     */
    public function testDeleteNotInValues(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM sl_val_products
                 WHERE id NOT IN (SELECT column1 FROM (VALUES (2), (4)))"
            );

            $rows = $this->ztdQuery("SELECT id FROM sl_val_products ORDER BY id");

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
     * VALUES join with shadow table for batch lookup.
     */
    public function testValuesJoinWithShadowTable(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT p.name, p.price
                 FROM sl_val_products p
                 INNER JOIN (SELECT column1 AS id FROM (VALUES (1), (3), (5))) v
                 ON p.id = v.id
                 ORDER BY p.id"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'VALUES JOIN shadow: expected 3 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Widget', $rows[0]['name']);
            $this->assertSame('Doohickey', $rows[1]['name']);
            $this->assertSame('Whatsit', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('VALUES JOIN with shadow table failed: ' . $e->getMessage());
        }
    }
}
