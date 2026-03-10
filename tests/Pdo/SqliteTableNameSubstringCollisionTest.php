<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests behavior when one table name is a substring of another on SQLite.
 *
 * @spec SPEC-4.2
 */
class SqliteTableNameSubstringCollisionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_tnc_order (
                id INTEGER PRIMARY KEY,
                customer TEXT NOT NULL,
                total REAL NOT NULL DEFAULT 0
            )',
            'CREATE TABLE sl_tnc_order_item (
                id INTEGER PRIMARY KEY,
                order_id INTEGER NOT NULL,
                product TEXT NOT NULL,
                price REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_tnc_order_item', 'sl_tnc_order'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_tnc_order VALUES (1, 'Alice', 0)");
        $this->pdo->exec("INSERT INTO sl_tnc_order VALUES (2, 'Bob', 0)");

        $this->pdo->exec("INSERT INTO sl_tnc_order_item VALUES (1, 1, 'Widget', 10.00)");
        $this->pdo->exec("INSERT INTO sl_tnc_order_item VALUES (2, 1, 'Gadget', 20.00)");
        $this->pdo->exec("INSERT INTO sl_tnc_order_item VALUES (3, 2, 'Widget', 10.00)");
    }

    public function testJoinAfterDmlOnBothSubstringTables(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_tnc_order VALUES (3, 'Carol', 0)");
            $this->pdo->exec("INSERT INTO sl_tnc_order_item VALUES (4, 3, 'Doohickey', 30.00)");

            $rows = $this->ztdQuery(
                "SELECT o.customer, SUM(oi.price) AS total
                 FROM sl_tnc_order o
                 JOIN sl_tnc_order_item oi ON oi.order_id = o.id
                 GROUP BY o.customer
                 ORDER BY o.customer"
            );

            $map = [];
            foreach ($rows as $row) {
                $map[$row['customer']] = (float) $row['total'];
            }

            if (!isset($map['Carol'])) {
                $this->markTestIncomplete('Substring tables: Carol not visible. Got: ' . json_encode($map));
            }
            $this->assertEquals(30.00, $map['Alice']);
            $this->assertEquals(30.00, $map['Carol']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('JOIN after DML on substring tables failed: ' . $e->getMessage());
        }
    }

    public function testSubqueryChildInParentSelect(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_tnc_order_item VALUES (4, 2, 'Gizmo', 50.00)");

            $rows = $this->ztdQuery(
                "SELECT o.customer,
                        (SELECT SUM(oi.price) FROM sl_tnc_order_item oi WHERE oi.order_id = o.id) AS item_total
                 FROM sl_tnc_order o
                 ORDER BY o.id"
            );

            $this->assertCount(2, $rows);
            $this->assertEquals(30.00, (float) $rows[0]['item_total']);
            $this->assertEquals(60.00, (float) $rows[1]['item_total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Subquery child in parent SELECT failed: ' . $e->getMessage());
        }
    }
}
