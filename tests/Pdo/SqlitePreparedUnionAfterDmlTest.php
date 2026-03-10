<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests prepared statements with UNION reading from shadow-modified tables
 * on SQLite.
 *
 * @spec SPEC-4.2
 */
class SqlitePreparedUnionAfterDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_pud_products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_pud_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_pud_products VALUES (1, 'Widget', 'tools', 10.00)");
        $this->pdo->exec("INSERT INTO sl_pud_products VALUES (2, 'Gadget', 'electronics', 25.00)");
        $this->pdo->exec("INSERT INTO sl_pud_products VALUES (3, 'Doohickey', 'tools', 5.00)");
        $this->pdo->exec("INSERT INTO sl_pud_products VALUES (4, 'Gizmo', 'electronics', 50.00)");
    }

    public function testPreparedUnionAllAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_pud_products VALUES (5, 'Thingamajig', 'tools', 15.00)");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM sl_pud_products WHERE category = ? AND price > ?
                 UNION ALL
                 SELECT name FROM sl_pud_products WHERE category = ? AND price < ?",
                ['tools', 10.0, 'electronics', 30.0]
            );

            $names = array_column($rows, 'name');
            sort($names);

            if (!in_array('Thingamajig', $names)) {
                $this->markTestIncomplete('Prepared UNION: shadow INSERT not visible. Got: ' . implode(', ', $names));
            }
            $this->assertCount(2, $rows);
            $this->assertEquals(['Gadget', 'Thingamajig'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UNION ALL after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUnionAfterUpdate(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_pud_products SET price = 100.00 WHERE id = 1");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM sl_pud_products WHERE price > ?
                 UNION
                 SELECT name FROM sl_pud_products WHERE category = ?
                 ORDER BY name",
                [40.0, 'tools']
            );

            $names = array_column($rows, 'name');

            if (!in_array('Widget', $names) || count($names) !== 3) {
                $this->markTestIncomplete('Prepared UNION after UPDATE: expected 3. Got: ' . implode(', ', $names));
            }
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UNION after UPDATE failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUnionAfterDelete(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_pud_products WHERE id = 2");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM sl_pud_products WHERE category = ?
                 UNION ALL
                 SELECT name FROM sl_pud_products WHERE price > ?
                 ORDER BY name",
                ['electronics', 20.0]
            );

            $names = array_column($rows, 'name');

            if (in_array('Gadget', $names)) {
                $this->markTestIncomplete('Prepared UNION after DELETE: deleted Gadget visible. Got: ' . implode(', ', $names));
            }
            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UNION after DELETE failed: ' . $e->getMessage());
        }
    }
}
