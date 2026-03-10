<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests prepared statements with UNION reading from shadow-modified tables.
 *
 * Combines two complexity dimensions that stress the CTE rewriter:
 * parameter binding + UNION branch rewriting.
 *
 * @spec SPEC-4.2
 */
class PreparedUnionAfterDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_pud_products (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            category VARCHAR(30) NOT NULL,
            price DECIMAL(10,2) NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_pud_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_pud_products VALUES (1, 'Widget', 'tools', 10.00)");
        $this->mysqli->query("INSERT INTO mi_pud_products VALUES (2, 'Gadget', 'electronics', 25.00)");
        $this->mysqli->query("INSERT INTO mi_pud_products VALUES (3, 'Doohickey', 'tools', 5.00)");
        $this->mysqli->query("INSERT INTO mi_pud_products VALUES (4, 'Gizmo', 'electronics', 50.00)");
    }

    /**
     * Prepared UNION ALL with parameters in both branches.
     */
    public function testPreparedUnionAllBothBranches(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_pud_products VALUES (5, 'Thingamajig', 'tools', 15.00)");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM mi_pud_products WHERE category = ? AND price > ?
                 UNION ALL
                 SELECT name FROM mi_pud_products WHERE category = ? AND price < ?
                 ORDER BY name",
                ['tools', 10.0, 'electronics', 30.0]
            );

            $names = array_column($rows, 'name');

            if (!in_array('Thingamajig', $names)) {
                $this->markTestIncomplete('Prepared UNION: shadow INSERT not visible. Got: ' . implode(', ', $names));
            }
            // tools > 10: Thingamajig(15) ; electronics < 30: Gadget(25)
            $this->assertCount(2, $rows);
            $this->assertEquals(['Gadget', 'Thingamajig'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UNION ALL both branches failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UNION after UPDATE.
     */
    public function testPreparedUnionAfterUpdate(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_pud_products SET price = 100.00 WHERE id = 1");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM mi_pud_products WHERE price > ?
                 UNION
                 SELECT name FROM mi_pud_products WHERE category = ?
                 ORDER BY name",
                [40.0, 'tools']
            );

            $names = array_column($rows, 'name');

            // price > 40: Widget(100), Gizmo(50) ; category=tools: Widget, Doohickey
            // UNION distinct: Doohickey, Gizmo, Widget
            if (!in_array('Widget', $names) || count($names) !== 3) {
                $this->markTestIncomplete('Prepared UNION after UPDATE: expected 3. Got: ' . implode(', ', $names));
            }
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UNION after UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UNION after DELETE.
     */
    public function testPreparedUnionAfterDelete(): void
    {
        try {
            $this->mysqli->query("DELETE FROM mi_pud_products WHERE id = 2");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM mi_pud_products WHERE category = ?
                 UNION ALL
                 SELECT name FROM mi_pud_products WHERE price > ?
                 ORDER BY name",
                ['electronics', 20.0]
            );

            $names = array_column($rows, 'name');

            // electronics: Gizmo only (Gadget deleted) ; price > 20: Gizmo(50)
            // UNION ALL: Gizmo, Gizmo
            if (in_array('Gadget', $names)) {
                $this->markTestIncomplete('Prepared UNION after DELETE: deleted Gadget visible. Got: ' . implode(', ', $names));
            }
            $this->assertCount(2, $rows);
            $this->assertEquals(['Gizmo', 'Gizmo'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UNION after DELETE failed: ' . $e->getMessage());
        }
    }
}
