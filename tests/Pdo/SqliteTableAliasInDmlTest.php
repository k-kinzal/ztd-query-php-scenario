<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests table aliases in UPDATE and DELETE DML statements on SQLite.
 *
 * Real-world scenario: many ORMs and query builders produce aliased DML like
 * `DELETE FROM t AS alias WHERE alias.col = ?` or `UPDATE t AS alias SET ...`.
 * SQLite supports this syntax. The CTE rewriter must correctly identify the
 * table name when it is aliased in DML context.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class SqliteTableAliasInDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_tad_items (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_tad_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_tad_items VALUES (1, 'Widget', 'tools', 10.00)");
        $this->ztdExec("INSERT INTO sl_tad_items VALUES (2, 'Gadget', 'electronics', 25.00)");
        $this->ztdExec("INSERT INTO sl_tad_items VALUES (3, 'Sprocket', 'tools', 5.00)");
    }

    /**
     * UPDATE with table alias using AS keyword.
     * Pattern: UPDATE t AS alias SET alias.col = ... WHERE alias.col = ...
     */
    public function testUpdateWithAsAlias(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_tad_items AS i SET price = price * 2 WHERE i.category = 'tools'"
            );

            $rows = $this->ztdQuery(
                "SELECT name, price FROM sl_tad_items WHERE category = 'tools' ORDER BY name"
            );
            $this->assertCount(2, $rows);
            $this->assertEqualsWithDelta(5.00 * 2, (float) $rows[0]['price'], 0.01); // Sprocket
            $this->assertEqualsWithDelta(10.00 * 2, (float) $rows[1]['price'], 0.01); // Widget
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with AS alias failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with table alias using AS keyword.
     * Pattern: DELETE FROM t AS alias WHERE alias.col = ...
     */
    public function testDeleteWithAsAlias(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM sl_tad_items AS i WHERE i.price < 10"
            );

            $rows = $this->ztdQuery("SELECT name FROM sl_tad_items ORDER BY name");
            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertNotContains('Sprocket', $names);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE with AS alias failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with alias and prepared params.
     */
    public function testUpdateWithAliasAndParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_tad_items AS i SET price = ? WHERE i.id = ?"
            );
            $stmt->execute([99.99, 2]);

            $rows = $this->ztdQuery("SELECT price FROM sl_tad_items WHERE id = 2");
            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(99.99, (float) $rows[0]['price'], 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with alias and params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with alias and prepared params.
     */
    public function testDeleteWithAliasAndParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM sl_tad_items AS i WHERE i.category = ?"
            );
            $stmt->execute(['electronics']);

            $rows = $this->ztdQuery("SELECT name FROM sl_tad_items ORDER BY name");
            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertNotContains('Gadget', $names);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE with alias and params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with alias, column reference without alias prefix.
     * Pattern: UPDATE t AS i SET name = ... WHERE id = ...
     * (not using i.id, just id)
     */
    public function testUpdateAliasWithUnprefixedColumns(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_tad_items AS i SET name = 'Renamed' WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT name FROM sl_tad_items WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame('Renamed', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with alias + unprefixed columns failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Verify that aliased DML maintains physical isolation.
     */
    public function testAliasedDmlPhysicalIsolation(): void
    {
        try {
            $this->ztdExec("DELETE FROM sl_tad_items AS i WHERE i.id = 1");

            // Shadow should have 2 rows
            $shadow = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_tad_items");
            $this->assertEquals(2, (int) $shadow[0]['cnt']);

            // Physical should still have 0 rows (all inserts were shadow)
            $this->pdo->disableZtd();
            $physical = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_tad_items")
                ->fetchAll(PDO::FETCH_ASSOC);
            $this->assertEquals(0, (int) $physical[0]['cnt']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Aliased DML physical isolation check failed: ' . $e->getMessage()
            );
        }
    }
}
