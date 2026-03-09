<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT...SELECT with a UNION source through CTE shadow store on SQLite.
 *
 * INSERT...SELECT is a common pattern for merging data from multiple tables.
 * When the SELECT source is a UNION query, the CTE rewriter must handle the
 * compound SELECT inside an INSERT context.
 */
class SqliteInsertSelectUnionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_isu_source_a (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL
            )',
            'CREATE TABLE sl_isu_source_b (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL
            )',
            'CREATE TABLE sl_isu_combined (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_isu_combined', 'sl_isu_source_a', 'sl_isu_source_b'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Source A: widgets
        $this->pdo->exec("INSERT INTO sl_isu_source_a VALUES (1, 'Widget Alpha', 'widget')");
        $this->pdo->exec("INSERT INTO sl_isu_source_a VALUES (2, 'Widget Beta', 'widget')");
        $this->pdo->exec("INSERT INTO sl_isu_source_a VALUES (3, 'Widget Gamma', 'widget')");

        // Source B: gadgets
        $this->pdo->exec("INSERT INTO sl_isu_source_b VALUES (1, 'Gadget One', 'gadget')");
        $this->pdo->exec("INSERT INTO sl_isu_source_b VALUES (2, 'Gadget Two', 'gadget')");
    }

    /**
     * INSERT...SELECT with UNION source should insert all rows from both sources.
     *
     * Uses UNION ALL since rows are distinct anyway and we want all rows.
     * Expected: 5 rows in the combined table.
     */
    public function testInsertSelectWithUnionAll(): void
    {
        $this->pdo->exec(
            "INSERT INTO sl_isu_combined (id, name, type)
             SELECT id, name, type FROM sl_isu_source_a
             UNION ALL
             SELECT id + 100, name, type FROM sl_isu_source_b"
        );

        $rows = $this->ztdQuery(
            "SELECT id, name, type FROM sl_isu_combined ORDER BY id"
        );

        $this->assertCount(5, $rows, 'INSERT...SELECT UNION ALL should insert 5 rows');

        // Verify source A rows
        $this->assertSame('Widget Alpha', $rows[0]['name']);
        $this->assertSame('widget', $rows[0]['type']);
        $this->assertSame('Widget Beta', $rows[1]['name']);
        $this->assertSame('Widget Gamma', $rows[2]['name']);

        // Verify source B rows
        $this->assertSame('Gadget One', $rows[3]['name']);
        $this->assertSame('gadget', $rows[3]['type']);
        $this->assertSame('Gadget Two', $rows[4]['name']);
    }

    /**
     * INSERT...SELECT with UNION (distinct) source.
     *
     * Uses UNION (not UNION ALL) with name and type only — no ID overlap concern.
     * Expected: 5 distinct rows inserted.
     */
    public function testInsertSelectWithUnionDistinct(): void
    {
        $this->pdo->exec(
            "INSERT INTO sl_isu_combined (name, type)
             SELECT name, type FROM sl_isu_source_a
             UNION
             SELECT name, type FROM sl_isu_source_b"
        );

        $rows = $this->ztdQuery(
            "SELECT name, type FROM sl_isu_combined ORDER BY name"
        );

        $this->assertCount(5, $rows, 'INSERT...SELECT UNION should insert 5 distinct rows');

        $names = array_column($rows, 'name');
        $this->assertSame([
            'Gadget One',
            'Gadget Two',
            'Widget Alpha',
            'Widget Beta',
            'Widget Gamma',
        ], $names);
    }

    /**
     * Verify inserted data has correct column values (no NULL columns).
     */
    public function testInsertedColumnsAreNotNull(): void
    {
        $this->pdo->exec(
            "INSERT INTO sl_isu_combined (id, name, type)
             SELECT id, name, type FROM sl_isu_source_a
             UNION ALL
             SELECT id + 100, name, type FROM sl_isu_source_b"
        );

        $rows = $this->ztdQuery(
            "SELECT id, name, type FROM sl_isu_combined ORDER BY id"
        );

        foreach ($rows as $i => $row) {
            $this->assertNotNull($row['id'], "Row {$i}: id should not be NULL");
            $this->assertNotNull($row['name'], "Row {$i}: name should not be NULL");
            $this->assertNotNull($row['type'], "Row {$i}: type should not be NULL");
            $this->assertNotSame('', $row['name'], "Row {$i}: name should not be empty");
            $this->assertNotSame('', $row['type'], "Row {$i}: type should not be empty");
        }
    }
}
