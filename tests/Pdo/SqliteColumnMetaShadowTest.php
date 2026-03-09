<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests getColumnMeta() on shadow-rewritten queries.
 *
 * ZtdPdoStatement delegates getColumnMeta() to the inner statement, which
 * was prepared with CTE-rewritten SQL. Column metadata may reflect the CTE
 * temporary names rather than original table/column information.
 *
 * @spec SPEC-3.3
 */
class SqliteColumnMetaShadowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE meta_t (id INTEGER PRIMARY KEY, name TEXT, price REAL, active INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['meta_t'];
    }

    /**
     * Column names preserved in getColumnMeta after shadow INSERT.
     */
    public function testColumnNamesPreservedAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO meta_t (id, name, price, active) VALUES (1, 'Widget', 29.99, 1)");

        $stmt = $this->pdo->query('SELECT id, name, price FROM meta_t WHERE id = 1');
        $meta0 = $stmt->getColumnMeta(0);
        $meta1 = $stmt->getColumnMeta(1);
        $meta2 = $stmt->getColumnMeta(2);

        $this->assertSame('id', $meta0['name']);
        $this->assertSame('name', $meta1['name']);
        $this->assertSame('price', $meta2['name']);
    }

    /**
     * columnCount() correct after shadow INSERT.
     */
    public function testColumnCountAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO meta_t (id, name, price, active) VALUES (1, 'Widget', 29.99, 1)");

        $stmt = $this->pdo->query('SELECT * FROM meta_t WHERE id = 1');
        $this->assertEquals(4, $stmt->columnCount());
    }

    /**
     * Column names with aliased SELECT.
     */
    public function testColumnNamesWithAliases(): void
    {
        $this->pdo->exec("INSERT INTO meta_t (id, name, price, active) VALUES (1, 'Widget', 29.99, 1)");

        $stmt = $this->pdo->query('SELECT name AS product_name, price AS cost FROM meta_t WHERE id = 1');
        $meta0 = $stmt->getColumnMeta(0);
        $meta1 = $stmt->getColumnMeta(1);

        $this->assertSame('product_name', $meta0['name']);
        $this->assertSame('cost', $meta1['name']);
    }

    /**
     * Column meta after UPDATE (shadow store has been mutated).
     */
    public function testColumnMetaAfterUpdate(): void
    {
        $this->pdo->exec("INSERT INTO meta_t (id, name, price, active) VALUES (1, 'Widget', 29.99, 1)");
        $this->pdo->exec("UPDATE meta_t SET price = 39.99, name = 'Updated Widget' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT id, name, price FROM meta_t WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Updated Widget', $row['name']);

        // Meta should still report correct column names
        $stmt2 = $this->pdo->query('SELECT id, name, price FROM meta_t WHERE id = 1');
        $meta1 = $stmt2->getColumnMeta(1);
        $this->assertSame('name', $meta1['name']);
    }

    /**
     * Column meta table field reflects original table (not CTE name).
     */
    public function testColumnMetaTableField(): void
    {
        $this->pdo->exec("INSERT INTO meta_t (id, name, price, active) VALUES (1, 'Widget', 29.99, 1)");

        $stmt = $this->pdo->query('SELECT id, name FROM meta_t WHERE id = 1');
        $meta = $stmt->getColumnMeta(0);

        // SQLite may or may not populate 'table', but if it does, it should
        // be 'meta_t', not an internal CTE name like 'ztd_shadow_...'
        if (isset($meta['table']) && $meta['table'] !== '') {
            $this->assertStringNotContainsString('ztd', strtolower($meta['table']),
                'Column meta table should not expose internal CTE name');
            $this->assertSame('meta_t', $meta['table']);
        } else {
            // SQLite often returns empty table for CTE-backed queries — document this
            $this->addToAssertionCount(1);
        }
    }

    /**
     * Column meta native_type preserved for INTEGER column.
     */
    public function testColumnMetaNativeType(): void
    {
        $this->pdo->exec("INSERT INTO meta_t (id, name, price, active) VALUES (1, 'Widget', 29.99, 1)");

        $stmt = $this->pdo->query('SELECT id, name, price, active FROM meta_t WHERE id = 1');
        $metaId = $stmt->getColumnMeta(0);
        $metaName = $stmt->getColumnMeta(1);

        // Check that native_type is present and reasonable
        $this->assertArrayHasKey('native_type', $metaId);
        $this->assertArrayHasKey('native_type', $metaName);
    }

    /**
     * Column meta on computed expression column.
     */
    public function testColumnMetaOnComputedColumn(): void
    {
        $this->pdo->exec("INSERT INTO meta_t (id, name, price, active) VALUES (1, 'Widget', 29.99, 1)");

        $stmt = $this->pdo->query('SELECT price * 1.1 AS with_tax, UPPER(name) AS upper_name FROM meta_t WHERE id = 1');
        $meta0 = $stmt->getColumnMeta(0);
        $meta1 = $stmt->getColumnMeta(1);

        $this->assertSame('with_tax', $meta0['name']);
        $this->assertSame('upper_name', $meta1['name']);
    }

    /**
     * Column meta on query with no shadow data (clean table).
     */
    public function testColumnMetaNoShadowData(): void
    {
        // No INSERT — query against empty table
        $stmt = $this->pdo->query('SELECT id, name, price FROM meta_t');
        $this->assertEquals(3, $stmt->columnCount());
        $meta0 = $stmt->getColumnMeta(0);
        $this->assertSame('id', $meta0['name']);
    }
}
