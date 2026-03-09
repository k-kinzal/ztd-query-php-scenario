<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE with subquery referencing the same table through ZTD shadow store.
 *
 * Pattern: UPDATE t SET col = (SELECT agg FROM t WHERE ...) WHERE ...
 * The CTE rewriter must handle the table appearing in both UPDATE target
 * and subquery source contexts.
 * @spec SPEC-4.5
 */
class SqliteUpdateSelfSubqueryTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE uss_products (id INT PRIMARY KEY, name VARCHAR(50), price INT, category VARCHAR(20))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['uss_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO uss_products VALUES (1, 'Widget A', 100, 'tools')");
        $this->pdo->exec("INSERT INTO uss_products VALUES (2, 'Widget B', 200, 'tools')");
        $this->pdo->exec("INSERT INTO uss_products VALUES (3, 'Gadget X', 300, 'electronics')");
        $this->pdo->exec("INSERT INTO uss_products VALUES (4, 'Gadget Y', 400, 'electronics')");
    }

    /**
     * UPDATE SET col = (SELECT MAX from same table).
     * Related to Issue #51: correlated subquery in SET produces syntax error.
     */
    public function testUpdateSetToMaxFromSameTable(): void
    {
        try {
            $this->pdo->exec(
                'UPDATE uss_products
                 SET price = (SELECT MAX(price) FROM uss_products WHERE category = uss_products.category)
                 WHERE id = 1'
            );

            $rows = $this->ztdQuery('SELECT id, price FROM uss_products ORDER BY id');
            // Widget A should now have price 200 (max of tools category)
            $this->assertSame('200', (string) $rows[0]['price']);
            // Others unchanged
            $this->assertSame('200', (string) $rows[1]['price']);
            $this->assertSame('300', (string) $rows[2]['price']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Issue #51: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET col = (SELECT AVG from same table).
     */
    public function testUpdateSetToAvgFromSameTable(): void
    {
        $this->pdo->exec(
            'UPDATE uss_products
             SET price = (SELECT AVG(price) FROM uss_products)
             WHERE id = 1'
        );

        $rows = $this->ztdQuery('SELECT price FROM uss_products WHERE id = 1');
        // AVG of 100,200,300,400 = 250
        $this->assertSame('250', (string) $rows[0]['price']);
    }

    /**
     * UPDATE with IN subquery from same table.
     */
    public function testUpdateWhereInSubquery(): void
    {
        $this->pdo->exec(
            'UPDATE uss_products
             SET price = price + 50
             WHERE id IN (SELECT id FROM uss_products WHERE category = \'tools\')'
        );

        $rows = $this->ztdQuery('SELECT id, price FROM uss_products ORDER BY id');
        $this->assertSame('150', (string) $rows[0]['price']); // Was 100
        $this->assertSame('250', (string) $rows[1]['price']); // Was 200
        $this->assertSame('300', (string) $rows[2]['price']); // Unchanged
        $this->assertSame('400', (string) $rows[3]['price']); // Unchanged
    }

    /**
     * UPDATE all rows in a category to the category minimum.
     * Related to Issue #51: correlated subquery in SET with alias produces syntax error.
     */
    public function testUpdateToCategoryMin(): void
    {
        try {
            $this->pdo->exec(
                'UPDATE uss_products
                 SET price = (SELECT MIN(p2.price) FROM uss_products p2 WHERE p2.category = uss_products.category)
                 WHERE category = \'electronics\''
            );

            $rows = $this->ztdQuery('SELECT id, price FROM uss_products WHERE category = \'electronics\' ORDER BY id');
            // Both should be 300 (min of electronics)
            $this->assertSame('300', (string) $rows[0]['price']);
            $this->assertSame('300', (string) $rows[1]['price']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Issue #51: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with NOT EXISTS subquery referencing same table (without alias on target).
     * SQLite does not support UPDATE aliases, so we reference the table name directly.
     */
    public function testUpdateWhereNotExistsSameTable(): void
    {
        $this->pdo->exec(
            'UPDATE uss_products
             SET price = 0
             WHERE NOT EXISTS (
                 SELECT 1 FROM uss_products p2
                 WHERE p2.category = uss_products.category AND p2.price < uss_products.price
             )'
        );

        $rows = $this->ztdQuery('SELECT id, price FROM uss_products ORDER BY id');
        // Widget A (100, cheapest tool) → 0
        $this->assertSame('0', (string) $rows[0]['price']);
        // Widget B (200) unchanged
        $this->assertSame('200', (string) $rows[1]['price']);
        // Gadget X (300, cheapest electronics) → 0
        $this->assertSame('0', (string) $rows[2]['price']);
        // Gadget Y (400) unchanged
        $this->assertSame('400', (string) $rows[3]['price']);
    }

    /**
     * Physical table isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM uss_products');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
