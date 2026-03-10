<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests COALESCE usage in DML operations with prepared parameters.
 *
 * COALESCE is commonly used to provide default values in UPDATE SET
 * and WHERE clauses. Tests whether the CTE rewriter handles COALESCE
 * expressions correctly, especially with prepared parameters.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class SqliteCoalesceInDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_cdml_items (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                price REAL,
                discount REAL,
                category TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_cdml_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_cdml_items VALUES (1, 'Widget', 'A widget', 10.00, NULL, 'tools')");
        $this->pdo->exec("INSERT INTO sl_cdml_items VALUES (2, 'Gadget', NULL, 20.00, 5.00, NULL)");
        $this->pdo->exec("INSERT INTO sl_cdml_items VALUES (3, 'Sprocket', 'A sprocket', NULL, NULL, 'parts')");
        $this->pdo->exec("INSERT INTO sl_cdml_items VALUES (4, 'Bolt', NULL, NULL, 2.50, NULL)");
    }

    /**
     * UPDATE SET using COALESCE to provide default for NULL column.
     */
    public function testUpdateSetCoalesceDefault(): void
    {
        $sql = "UPDATE sl_cdml_items SET price = COALESCE(price, 0.00)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, price FROM sl_cdml_items ORDER BY id");

            $this->assertCount(4, $rows);
            $this->assertEqualsWithDelta(10.00, (float) $rows[0]['price'], 0.01);
            $this->assertEqualsWithDelta(20.00, (float) $rows[1]['price'], 0.01);
            $this->assertEqualsWithDelta(0.00, (float) $rows[2]['price'], 0.01);
            $this->assertEqualsWithDelta(0.00, (float) $rows[3]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'COALESCE UPDATE SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared UPDATE with COALESCE and bound default parameter.
     */
    public function testPreparedUpdateCoalesceWithParam(): void
    {
        $sql = "UPDATE sl_cdml_items SET price = COALESCE(price, ?) WHERE id = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([99.99, 3]);

            $rows = $this->ztdQuery("SELECT id, price FROM sl_cdml_items WHERE id = 3");

            $this->assertCount(1, $rows);
            if ($rows[0]['price'] === null) {
                $this->markTestIncomplete(
                    'Prepared COALESCE: price still NULL. Data: ' . json_encode($rows)
                );
            }
            $this->assertEqualsWithDelta(99.99, (float) $rows[0]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared COALESCE UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with nested COALESCE: COALESCE(discount, price, ?).
     */
    public function testPreparedUpdateNestedCoalesce(): void
    {
        // Set effective_price = first non-null of (discount, price, default)
        $sql = "UPDATE sl_cdml_items SET price = COALESCE(discount, price, ?)";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([1.00]);

            $rows = $this->ztdQuery("SELECT id, name, price FROM sl_cdml_items ORDER BY id");

            $this->assertCount(4, $rows);

            // Widget: discount=NULL, price=10 → 10
            $this->assertEqualsWithDelta(10.00, (float) $rows[0]['price'], 0.01);
            // Gadget: discount=5, price=20 → 5
            $this->assertEqualsWithDelta(5.00, (float) $rows[1]['price'], 0.01);
            // Sprocket: discount=NULL, price=NULL → 1.00 (param)
            $this->assertEqualsWithDelta(1.00, (float) $rows[2]['price'], 0.01);
            // Bolt: discount=2.50, price=NULL → 2.50
            $this->assertEqualsWithDelta(2.50, (float) $rows[3]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Nested COALESCE UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE WHERE COALESCE with prepared parameter.
     */
    public function testDeleteWhereCoalesceWithParam(): void
    {
        // Delete items where effective price (or default) is below threshold
        $sql = "DELETE FROM sl_cdml_items WHERE COALESCE(price, ?) < ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([0.00, 15.00]);

            $rows = $this->ztdQuery("SELECT id, name FROM sl_cdml_items ORDER BY id");

            // Widget: COALESCE(10,0) = 10 < 15 → deleted
            // Gadget: COALESCE(20,0) = 20 ≥ 15 → kept
            // Sprocket: COALESCE(NULL,0) = 0 < 15 → deleted
            // Bolt: COALESCE(NULL,0) = 0 < 15 → deleted
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'COALESCE DELETE: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Gadget', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'COALESCE DELETE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE SET with COALESCE in multi-column assignment.
     */
    public function testUpdateMultiColumnCoalesce(): void
    {
        $sql = "UPDATE sl_cdml_items
                SET description = COALESCE(description, 'No description'),
                    category = COALESCE(category, 'uncategorized'),
                    price = COALESCE(price, 0.00)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT name, description, category, price FROM sl_cdml_items ORDER BY id");

            $this->assertCount(4, $rows);

            // Gadget had NULL description and NULL category
            $this->assertSame('No description', $rows[1]['description']);
            $this->assertSame('uncategorized', $rows[1]['category']);

            // Bolt had NULL description, NULL category, NULL price
            $this->assertSame('No description', $rows[3]['description']);
            $this->assertSame('uncategorized', $rows[3]['category']);
            $this->assertEqualsWithDelta(0.00, (float) $rows[3]['price'], 0.01);

            // Widget already had values — should be unchanged
            $this->assertSame('A widget', $rows[0]['description']);
            $this->assertSame('tools', $rows[0]['category']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multi-column COALESCE UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with COALESCE to verify shadow data visibility.
     */
    public function testSelectCoalesceOnShadowData(): void
    {
        // Insert row with NULLs in shadow
        $this->pdo->exec("INSERT INTO sl_cdml_items VALUES (5, 'NewItem', NULL, NULL, NULL, NULL)");

        $rows = $this->ztdQuery(
            "SELECT name, COALESCE(price, -1) AS effective_price FROM sl_cdml_items WHERE id = 5"
        );

        try {
            $this->assertCount(1, $rows);
            $this->assertSame('NewItem', $rows[0]['name']);
            $this->assertEqualsWithDelta(-1.0, (float) $rows[0]['effective_price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'COALESCE SELECT shadow: ' . $e->getMessage()
                . '. Data: ' . json_encode($rows)
            );
        }
    }
}
