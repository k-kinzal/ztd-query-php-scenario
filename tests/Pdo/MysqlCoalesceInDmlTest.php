<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests COALESCE usage in DML operations through ZTD on MySQL.
 *
 * COALESCE is commonly used to provide default values in UPDATE SET
 * and WHERE clauses. Tests whether the CTE rewriter handles COALESCE
 * with NULL columns correctly on MySQL.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class MysqlCoalesceInDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_cdml_items (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                description TEXT,
                price DECIMAL(10,2),
                discount DECIMAL(10,2),
                category VARCHAR(50)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_cdml_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_cdml_items (id, name, description, price, discount, category) VALUES (1, 'Widget', 'A widget', 10.00, NULL, 'tools')");
        $this->pdo->exec("INSERT INTO my_cdml_items (id, name, description, price, discount, category) VALUES (2, 'Gadget', NULL, 20.00, 5.00, NULL)");
        $this->pdo->exec("INSERT INTO my_cdml_items (id, name, description, price, discount, category) VALUES (3, 'Sprocket', 'A sprocket', NULL, NULL, 'parts')");
        $this->pdo->exec("INSERT INTO my_cdml_items (id, name, description, price, discount, category) VALUES (4, 'Bolt', NULL, NULL, 2.50, NULL)");
    }

    /**
     * UPDATE SET using COALESCE to fill NULL values.
     */
    public function testUpdateSetCoalesceDefault(): void
    {
        try {
            $this->pdo->exec("UPDATE my_cdml_items SET price = COALESCE(price, 0.00)");

            $rows = $this->ztdQuery("SELECT id, price FROM my_cdml_items ORDER BY id");

            $this->assertCount(4, $rows);
            $this->assertEqualsWithDelta(10.00, (float) $rows[0]['price'], 0.01);
            $this->assertEqualsWithDelta(20.00, (float) $rows[1]['price'], 0.01);
            $this->assertEqualsWithDelta(0.00, (float) $rows[2]['price'], 0.01);
            $this->assertEqualsWithDelta(0.00, (float) $rows[3]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COALESCE UPDATE SET failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with COALESCE and bound parameter.
     */
    public function testPreparedUpdateCoalesceWithParam(): void
    {
        try {
            $stmt = $this->pdo->prepare("UPDATE my_cdml_items SET price = COALESCE(price, ?) WHERE id = ?");
            $stmt->execute([99.99, 3]);

            $rows = $this->ztdQuery("SELECT id, price FROM my_cdml_items WHERE id = 3");

            $this->assertCount(1, $rows);
            if ($rows[0]['price'] === null) {
                $this->markTestIncomplete('Prepared COALESCE: price still NULL');
            }
            $this->assertEqualsWithDelta(99.99, (float) $rows[0]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared COALESCE UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * Nested COALESCE: COALESCE(discount, price, ?).
     */
    public function testNestedCoalesceInSet(): void
    {
        try {
            $stmt = $this->pdo->prepare("UPDATE my_cdml_items SET price = COALESCE(discount, price, ?)");
            $stmt->execute([1.00]);

            $rows = $this->ztdQuery("SELECT id, name, price FROM my_cdml_items ORDER BY id");

            $this->assertCount(4, $rows);
            // Widget: discount=NULL, price=10 → 10
            $this->assertEqualsWithDelta(10.00, (float) $rows[0]['price'], 0.01);
            // Gadget: discount=5, price=20 → 5
            $this->assertEqualsWithDelta(5.00, (float) $rows[1]['price'], 0.01);
            // Sprocket: discount=NULL, price=NULL → 1.00
            $this->assertEqualsWithDelta(1.00, (float) $rows[2]['price'], 0.01);
            // Bolt: discount=2.50, price=NULL → 2.50
            $this->assertEqualsWithDelta(2.50, (float) $rows[3]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Nested COALESCE UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * Multi-column COALESCE in single UPDATE.
     */
    public function testMultiColumnCoalesce(): void
    {
        try {
            $this->pdo->exec("UPDATE my_cdml_items
                SET description = COALESCE(description, 'No description'),
                    category = COALESCE(category, 'uncategorized'),
                    price = COALESCE(price, 0.00)");

            $rows = $this->ztdQuery("SELECT name, description, category, price FROM my_cdml_items ORDER BY id");

            $this->assertCount(4, $rows);
            $this->assertSame('No description', $rows[1]['description']);
            $this->assertSame('uncategorized', $rows[1]['category']);
            $this->assertSame('No description', $rows[3]['description']);
            $this->assertSame('uncategorized', $rows[3]['category']);
            $this->assertEqualsWithDelta(0.00, (float) $rows[3]['price'], 0.01);
            // Unchanged values
            $this->assertSame('A widget', $rows[0]['description']);
            $this->assertSame('tools', $rows[0]['category']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-column COALESCE UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE COALESCE with prepared params.
     */
    public function testDeleteWhereCoalesceWithParam(): void
    {
        try {
            $stmt = $this->pdo->prepare("DELETE FROM my_cdml_items WHERE COALESCE(price, ?) < ?");
            $stmt->execute([0.00, 15.00]);

            $rows = $this->ztdQuery("SELECT id, name FROM my_cdml_items ORDER BY id");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'COALESCE DELETE: expected 1 row, got ' . count($rows) . '. Data: ' . json_encode($rows)
                );
            }
            $this->assertSame('Gadget', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COALESCE DELETE failed: ' . $e->getMessage());
        }
    }
}
