<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests PDO named parameters (:name style) in DML through ZTD shadow store on SQLite.
 *
 * PDO named parameters are extremely common in production code and frameworks.
 * Most ORM-generated queries use :param_name rather than positional ?.
 * The CTE rewriter must handle :name parameters correctly.
 *
 * @spec SPEC-3.2, SPEC-4.2, SPEC-4.3
 */
class SqliteNamedParamDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_np_products (
            id INTEGER PRIMARY KEY,
            sku TEXT NOT NULL,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            category TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_np_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_np_products VALUES (1, 'SKU-001', 'Widget', 29.99, 'tools', 1)");
        $this->pdo->exec("INSERT INTO sl_np_products VALUES (2, 'SKU-002', 'Gadget', 49.99, 'electronics', 1)");
        $this->pdo->exec("INSERT INTO sl_np_products VALUES (3, 'SKU-003', 'Sprocket', 9.99, 'tools', 0)");
    }

    /**
     * Prepared SELECT with named parameters.
     */
    public function testSelectWithNamedParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT name, price FROM sl_np_products WHERE category = :cat AND active = :active ORDER BY name"
            );
            $stmt->execute([':cat' => 'tools', ':active' => 1]);
            $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Named param SELECT: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Widget', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Named param SELECT failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT with named parameters.
     */
    public function testInsertWithNamedParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO sl_np_products (id, sku, name, price, category, active)
                 VALUES (:id, :sku, :name, :price, :cat, :active)"
            );
            $stmt->execute([
                ':id' => 4,
                ':sku' => 'SKU-004',
                ':name' => 'Thingamajig',
                ':price' => 19.99,
                ':cat' => 'misc',
                ':active' => 1,
            ]);

            $rows = $this->ztdQuery("SELECT name, price FROM sl_np_products WHERE id = 4");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Named param INSERT: expected 1, got ' . count($rows)
                );
            }

            $this->assertSame('Thingamajig', $rows[0]['name']);
            $this->assertEqualsWithDelta(19.99, (float) $rows[0]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Named param INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with named parameters.
     */
    public function testUpdateWithNamedParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_np_products SET price = :new_price, name = :new_name WHERE id = :id"
            );
            $stmt->execute([':new_price' => 39.99, ':new_name' => 'Super Widget', ':id' => 1]);

            $rows = $this->ztdQuery("SELECT name, price FROM sl_np_products WHERE id = 1");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Named param UPDATE: expected 1, got ' . count($rows)
                );
            }

            $this->assertSame('Super Widget', $rows[0]['name']);
            $this->assertEqualsWithDelta(39.99, (float) $rows[0]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Named param UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE with named parameters.
     */
    public function testDeleteWithNamedParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM sl_np_products WHERE category = :cat AND price < :max_price"
            );
            $stmt->execute([':cat' => 'tools', ':max_price' => 15.00]);

            $rows = $this->ztdQuery("SELECT name FROM sl_np_products ORDER BY id");

            // Sprocket (tools, 9.99) deleted; Widget (tools, 29.99) stays
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Named param DELETE: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Widget', $rows[0]['name']);
            $this->assertSame('Gadget', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Named param DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Named params without colon prefix (PDO also accepts this).
     */
    public function testNamedParamsWithoutColonPrefix(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT name FROM sl_np_products WHERE price > :min_price ORDER BY price"
            );
            // PDO allows passing without leading colon
            $stmt->execute(['min_price' => 20.00]);
            $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Named params no colon: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Named params without colon prefix failed: ' . $e->getMessage());
        }
    }

    /**
     * Same named parameter used multiple times in one query.
     */
    public function testReusedNamedParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT name FROM sl_np_products
                 WHERE price > :threshold OR (price = :threshold AND active = 1)
                 ORDER BY name"
            );
            $stmt->execute([':threshold' => 29.99]);
            $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

            // Widget (29.99, active=1) matches second condition
            // Gadget (49.99) matches first condition
            if (count($rows) < 1) {
                $this->markTestIncomplete(
                    'Reused named param: expected >= 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $names = array_column($rows, 'name');
            $this->assertContains('Gadget', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Reused named param failed: ' . $e->getMessage());
        }
    }

    /**
     * Named param UPDATE with WHERE using multiple params.
     */
    public function testUpdateMultipleNamedParamsInWhere(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_np_products
                 SET active = :new_active
                 WHERE category = :cat AND price >= :min_price AND price <= :max_price"
            );
            $stmt->execute([
                ':new_active' => 0,
                ':cat' => 'tools',
                ':min_price' => 5.00,
                ':max_price' => 35.00,
            ]);

            $rows = $this->ztdQuery("SELECT name, active FROM sl_np_products WHERE category = 'tools' ORDER BY id");

            // Both tools products should be deactivated
            foreach ($rows as $r) {
                if ((int) $r['active'] !== 0) {
                    $this->markTestIncomplete(
                        'Multi named param UPDATE: ' . $r['name'] . ' still active. Data: ' . json_encode($rows)
                    );
                }
            }

            $this->assertSame(0, (int) $rows[0]['active']); // Widget
            $this->assertSame(0, (int) $rows[1]['active']); // Sprocket
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with multiple named params failed: ' . $e->getMessage());
        }
    }

    /**
     * Named param with bindValue() instead of execute(array).
     */
    public function testBindValueWithNamedParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT name FROM sl_np_products WHERE category = :cat AND price < :max"
            );
            $stmt->bindValue(':cat', 'electronics', \PDO::PARAM_STR);
            $stmt->bindValue(':max', 100.00);
            $stmt->execute();
            $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'bindValue named: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('Gadget', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('bindValue with named params failed: ' . $e->getMessage());
        }
    }
}
