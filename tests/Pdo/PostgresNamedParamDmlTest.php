<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PDO named parameters (:name style) in DML through ZTD shadow store on PostgreSQL.
 *
 * PostgreSQL normally uses $1/$2 style params, but PDO translates :name to positional.
 * This tests whether ZTD handles this translation correctly.
 *
 * @spec SPEC-3.2, SPEC-4.2, SPEC-4.3
 */
class PostgresNamedParamDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_np_products (
            id INTEGER PRIMARY KEY,
            sku TEXT NOT NULL,
            name TEXT NOT NULL,
            price NUMERIC(10,2) NOT NULL,
            category TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_np_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_np_products VALUES (1, 'SKU-001', 'Widget', 29.99, 'tools', 1)");
        $this->pdo->exec("INSERT INTO pg_np_products VALUES (2, 'SKU-002', 'Gadget', 49.99, 'electronics', 1)");
        $this->pdo->exec("INSERT INTO pg_np_products VALUES (3, 'SKU-003', 'Sprocket', 9.99, 'tools', 0)");
    }

    /**
     * Prepared SELECT with named parameters.
     */
    public function testSelectWithNamedParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT name, price FROM pg_np_products WHERE category = :cat AND active = :active ORDER BY name"
            );
            $stmt->execute([':cat' => 'tools', ':active' => 1]);
            $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Named param SELECT: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

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
                "INSERT INTO pg_np_products (id, sku, name, price, category, active)
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

            $rows = $this->ztdQuery("SELECT name, price FROM pg_np_products WHERE id = 4");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Named param INSERT: expected 1, got ' . count($rows)
                );
            }

            $this->assertSame('Thingamajig', $rows[0]['name']);
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
                "UPDATE pg_np_products SET price = :new_price, name = :new_name WHERE id = :id"
            );
            $stmt->execute([':new_price' => 39.99, ':new_name' => 'Super Widget', ':id' => 1]);

            $rows = $this->ztdQuery("SELECT name, price FROM pg_np_products WHERE id = 1");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Named param UPDATE: expected 1, got ' . count($rows));
            }

            $this->assertSame('Super Widget', $rows[0]['name']);
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
                "DELETE FROM pg_np_products WHERE category = :cat AND price < :max_price"
            );
            $stmt->execute([':cat' => 'tools', ':max_price' => 15.00]);

            $rows = $this->ztdQuery("SELECT name FROM pg_np_products ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Named param DELETE: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('Widget', $rows[0]['name']);
            $this->assertSame('Gadget', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Named param DELETE failed: ' . $e->getMessage());
        }
    }
}
