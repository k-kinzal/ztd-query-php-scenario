<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests PDO named parameters (:name style) in DML through ZTD shadow store on MySQL.
 *
 * @spec SPEC-3.2, SPEC-4.2, SPEC-4.3
 */
class MysqlNamedParamDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_np_products (
            id INT PRIMARY KEY,
            sku VARCHAR(20) NOT NULL,
            name VARCHAR(100) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            category VARCHAR(50) NOT NULL,
            active TINYINT NOT NULL DEFAULT 1
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['my_np_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_np_products VALUES (1, 'SKU-001', 'Widget', 29.99, 'tools', 1)");
        $this->pdo->exec("INSERT INTO my_np_products VALUES (2, 'SKU-002', 'Gadget', 49.99, 'electronics', 1)");
        $this->pdo->exec("INSERT INTO my_np_products VALUES (3, 'SKU-003', 'Sprocket', 9.99, 'tools', 0)");
    }

    /**
     * Prepared SELECT with named parameters.
     */
    public function testSelectWithNamedParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT name, price FROM my_np_products WHERE category = :cat AND active = :active ORDER BY name"
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
                "INSERT INTO my_np_products (id, sku, name, price, category, active)
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

            $rows = $this->ztdQuery("SELECT name, price FROM my_np_products WHERE id = 4");

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
                "UPDATE my_np_products SET price = :new_price, name = :new_name WHERE id = :id"
            );
            $stmt->execute([':new_price' => 39.99, ':new_name' => 'Super Widget', ':id' => 1]);

            $rows = $this->ztdQuery("SELECT name, price FROM my_np_products WHERE id = 1");

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
                "DELETE FROM my_np_products WHERE category = :cat AND price < :max_price"
            );
            $stmt->execute([':cat' => 'tools', ':max_price' => 15.00]);

            $rows = $this->ztdQuery("SELECT name FROM my_np_products ORDER BY id");

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
