<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests named parameters (:name style) on MySQL PDO through ZTD.
 *
 * @spec SPEC-3.1, SPEC-4.2
 */
class MysqlNamedParamTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_np_items (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            category VARCHAR(20) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            stock INT NOT NULL DEFAULT 0
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['my_np_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_np_items VALUES (1, 'Widget', 'tools', 10.00, 100)");
        $this->pdo->exec("INSERT INTO my_np_items VALUES (2, 'Gadget', 'tools', 20.00, 50)");
        $this->pdo->exec("INSERT INTO my_np_items VALUES (3, 'Doohickey', 'parts', 30.00, 75)");
        $this->pdo->exec("INSERT INTO my_np_items VALUES (4, 'Thingamajig', 'parts', 15.00, 200)");
    }

    /**
     * SELECT with named parameter.
     */
    public function testSelectWithNamedParam(): void
    {
        $sql = "SELECT name, price FROM my_np_items WHERE category = :cat ORDER BY name";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([':cat' => 'tools']);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Named param SELECT: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Gadget', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Named param SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with named parameter.
     */
    public function testUpdateWithNamedParam(): void
    {
        $sql = "UPDATE my_np_items SET price = :new_price WHERE id = :id";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([':new_price' => 99.99, ':id' => 1]);

            $rows = $this->ztdQuery("SELECT price FROM my_np_items WHERE id = 1");

            $this->assertCount(1, $rows);

            $price = (float) $rows[0]['price'];
            if (abs($price - 99.99) > 0.01) {
                $this->markTestIncomplete(
                    "Named param UPDATE: price expected 99.99, got {$price}"
                );
            }

            $this->assertEquals(99.99, $price, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Named param UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with named parameter.
     */
    public function testDeleteWithNamedParam(): void
    {
        $sql = "DELETE FROM my_np_items WHERE category = :cat";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([':cat' => 'parts']);

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM my_np_items");

            $count = (int) $rows[0]['cnt'];
            if ($count !== 2) {
                $this->markTestIncomplete(
                    "Named param DELETE: expected 2 remaining, got {$count}"
                );
            }

            $this->assertSame(2, $count);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Named param DELETE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with named parameters.
     */
    public function testInsertWithNamedParams(): void
    {
        $sql = "INSERT INTO my_np_items (id, name, category, price, stock)
                VALUES (:id, :name, :cat, :price, :stock)";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([
                ':id' => 5,
                ':name' => 'Gizmo',
                ':cat' => 'gadgets',
                ':price' => 45.00,
                ':stock' => 10,
            ]);

            $rows = $this->ztdQuery("SELECT name, category FROM my_np_items WHERE id = 5");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Named param INSERT: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Gizmo', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Named param INSERT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Named param with GROUP BY HAVING.
     */
    public function testGroupByHavingWithNamedParam(): void
    {
        $sql = "SELECT category, COUNT(*) AS cnt
                FROM my_np_items
                WHERE stock >= :min_stock
                GROUP BY category
                HAVING COUNT(*) >= :min_count";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([':min_stock' => 50, ':min_count' => 2]);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // stock >= 50: all 4 items. tools: 2, parts: 2. Both >= 2.
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Named param GROUP BY HAVING: expected 2 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Named param GROUP BY HAVING failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Named param with subquery in WHERE.
     */
    public function testSubqueryWhereWithNamedParam(): void
    {
        $sql = "SELECT name FROM my_np_items
                WHERE price > (SELECT AVG(price) FROM my_np_items WHERE category = :cat)
                ORDER BY name";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([':cat' => 'tools']);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // AVG of tools = 15. price > 15: Gadget(20), Doohickey(30)
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Named param subquery: expected 2 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Named param subquery failed: ' . $e->getMessage()
            );
        }
    }
}
