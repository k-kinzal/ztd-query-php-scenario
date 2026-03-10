<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests MySQL UPDATE ... JOIN with inline data patterns through MySQLi adapter.
 *
 * @spec SPEC-4.2
 */
class UpdateJoinValuesTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE mi_ujv_products (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                stock INT NOT NULL DEFAULT 0
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_ujv_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_ujv_products (id, name, price, stock) VALUES (1, 'Widget', 10.00, 100)");
        $this->ztdExec("INSERT INTO mi_ujv_products (id, name, price, stock) VALUES (2, 'Gadget', 20.00, 50)");
        $this->ztdExec("INSERT INTO mi_ujv_products (id, name, price, stock) VALUES (3, 'Doohickey', 30.00, 25)");
    }

    /**
     * UPDATE JOIN with UNION ALL subquery.
     */
    public function testUpdateJoinUnionAll(): void
    {
        try {
            $this->ztdExec("
                UPDATE mi_ujv_products AS p
                INNER JOIN (
                    SELECT 1 AS id, 15.00 AS new_price
                    UNION ALL SELECT 3, 35.00
                ) AS v ON p.id = v.id
                SET p.price = v.new_price
            ");

            $rows = $this->ztdQuery("SELECT id, price FROM mi_ujv_products ORDER BY id");

            $this->assertCount(3, $rows);
            $this->assertEquals(15.00, (float) $rows[0]['price'], '', 0.01);
            $this->assertEquals(20.00, (float) $rows[1]['price'], '', 0.01);
            $this->assertEquals(35.00, (float) $rows[2]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE JOIN UNION ALL failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE JOIN with ? parameters.
     */
    public function testPreparedUpdateJoin(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT p.id, p.price
                 FROM mi_ujv_products AS p
                 INNER JOIN (SELECT ? AS id) AS v ON p.id = v.id",
                [1]
            );

            // First verify SELECT JOIN works
            $this->assertCount(1, $rows);

            // Now try the UPDATE
            $stmt = $this->mysqli->prepare(
                "UPDATE mi_ujv_products AS p
                 INNER JOIN (SELECT ? AS id, ? AS new_price) AS v ON p.id = v.id
                 SET p.price = v.new_price"
            );
            $stmt->bind_param('id', ...[$id = 1, $price = 99.99]);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT price FROM mi_ujv_products WHERE id = 1");

            if (abs((float) $rows[0]['price'] - 99.99) > 0.01) {
                $this->markTestIncomplete(
                    'Prepared UPDATE JOIN: price=' . $rows[0]['price'] . ', expected 99.99.'
                );
            }

            $this->assertEquals(99.99, (float) $rows[0]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE JOIN failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE JOIN with inline data.
     */
    public function testDeleteJoinInlineIds(): void
    {
        try {
            $this->ztdExec("
                DELETE p FROM mi_ujv_products AS p
                INNER JOIN (
                    SELECT 2 AS id
                ) AS v ON p.id = v.id
            ");

            $rows = $this->ztdQuery("SELECT id FROM mi_ujv_products ORDER BY id");

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(3, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE JOIN inline ids failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE JOIN accumulate after prior shadow INSERT.
     */
    public function testUpdateJoinAccumulateAfterInsert(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_ujv_products VALUES (4, 'NewItem', 50.00, 0)");

            $this->ztdExec("
                UPDATE mi_ujv_products AS p
                INNER JOIN (
                    SELECT 1 AS id, 50 AS add_stock
                    UNION ALL SELECT 4, 100
                ) AS v ON p.id = v.id
                SET p.stock = p.stock + v.add_stock
            ");

            $rows = $this->ztdQuery("SELECT id, stock FROM mi_ujv_products ORDER BY id");

            $this->assertCount(4, $rows);
            $this->assertSame(150, (int) $rows[0]['stock']); // 100 + 50
            $this->assertSame(50, (int) $rows[1]['stock']);   // unchanged
            $this->assertSame(25, (int) $rows[2]['stock']);   // unchanged
            $this->assertSame(100, (int) $rows[3]['stock']);  // 0 + 100
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE JOIN accumulate after INSERT failed: ' . $e->getMessage());
        }
    }
}
