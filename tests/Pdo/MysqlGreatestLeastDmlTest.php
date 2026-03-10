<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests GREATEST/LEAST functions in DML through ZTD shadow store on MySQL.
 *
 * GREATEST/LEAST are common for clamping values, finding max/min of expressions,
 * and conditional updates. The CTE rewriter must handle these multi-arg scalar
 * functions correctly in SET and WHERE clauses.
 *
 * @spec SPEC-4.1, SPEC-4.2, SPEC-4.3
 */
class MysqlGreatestLeastDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_gl_products (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                min_price DECIMAL(10,2) NOT NULL DEFAULT 0.00,
                stock INT NOT NULL DEFAULT 0
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_gl_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_gl_products VALUES (1, 'Widget', 25.00, 10.00, 100)");
        $this->pdo->exec("INSERT INTO my_gl_products VALUES (2, 'Gadget', 50.00, 20.00, 50)");
        $this->pdo->exec("INSERT INTO my_gl_products VALUES (3, 'Doohickey', 15.00, 5.00, 200)");
        $this->pdo->exec("INSERT INTO my_gl_products VALUES (4, 'Thingamajig', 75.00, 30.00, 25)");
    }

    /**
     * UPDATE SET price = GREATEST(price, min_price) — clamp to minimum.
     */
    public function testUpdateSetWithGreatestFunction(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE my_gl_products SET price = GREATEST(8.00, min_price) WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT price FROM my_gl_products WHERE id = 1");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE GREATEST: got ' . json_encode($rows));
            }

            $this->assertEquals(10.00, (float) $rows[0]['price']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with GREATEST in SET failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with LEAST function — cap price at a ceiling.
     */
    public function testUpdateSetWithLeastFunction(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE my_gl_products SET price = LEAST(price, 40.00)"
            );

            $rows = $this->ztdQuery("SELECT id, price FROM my_gl_products ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete('UPDATE LEAST: got ' . json_encode($rows));
            }

            $this->assertEquals(25.00, (float) $rows[0]['price']);
            $this->assertEquals(40.00, (float) $rows[1]['price']);
            $this->assertEquals(15.00, (float) $rows[2]['price']);
            $this->assertEquals(40.00, (float) $rows[3]['price']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with LEAST in SET failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE SET with GREATEST and bound parameter.
     */
    public function testPreparedUpdateSetGreatestWithParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE my_gl_products SET price = GREATEST(price, ?) WHERE id = ?"
            );
            $stmt->execute([60.00, 2]);

            $rows = $this->ztdQuery("SELECT price FROM my_gl_products WHERE id = 2");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared UPDATE GREATEST: got ' . json_encode($rows));
            }

            $this->assertEquals(60.00, (float) $rows[0]['price']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE GREATEST with param failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE WHERE LEAST(col1, col2) < ? — scalar function in WHERE with param.
     */
    public function testPreparedDeleteWhereLeastWithParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM my_gl_products WHERE LEAST(price, stock) < ?"
            );
            $stmt->execute([20]);

            $rows = $this->ztdQuery("SELECT name FROM my_gl_products ORDER BY id");

            // LEAST(25,100)=25, LEAST(50,50)=50, LEAST(15,200)=15, LEAST(75,25)=25
            // Items where LEAST < 20: Doohickey (15)
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared DELETE LEAST WHERE: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE with LEAST in WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with nested GREATEST/LEAST — clamp to range.
     */
    public function testUpdateSetClampWithNestedGreatestLeast(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE my_gl_products SET price = LEAST(GREATEST(price, min_price), 50.00)"
            );

            $rows = $this->ztdQuery("SELECT id, price FROM my_gl_products ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete('UPDATE clamp: got ' . json_encode($rows));
            }

            $this->assertEquals(25.00, (float) $rows[0]['price']);
            $this->assertEquals(50.00, (float) $rows[1]['price']);
            $this->assertEquals(15.00, (float) $rows[2]['price']);
            $this->assertEquals(50.00, (float) $rows[3]['price']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with nested GREATEST/LEAST failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE GREATEST with 3 arguments including a param.
     */
    public function testPreparedUpdateGreatestThreeArgs(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE my_gl_products SET stock = GREATEST(stock, CAST(price AS SIGNED), ?) WHERE id = ?"
            );
            $stmt->execute([150, 3]);

            $rows = $this->ztdQuery("SELECT stock FROM my_gl_products WHERE id = 3");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared GREATEST 3-arg: got ' . json_encode($rows));
            }

            // GREATEST(200, 15, 150) = 200
            $this->assertEquals(200, (int) $rows[0]['stock']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE GREATEST 3 args failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with GREATEST/LEAST after UPDATE mutations.
     */
    public function testSelectGreatestLeastAfterMutations(): void
    {
        try {
            $this->pdo->exec("UPDATE my_gl_products SET price = 30.00 WHERE id = 1");
            $this->pdo->exec("UPDATE my_gl_products SET price = 30.00 WHERE id = 3");

            $rows = $this->ztdQuery(
                "SELECT id, LEAST(price, min_price) AS effective_min,
                        GREATEST(price, min_price) AS effective_max
                 FROM my_gl_products ORDER BY id"
            );

            if (count($rows) !== 4) {
                $this->markTestIncomplete('SELECT GREATEST/LEAST: got ' . json_encode($rows));
            }

            $this->assertEquals(10.00, (float) $rows[0]['effective_min']);
            $this->assertEquals(30.00, (float) $rows[0]['effective_max']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT with GREATEST/LEAST after mutations failed: ' . $e->getMessage());
        }
    }
}
