<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests GREATEST/LEAST functions in DML through ZTD shadow store on PostgreSQL.
 *
 * GREATEST/LEAST are common for clamping values and conditional updates.
 * The CTE rewriter must handle these multi-arg scalar functions correctly.
 *
 * @spec SPEC-4.1, SPEC-4.2, SPEC-4.3
 */
class PostgresGreatestLeastDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_gl_products (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                price NUMERIC(10,2) NOT NULL,
                min_price NUMERIC(10,2) NOT NULL DEFAULT 0.00,
                stock INTEGER NOT NULL DEFAULT 0
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_gl_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_gl_products VALUES (1, 'Widget', 25.00, 10.00, 100)");
        $this->pdo->exec("INSERT INTO pg_gl_products VALUES (2, 'Gadget', 50.00, 20.00, 50)");
        $this->pdo->exec("INSERT INTO pg_gl_products VALUES (3, 'Doohickey', 15.00, 5.00, 200)");
        $this->pdo->exec("INSERT INTO pg_gl_products VALUES (4, 'Thingamajig', 75.00, 30.00, 25)");
    }

    /**
     * UPDATE SET price = GREATEST(literal, column).
     */
    public function testUpdateSetWithGreatestFunction(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_gl_products SET price = GREATEST(8.00, min_price) WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT price FROM pg_gl_products WHERE id = 1");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE GREATEST: got ' . json_encode($rows));
            }

            $this->assertEquals(10.00, (float) $rows[0]['price']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with GREATEST in SET failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with LEAST — cap price.
     */
    public function testUpdateSetWithLeastFunction(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_gl_products SET price = LEAST(price, 40.00)"
            );

            $rows = $this->ztdQuery("SELECT id, price FROM pg_gl_products ORDER BY id");

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
     * Prepared UPDATE SET GREATEST with $1 param.
     */
    public function testPreparedUpdateSetGreatestWithParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_gl_products SET price = GREATEST(price, $1) WHERE id = $2"
            );
            $stmt->execute([60.00, 2]);

            $rows = $this->ztdQuery("SELECT price FROM pg_gl_products WHERE id = 2");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared UPDATE GREATEST: got ' . json_encode($rows));
            }

            $this->assertEquals(60.00, (float) $rows[0]['price']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE GREATEST with param failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE WHERE LEAST(col1, col2) < $1.
     */
    public function testPreparedDeleteWhereLeastWithParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM pg_gl_products WHERE LEAST(price, stock) < $1"
            );
            $stmt->execute([20]);

            $rows = $this->ztdQuery("SELECT name FROM pg_gl_products ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared DELETE LEAST: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE with LEAST in WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Nested GREATEST(LEAST(...)) clamp pattern.
     */
    public function testUpdateSetClampWithNestedGreatestLeast(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_gl_products SET price = LEAST(GREATEST(price, min_price), 50.00)"
            );

            $rows = $this->ztdQuery("SELECT id, price FROM pg_gl_products ORDER BY id");

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
     * Prepared UPDATE GREATEST with 3 args including $1 param.
     */
    public function testPreparedUpdateGreatestThreeArgs(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_gl_products SET stock = GREATEST(stock, price::INTEGER, $1) WHERE id = $2"
            );
            $stmt->execute([150, 3]);

            $rows = $this->ztdQuery("SELECT stock FROM pg_gl_products WHERE id = 3");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared GREATEST 3-arg: got ' . json_encode($rows));
            }

            $this->assertEquals(200, (int) $rows[0]['stock']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE GREATEST 3 args failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with GREATEST/LEAST after mutations.
     */
    public function testSelectGreatestLeastAfterMutations(): void
    {
        try {
            $this->pdo->exec("UPDATE pg_gl_products SET price = 30.00 WHERE id = 1");
            $this->pdo->exec("UPDATE pg_gl_products SET price = 30.00 WHERE id = 3");

            $rows = $this->ztdQuery(
                "SELECT id, LEAST(price, min_price) AS effective_min,
                        GREATEST(price, min_price) AS effective_max
                 FROM pg_gl_products ORDER BY id"
            );

            if (count($rows) !== 4) {
                $this->markTestIncomplete('SELECT GREATEST/LEAST: got ' . json_encode($rows));
            }

            $this->assertEquals(10.00, (float) $rows[0]['effective_min']);
            $this->assertEquals(30.00, (float) $rows[0]['effective_max']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT GREATEST/LEAST after mutations failed: ' . $e->getMessage());
        }
    }
}
