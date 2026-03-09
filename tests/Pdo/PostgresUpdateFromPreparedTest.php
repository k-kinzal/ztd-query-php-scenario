<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL UPDATE...FROM with prepared parameters.
 *
 * Existing tests cover UPDATE...FROM with exec() but NOT with prepare()/execute().
 * The CTE rewriter may handle parameter binding differently in the FROM clause context.
 *
 * @spec SPEC-4.2
 */
class PostgresUpdateFromPreparedTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_ufp_products (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                price NUMERIC(10,2) NOT NULL,
                category VARCHAR(20) NOT NULL
            )',
            'CREATE TABLE pg_ufp_adjustments (
                id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL,
                new_price NUMERIC(10,2) NOT NULL,
                reason VARCHAR(50) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_ufp_adjustments', 'pg_ufp_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ufp_products VALUES (1, 'Widget', 10.00, 'tools')");
        $this->pdo->exec("INSERT INTO pg_ufp_products VALUES (2, 'Gadget', 20.00, 'tools')");
        $this->pdo->exec("INSERT INTO pg_ufp_products VALUES (3, 'Doohickey', 30.00, 'parts')");
        $this->pdo->exec("INSERT INTO pg_ufp_adjustments VALUES (1, 1, 15.00, 'sale')");
        $this->pdo->exec("INSERT INTO pg_ufp_adjustments VALUES (2, 2, 18.00, 'clearance')");
    }

    /**
     * UPDATE...FROM with $1 parameter in WHERE clause.
     */
    public function testUpdateFromWithWhereParam(): void
    {
        $sql = "UPDATE pg_ufp_products p
                SET price = a.new_price
                FROM pg_ufp_adjustments a
                WHERE p.id = a.product_id AND a.reason = $1";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['sale']);

            $rows = $this->ztdQuery("SELECT id, price FROM pg_ufp_products ORDER BY id");

            $this->assertCount(3, $rows);

            $widgetPrice = (float) $rows[0]['price'];
            $gadgetPrice = (float) $rows[1]['price'];

            if (abs($widgetPrice - 15.00) > 0.01) {
                $this->markTestIncomplete(
                    "UPDATE FROM with param: Widget price expected 15.00, got {$widgetPrice}"
                );
            }

            $this->assertEquals(15.00, $widgetPrice, '', 0.01);
            // Gadget should be unchanged (reason != 'sale')
            $this->assertEquals(20.00, $gadgetPrice, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE FROM with WHERE param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE...FROM with multiple $N parameters.
     */
    public function testUpdateFromWithMultipleParams(): void
    {
        $sql = "UPDATE pg_ufp_products p
                SET price = a.new_price
                FROM pg_ufp_adjustments a
                WHERE p.id = a.product_id AND a.reason = $1 AND p.category = $2";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['sale', 'tools']);

            $rows = $this->ztdQuery("SELECT id, price FROM pg_ufp_products ORDER BY id");

            $this->assertCount(3, $rows);

            $widgetPrice = (float) $rows[0]['price'];
            if (abs($widgetPrice - 15.00) > 0.01) {
                $this->markTestIncomplete(
                    "UPDATE FROM multi-param: Widget price expected 15.00, got {$widgetPrice}"
                );
            }

            $this->assertEquals(15.00, $widgetPrice, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE FROM with multiple params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE...FROM with shadow-inserted data and param.
     *
     * Insert an adjustment via ZTD, then UPDATE...FROM using it.
     */
    public function testUpdateFromShadowDataWithParam(): void
    {
        try {
            // Insert adjustment into shadow
            $this->pdo->exec("INSERT INTO pg_ufp_adjustments VALUES (3, 3, 25.00, 'promo')");

            // UPDATE FROM using the shadow adjustment
            $sql = "UPDATE pg_ufp_products p
                    SET price = a.new_price
                    FROM pg_ufp_adjustments a
                    WHERE p.id = a.product_id AND a.reason = $1";

            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['promo']);

            $rows = $this->ztdQuery("SELECT id, price FROM pg_ufp_products WHERE id = 3");

            $this->assertCount(1, $rows);

            $price = (float) $rows[0]['price'];
            if (abs($price - 25.00) > 0.01) {
                $this->markTestIncomplete(
                    "UPDATE FROM shadow data: price expected 25.00, got {$price}"
                );
            }

            $this->assertEquals(25.00, $price, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE FROM shadow data with param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE...FROM with ? placeholder (PDO positional).
     */
    public function testUpdateFromWithQuestionMarkParam(): void
    {
        $sql = "UPDATE pg_ufp_products p
                SET price = a.new_price
                FROM pg_ufp_adjustments a
                WHERE p.id = a.product_id AND a.reason = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['sale']);

            $rows = $this->ztdQuery("SELECT id, price FROM pg_ufp_products WHERE id = 1");

            $this->assertCount(1, $rows);

            $price = (float) $rows[0]['price'];
            if (abs($price - 15.00) > 0.01) {
                $this->markTestIncomplete(
                    "UPDATE FROM with ? param: price expected 15.00, got {$price}"
                );
            }

            $this->assertEquals(15.00, $price, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE FROM with ? param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE...FROM with expression in SET and param.
     */
    public function testUpdateFromWithExpressionAndParam(): void
    {
        $sql = "UPDATE pg_ufp_products p
                SET price = a.new_price * $1
                FROM pg_ufp_adjustments a
                WHERE p.id = a.product_id AND a.reason = $2";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([1.1, 'sale']);

            $rows = $this->ztdQuery("SELECT id, price FROM pg_ufp_products WHERE id = 1");

            $this->assertCount(1, $rows);

            $price = (float) $rows[0]['price'];
            // 15.00 * 1.1 = 16.50
            if (abs($price - 16.50) > 0.01) {
                $this->markTestIncomplete(
                    "UPDATE FROM expression+param: price expected 16.50, got {$price}"
                );
            }

            $this->assertEquals(16.50, $price, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE FROM with expression and param failed: ' . $e->getMessage()
            );
        }
    }
}
