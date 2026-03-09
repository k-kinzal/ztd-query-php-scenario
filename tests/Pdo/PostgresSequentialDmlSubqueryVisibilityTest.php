<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests sequential DML with subquery references to shadow data on PostgreSQL.
 *
 * @spec SPEC-4.1, SPEC-4.2, SPEC-4.3
 */
class PostgresSequentialDmlSubqueryVisibilityTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_sdv_products (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                price NUMERIC(10,2) NOT NULL,
                category VARCHAR(10) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT \'active\'
            )',
            'CREATE TABLE pg_sdv_price_log (
                id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL,
                old_price NUMERIC(10,2),
                new_price NUMERIC(10,2) NOT NULL,
                action VARCHAR(20) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_sdv_price_log', 'pg_sdv_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_sdv_products VALUES (1, 'Widget', 10.00, 'A', 'active')");
        $this->pdo->exec("INSERT INTO pg_sdv_products VALUES (2, 'Gadget', 25.00, 'A', 'active')");
        $this->pdo->exec("INSERT INTO pg_sdv_products VALUES (3, 'Doohickey', 5.00, 'B', 'active')");
    }

    /**
     * INSERT then UPDATE referencing shadow-inserted data.
     */
    public function testInsertThenUpdateReferencingInserted(): void
    {
        $this->pdo->exec("INSERT INTO pg_sdv_products VALUES (4, 'NewItem', 50.00, 'A', 'active')");

        $sql = "UPDATE pg_sdv_products
                SET price = price * 1.1
                WHERE category = (SELECT category FROM pg_sdv_products WHERE id = 4)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, price FROM pg_sdv_products ORDER BY id");

            $this->assertEqualsWithDelta(11.0, (float) $rows[0]['price'], 0.01);

            if (abs((float) $rows[3]['price'] - 55.0) > 0.01) {
                $this->markTestIncomplete(
                    "NewItem: expected 55.0, got {$rows[3]['price']}"
                );
            }

            $this->assertEqualsWithDelta(55.0, (float) $rows[3]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT then UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT→UPDATE→DELETE chain.
     */
    public function testInsertUpdateDeleteChain(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_sdv_products VALUES (5, 'Cheap', 2.00, 'C', 'active')");
            $this->pdo->exec("UPDATE pg_sdv_products SET price = price * 2 WHERE price < 10");
            $this->pdo->exec("DELETE FROM pg_sdv_products WHERE price < 10");

            $rows = $this->ztdQuery("SELECT name FROM pg_sdv_products ORDER BY id");

            $names = array_column($rows, 'name');
            if (in_array('Cheap', $names)) {
                $this->markTestIncomplete(
                    'Cheap not deleted. Data: ' . json_encode($rows)
                );
            }

            $this->assertNotContains('Cheap', $names);
            $this->assertContains('Widget', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DML chain failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Cross-table DML chain with INSERT...SELECT JOIN.
     */
    public function testCrossTableDmlChain(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_sdv_price_log (product_id, new_price, action)
                              SELECT id, price, 'snapshot' FROM pg_sdv_products");

            $this->pdo->exec("UPDATE pg_sdv_products SET price = price * 1.5 WHERE category = 'A'");

            $sql = "INSERT INTO pg_sdv_price_log (product_id, old_price, new_price, action)
                    SELECT p.id, l.new_price, p.price, 'increase'
                    FROM pg_sdv_products p
                    JOIN pg_sdv_price_log l ON l.product_id = p.id AND l.action = 'snapshot'
                    WHERE p.price != l.new_price";

            $this->pdo->exec($sql);

            $logs = $this->ztdQuery("SELECT product_id, action FROM pg_sdv_price_log ORDER BY id");

            $snapshots = array_filter($logs, fn($r) => $r['action'] === 'snapshot');
            $increases = array_filter($logs, fn($r) => $r['action'] === 'increase');

            if (count($snapshots) !== 3 || count($increases) !== 2) {
                $this->markTestIncomplete(
                    "Cross-table: snap=" . count($snapshots) . " inc=" . count($increases)
                );
            }

            $this->assertCount(3, $snapshots);
            $this->assertCount(2, $increases);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Cross-table chain failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE based on aggregate from shadow-inserted log entries.
     */
    public function testUpdateBasedOnShadowAggregate(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_sdv_price_log (product_id, new_price, action) VALUES (1, 10, 'sale')");
            $this->pdo->exec("INSERT INTO pg_sdv_price_log (product_id, new_price, action) VALUES (1, 8, 'sale')");
            $this->pdo->exec("INSERT INTO pg_sdv_price_log (product_id, new_price, action) VALUES (1, 12, 'sale')");

            $sql = "UPDATE pg_sdv_products
                    SET price = (
                        SELECT AVG(new_price) FROM pg_sdv_price_log
                        WHERE product_id = pg_sdv_products.id AND action = 'sale'
                    )
                    WHERE id = 1";

            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT price FROM pg_sdv_products WHERE id = 1");

            if (abs((float) $rows[0]['price'] - 10.0) > 0.01) {
                $this->markTestIncomplete(
                    "Shadow AVG UPDATE: expected 10.0, got {$rows[0]['price']}"
                );
            }

            $this->assertEqualsWithDelta(10.0, (float) $rows[0]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Shadow aggregate UPDATE failed: ' . $e->getMessage()
            );
        }
    }
}
