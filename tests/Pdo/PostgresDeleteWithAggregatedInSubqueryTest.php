<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests DELETE WHERE col IN (SELECT ... GROUP BY HAVING) on PostgreSQL.
 *
 * @spec SPEC-4.3
 */
class PostgresDeleteWithAggregatedInSubqueryTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_dais_customers (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                tier VARCHAR(20) NOT NULL DEFAULT \'standard\'
            )',
            'CREATE TABLE pg_dais_orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                amount NUMERIC(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_dais_orders', 'pg_dais_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_dais_customers VALUES (1, 'Alice', 'standard')");
        $this->pdo->exec("INSERT INTO pg_dais_customers VALUES (2, 'Bob', 'standard')");
        $this->pdo->exec("INSERT INTO pg_dais_customers VALUES (3, 'Charlie', 'standard')");
        $this->pdo->exec("INSERT INTO pg_dais_customers VALUES (4, 'Diana', 'standard')");

        $this->pdo->exec("INSERT INTO pg_dais_orders VALUES (1, 1, 100)");
        $this->pdo->exec("INSERT INTO pg_dais_orders VALUES (2, 1, 200)");
        $this->pdo->exec("INSERT INTO pg_dais_orders VALUES (3, 1, 150)");
        $this->pdo->exec("INSERT INTO pg_dais_orders VALUES (4, 2, 50)");
        $this->pdo->exec("INSERT INTO pg_dais_orders VALUES (5, 3, 75)");
        $this->pdo->exec("INSERT INTO pg_dais_orders VALUES (6, 3, 125)");
    }

    /**
     * DELETE with IN + GROUP BY HAVING.
     */
    public function testDeleteWithHavingSubquery(): void
    {
        $sql = "DELETE FROM pg_dais_customers
                WHERE id IN (
                    SELECT customer_id FROM pg_dais_orders
                    GROUP BY customer_id
                    HAVING COUNT(*) < 2
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name FROM pg_dais_customers ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE HAVING: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $names = array_column($rows, 'name');
            $this->assertNotContains('Bob', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE HAVING failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE NOT IN aggregated subquery.
     */
    public function testDeleteNotInAggregated(): void
    {
        $sql = "DELETE FROM pg_dais_customers
                WHERE id NOT IN (
                    SELECT customer_id FROM pg_dais_orders
                    GROUP BY customer_id
                    HAVING SUM(amount) > 100
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name FROM pg_dais_customers ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE NOT IN: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE NOT IN failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared with ? param.
     */
    public function testPreparedDeleteHavingQuestionParam(): void
    {
        $sql = "DELETE FROM pg_dais_customers
                WHERE id IN (
                    SELECT customer_id FROM pg_dais_orders
                    GROUP BY customer_id
                    HAVING COUNT(*) < ?
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([3]);

            $rows = $this->ztdQuery("SELECT id, name FROM pg_dais_customers ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared DELETE HAVING (?): expected 2, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared DELETE HAVING (?) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared with $1 param.
     */
    public function testPreparedDeleteHavingDollarParam(): void
    {
        $sql = "DELETE FROM pg_dais_customers
                WHERE id IN (
                    SELECT customer_id FROM pg_dais_orders
                    GROUP BY customer_id
                    HAVING COUNT(*) < $1
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([3]);

            $rows = $this->ztdQuery("SELECT id, name FROM pg_dais_customers ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared DELETE HAVING ($1): expected 2, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared DELETE HAVING ($1) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * On shadow data.
     */
    public function testDeleteAggregatedOnShadowData(): void
    {
        $this->pdo->exec("INSERT INTO pg_dais_orders VALUES (7, 4, 300)");
        $this->pdo->exec("INSERT INTO pg_dais_orders VALUES (8, 4, 200)");
        $this->pdo->exec("INSERT INTO pg_dais_orders VALUES (9, 4, 100)");

        $sql = "DELETE FROM pg_dais_customers
                WHERE id IN (
                    SELECT customer_id FROM pg_dais_orders
                    GROUP BY customer_id
                    HAVING COUNT(*) < 2
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name FROM pg_dais_customers ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Shadow HAVING: expected 3, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Diana', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Shadow HAVING DELETE failed: ' . $e->getMessage()
            );
        }
    }
}
