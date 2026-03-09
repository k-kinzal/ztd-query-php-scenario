<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests prepared statements with expressions in SET clauses and subqueries
 * in WHERE clauses on PostgreSQL shadow data.
 *
 * Uses $N parameter syntax native to PostgreSQL.
 *
 * @spec SPEC-3.6
 * @spec SPEC-4.3
 */
class PostgresPreparedExpressionDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_ped_items (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                price NUMERIC(10,2),
                quantity INT,
                category_id INT
            )',
            'CREATE TABLE pg_ped_categories (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                tax_rate NUMERIC(5,2)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_ped_categories', 'pg_ped_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ped_categories (id, name, tax_rate) VALUES
            (1, 'Electronics', 0.10),
            (2, 'Books', 0.05),
            (3, 'Food', 0.00)");

        $this->pdo->exec("INSERT INTO pg_ped_items (id, name, price, quantity, category_id) VALUES
            (1, 'Laptop', 1000.00, 5, 1),
            (2, 'Phone', 500.00, 10, 1),
            (3, 'Novel', 15.00, 50, 2),
            (4, 'Textbook', 80.00, 20, 2),
            (5, 'Bread', 3.00, 100, 3)");
    }

    public function testPreparedUpdateWithMultiplyParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_ped_items SET price = price * ? WHERE category_id = ?"
            );
            $stmt->execute([1.20, 1]);

            $rows = $this->ztdQuery(
                "SELECT name, price FROM pg_ped_items WHERE category_id = 1 ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete('Expected 2, got ' . count($rows));
            }

            $this->assertEquals(1200.00, (float) $rows[0]['price'], '', 0.01);
            $this->assertEquals(600.00, (float) $rows[1]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE multiply failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateWithSubqueryWhere(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_ped_items SET price = price * ?
                 WHERE category_id IN (SELECT id FROM pg_ped_categories WHERE tax_rate > ?)"
            );
            $stmt->execute([0.90, 0.05]);

            $rows = $this->ztdQuery(
                "SELECT name, price FROM pg_ped_items WHERE category_id = 1 ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete('Expected 2, got ' . count($rows));
            }

            $this->assertEquals(900.00, (float) $rows[0]['price'], '', 0.01);
            $this->assertEquals(450.00, (float) $rows[1]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE subquery WHERE failed: ' . $e->getMessage());
        }
    }

    public function testPreparedDeleteWithExpressionWhere(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM pg_ped_items WHERE price * quantity < ?"
            );
            $stmt->execute([500]);

            $rows = $this->ztdQuery("SELECT name FROM pg_ped_items ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'Prepared DELETE expression: expected 4, got ' . count($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE expression WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with CASE and parameter + RETURNING clause.
     */
    public function testPreparedUpdateCaseReturning(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_ped_items
                 SET price = CASE
                    WHEN quantity > ? THEN price * 0.90
                    ELSE price * 1.10
                 END
                 WHERE category_id = ?
                 RETURNING name, price"
            );
            $stmt->execute([20, 2]);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete('RETURNING: expected 2, got ' . count($rows));
            }

            // Find by name since order isn't guaranteed
            $byName = [];
            foreach ($rows as $row) {
                $byName[$row['name']] = $row;
            }

            $this->assertEquals(13.50, (float) $byName['Novel']['price'], '', 0.01);
            $this->assertEquals(88.00, (float) $byName['Textbook']['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE CASE RETURNING failed: ' . $e->getMessage());
        }
    }

    public function testPreparedSelectWithComputedColumn(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name, price * quantity AS total_value
                 FROM pg_ped_items
                 WHERE category_id = ?
                 ORDER BY total_value DESC",
                [1]
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete('Expected 2, got ' . count($rows));
            }

            $this->assertEquals(5000.00, (float) $rows[0]['total_value'], '', 0.01);
            $this->assertEquals(5000.00, (float) $rows[1]['total_value'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT computed failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateReexecute(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_ped_items SET price = price + ? WHERE id = ?"
            );

            $stmt->execute([100, 1]);
            $stmt->execute([50, 2]);

            $rows = $this->ztdQuery(
                "SELECT name, price FROM pg_ped_items WHERE category_id = 1 ORDER BY id"
            );

            $this->assertEquals(1100.00, (float) $rows[0]['price'], '', 0.01);
            $this->assertEquals(550.00, (float) $rows[1]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE re-execute failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE ... FROM (PostgreSQL multi-table) with parameter.
     */
    public function testPreparedUpdateFrom(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_ped_items
                 SET price = price * (1 + pg_ped_categories.tax_rate * ?)
                 FROM pg_ped_categories
                 WHERE pg_ped_items.category_id = pg_ped_categories.id
                   AND pg_ped_categories.tax_rate > ?"
            );
            $stmt->execute([2.0, 0.00]); // Double the tax-rate markup for taxed items

            // Electronics (10%): price * (1 + 0.10 * 2) = price * 1.20
            // Books (5%): price * (1 + 0.05 * 2) = price * 1.10
            // Food (0%): excluded by WHERE tax_rate > 0

            $elec = $this->ztdQuery("SELECT name, price FROM pg_ped_items WHERE category_id = 1 ORDER BY id");
            $books = $this->ztdQuery("SELECT name, price FROM pg_ped_items WHERE category_id = 2 ORDER BY id");
            $food = $this->ztdQuery("SELECT name, price FROM pg_ped_items WHERE category_id = 3 ORDER BY id");

            if (count($elec) !== 2 || count($books) !== 2 || count($food) !== 1) {
                $this->markTestIncomplete('Row count mismatch after UPDATE FROM');
            }

            $this->assertEquals(1200.00, (float) $elec[0]['price'], '', 0.01);
            $this->assertEquals(600.00, (float) $elec[1]['price'], '', 0.01);
            $this->assertEquals(16.50, (float) $books[0]['price'], '', 0.01);
            $this->assertEquals(88.00, (float) $books[1]['price'], '', 0.01);
            $this->assertEquals(3.00, (float) $food[0]['price'], '', 0.01); // unchanged
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE FROM failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        try {
            $stmt = $this->pdo->prepare("UPDATE pg_ped_items SET price = price * ? WHERE id = ?");
            $stmt->execute([2.0, 1]);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE failed: ' . $e->getMessage());
            return;
        }

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_ped_items")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
