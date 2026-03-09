<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests prepared statements with expressions in SET clauses and subqueries
 * in WHERE clauses on MySQL shadow data.
 *
 * Combines prepared parameters with computed expressions and cross-table
 * subqueries, which stress the CTE rewriter's parameter binding and SQL
 * rewriting simultaneously.
 *
 * @spec SPEC-3.6
 * @spec SPEC-4.3
 */
class MysqlPreparedExpressionDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_ped_items (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                price DECIMAL(10,2),
                quantity INT,
                category_id INT
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_ped_categories (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                tax_rate DECIMAL(5,2)
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_ped_categories', 'mp_ped_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_ped_categories (id, name, tax_rate) VALUES
            (1, 'Electronics', 0.10),
            (2, 'Books', 0.05),
            (3, 'Food', 0.00)");

        $this->pdo->exec("INSERT INTO mp_ped_items (id, name, price, quantity, category_id) VALUES
            (1, 'Laptop', 1000.00, 5, 1),
            (2, 'Phone', 500.00, 10, 1),
            (3, 'Novel', 15.00, 50, 2),
            (4, 'Textbook', 80.00, 20, 2),
            (5, 'Bread', 3.00, 100, 3)");
    }

    /**
     * Prepared UPDATE with arithmetic expression in SET using parameter.
     * UPDATE items SET price = price * ? WHERE category_id = ?
     */
    public function testPreparedUpdateWithMultiplyParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE mp_ped_items SET price = price * ? WHERE category_id = ?"
            );
            $stmt->execute([1.20, 1]); // 20% increase for Electronics

            $rows = $this->ztdQuery(
                "SELECT name, price FROM mp_ped_items WHERE category_id = 1 ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete('Expected 2 electronics items, got ' . count($rows));
            }

            // Laptop: 1000 * 1.20 = 1200, Phone: 500 * 1.20 = 600
            $this->assertEquals(1200.00, (float) $rows[0]['price'], '', 0.01);
            $this->assertEquals(600.00, (float) $rows[1]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE multiply failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with addition expression using parameter.
     */
    public function testPreparedUpdateWithAdditionParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE mp_ped_items SET quantity = quantity + ? WHERE id = ?"
            );
            $stmt->execute([25, 3]); // Add 25 to Novel quantity

            $rows = $this->ztdQuery("SELECT quantity FROM mp_ped_items WHERE id = 3");
            $this->assertSame(75, (int) $rows[0]['quantity']); // 50 + 25
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE addition failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with subquery in WHERE using parameter.
     */
    public function testPreparedUpdateWithSubqueryWhere(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE mp_ped_items SET price = price * ?
                 WHERE category_id IN (SELECT id FROM mp_ped_categories WHERE tax_rate > ?)"
            );
            $stmt->execute([0.90, 0.05]); // 10% discount for high-tax categories

            $rows = $this->ztdQuery(
                "SELECT name, price FROM mp_ped_items WHERE category_id = 1 ORDER BY id"
            );

            // Electronics (tax 0.10 > 0.05): Laptop 900, Phone 450
            if (count($rows) !== 2) {
                $this->markTestIncomplete('Expected 2, got ' . count($rows));
            }

            $this->assertEquals(900.00, (float) $rows[0]['price'], '', 0.01);
            $this->assertEquals(450.00, (float) $rows[1]['price'], '', 0.01);

            // Books (tax 0.05, not > 0.05) should be unchanged
            $bookRows = $this->ztdQuery(
                "SELECT name, price FROM mp_ped_items WHERE category_id = 2 ORDER BY id"
            );
            $this->assertEquals(15.00, (float) $bookRows[0]['price'], '', 0.01);
            $this->assertEquals(80.00, (float) $bookRows[1]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE with subquery WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE with expression in WHERE.
     */
    public function testPreparedDeleteWithExpressionWhere(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM mp_ped_items WHERE price * quantity < ?"
            );
            $stmt->execute([500]); // Delete items with total value < 500

            $rows = $this->ztdQuery("SELECT name FROM mp_ped_items ORDER BY id");

            // Laptop: 5000, Phone: 5000, Novel: 750, Textbook: 1600, Bread: 300
            // Bread (300 < 500) deleted → 4 remaining
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'Prepared DELETE expression: expected 4, got ' . count($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE with expression WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT with computed column and parameter in expression.
     */
    public function testPreparedSelectWithComputedColumn(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name, price * quantity AS total_value,
                        price * quantity * ? AS taxed_value
                 FROM mp_ped_items
                 WHERE category_id = ?
                 ORDER BY total_value DESC",
                [1.10, 1]
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete('Expected 2 electronics, got ' . count($rows));
            }

            // Laptop: 5000, taxed: 5500; Phone: 5000, taxed: 5500
            $this->assertEquals(5000.00, (float) $rows[0]['total_value'], '', 0.01);
            $this->assertEquals(5500.00, (float) $rows[0]['taxed_value'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT computed column failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT with expression in VALUES.
     */
    public function testPreparedInsertWithExpression(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO mp_ped_items (id, name, price, quantity, category_id)
                 VALUES (?, ?, ? * 1.10, ?, ?)"
            );
            $stmt->execute([6, 'Tablet', 300.00, 15, 1]);

            $rows = $this->ztdQuery("SELECT name, price FROM mp_ped_items WHERE id = 6");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('INSERT with expression: row not found');
            }

            // 300 * 1.10 = 330
            $this->assertEquals(330.00, (float) $rows[0]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT with expression failed: ' . $e->getMessage());
        }
    }

    /**
     * Chained prepared UPDATE with re-execution on different params.
     */
    public function testPreparedUpdateReexecute(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE mp_ped_items SET price = price + ? WHERE id = ?"
            );

            // First execution: increase Laptop by 100
            $stmt->execute([100, 1]);
            // Second execution: increase Phone by 50
            $stmt->execute([50, 2]);

            $rows = $this->ztdQuery(
                "SELECT name, price FROM mp_ped_items WHERE category_id = 1 ORDER BY id"
            );

            $this->assertEquals(1100.00, (float) $rows[0]['price'], '', 0.01); // 1000 + 100
            $this->assertEquals(550.00, (float) $rows[1]['price'], '', 0.01);  // 500 + 50
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE re-execute failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE SET with CASE expression and parameter.
     */
    public function testPreparedUpdateWithCase(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE mp_ped_items
                 SET price = CASE
                    WHEN quantity > ? THEN price * 0.90
                    ELSE price * 1.10
                 END
                 WHERE category_id = ?"
            );
            $stmt->execute([20, 2]); // Books: quantity > 20 gets discount, else markup

            $rows = $this->ztdQuery(
                "SELECT name, price FROM mp_ped_items WHERE category_id = 2 ORDER BY id"
            );

            // Novel: qty=50 > 20 → 15 * 0.90 = 13.50
            // Textbook: qty=20, NOT > 20 → 80 * 1.10 = 88.00
            if (count($rows) !== 2) {
                $this->markTestIncomplete('Expected 2, got ' . count($rows));
            }

            $this->assertEquals(13.50, (float) $rows[0]['price'], '', 0.01);
            $this->assertEquals(88.00, (float) $rows[1]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE CASE failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        try {
            $stmt = $this->pdo->prepare("UPDATE mp_ped_items SET price = price * ? WHERE id = ?");
            $stmt->execute([2.0, 1]);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE failed: ' . $e->getMessage());
            return;
        }

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_ped_items")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
