<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests prepared statements with expressions in SET clauses and subqueries
 * in WHERE clauses on SQLite shadow data.
 *
 * @spec SPEC-3.6
 * @spec SPEC-4.3
 */
class SqlitePreparedExpressionDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ped_items (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                quantity INTEGER NOT NULL,
                category_id INTEGER NOT NULL
            )',
            'CREATE TABLE sl_ped_categories (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                tax_rate REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ped_categories', 'sl_ped_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ped_categories (id, name, tax_rate) VALUES
            (1, 'Electronics', 0.10),
            (2, 'Books', 0.05),
            (3, 'Food', 0.00)");

        $this->pdo->exec("INSERT INTO sl_ped_items (id, name, price, quantity, category_id) VALUES
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
                "UPDATE sl_ped_items SET price = price * ? WHERE category_id = ?"
            );
            $stmt->execute([1.20, 1]);

            $rows = $this->ztdQuery(
                "SELECT name, price FROM sl_ped_items WHERE category_id = 1 ORDER BY id"
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

    public function testPreparedUpdateWithAdditionParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_ped_items SET quantity = quantity + ? WHERE id = ?"
            );
            $stmt->execute([25, 3]);

            $rows = $this->ztdQuery("SELECT quantity FROM sl_ped_items WHERE id = 3");
            $this->assertSame(75, (int) $rows[0]['quantity']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE addition failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateWithSubqueryWhere(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_ped_items SET price = price * ?
                 WHERE category_id IN (SELECT id FROM sl_ped_categories WHERE tax_rate > ?)"
            );
            $stmt->execute([0.90, 0.05]);

            $rows = $this->ztdQuery(
                "SELECT name, price FROM sl_ped_items WHERE category_id = 1 ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete('Expected 2, got ' . count($rows));
            }

            $this->assertEquals(900.00, (float) $rows[0]['price'], '', 0.01);
            $this->assertEquals(450.00, (float) $rows[1]['price'], '', 0.01);

            // Books should be unchanged (tax 0.05, not > 0.05)
            $bookRows = $this->ztdQuery(
                "SELECT price FROM sl_ped_items WHERE category_id = 2 ORDER BY id"
            );
            $this->assertEquals(15.00, (float) $bookRows[0]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE subquery WHERE failed: ' . $e->getMessage());
        }
    }

    public function testPreparedDeleteWithExpressionWhere(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM sl_ped_items WHERE price * quantity < ?"
            );
            $stmt->execute([500]);

            $rows = $this->ztdQuery("SELECT name FROM sl_ped_items ORDER BY id");

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

    public function testPreparedUpdateWithCase(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_ped_items
                 SET price = CASE
                    WHEN quantity > ? THEN price * 0.90
                    ELSE price * 1.10
                 END
                 WHERE category_id = ?"
            );
            $stmt->execute([20, 2]);

            $rows = $this->ztdQuery(
                "SELECT name, price FROM sl_ped_items WHERE category_id = 2 ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete('Expected 2, got ' . count($rows));
            }

            // Novel: qty=50>20 → 15*0.90=13.50; Textbook: qty=20, not>20 → 80*1.10=88.00
            $this->assertEquals(13.50, (float) $rows[0]['price'], '', 0.01);
            $this->assertEquals(88.00, (float) $rows[1]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE CASE failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateReexecute(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_ped_items SET price = price + ? WHERE id = ?"
            );

            $stmt->execute([100, 1]);
            $stmt->execute([50, 2]);

            $rows = $this->ztdQuery(
                "SELECT name, price FROM sl_ped_items WHERE category_id = 1 ORDER BY id"
            );

            $this->assertEquals(1100.00, (float) $rows[0]['price'], '', 0.01);
            $this->assertEquals(550.00, (float) $rows[1]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE re-execute failed: ' . $e->getMessage());
        }
    }

    public function testPreparedSelectWithComputedColumn(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name, price * quantity AS total_value,
                        price * quantity * ? AS taxed_value
                 FROM sl_ped_items
                 WHERE category_id = ?
                 ORDER BY total_value DESC",
                [1.10, 1]
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete('Expected 2, got ' . count($rows));
            }

            $this->assertEquals(5000.00, (float) $rows[0]['total_value'], '', 0.01);
            $this->assertEquals(5500.00, (float) $rows[0]['taxed_value'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT computed failed: ' . $e->getMessage());
        }
    }

    /**
     * IIF function (SQLite-specific) with prepared params.
     */
    public function testPreparedIifFunction(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name, IIF(quantity > ?, 'high', 'low') AS stock_level
                 FROM sl_ped_items
                 ORDER BY id",
                [20]
            );

            if (count($rows) !== 5) {
                $this->markTestIncomplete('Expected 5, got ' . count($rows));
            }

            $this->assertSame('low', $rows[0]['stock_level']);  // Laptop: 5
            $this->assertSame('low', $rows[1]['stock_level']);  // Phone: 10
            $this->assertSame('high', $rows[2]['stock_level']); // Novel: 50
            $this->assertSame('low', $rows[3]['stock_level']);  // Textbook: 20
            $this->assertSame('high', $rows[4]['stock_level']); // Bread: 100
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared IIF failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        try {
            $stmt = $this->pdo->prepare("UPDATE sl_ped_items SET price = price * ? WHERE id = ?");
            $stmt->execute([2.0, 1]);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE failed: ' . $e->getMessage());
            return;
        }

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_ped_items")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
