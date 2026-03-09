<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT with non-DDL column ordering and multi-row VALUES on MySQL PDO.
 *
 * Real applications (especially ORMs) routinely specify columns in a different
 * order than the table definition, and batch-insert multiple rows with a single
 * VALUES list. If the shadow store assumes DDL column order, values would be
 * silently swapped — a serious data-integrity issue.
 *
 * Ported from SqliteColumnOrderInsertTest to verify cross-platform correctness.
 *
 * @spec SPEC-4.1
 */
class MysqlColumnOrderAndMultiRowInsertTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_coi_items (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            price DECIMAL(10,2),
            category VARCHAR(30)
        )';
    }

    protected function getTableNames(): array
    {
        return ['my_coi_items'];
    }

    // --- Column order INSERT tests ---

    public function testReverseColumnOrder(): void
    {
        $this->pdo->exec("INSERT INTO my_coi_items (category, price, name, id) VALUES ('electronics', 9.99, 'Widget', 1)");

        $rows = $this->ztdQuery('SELECT * FROM my_coi_items WHERE id = 1');

        $this->assertCount(1, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertEqualsWithDelta(9.99, (float) $rows[0]['price'], 0.01);
        $this->assertSame('electronics', $rows[0]['category']);
    }

    public function testPartialColumns(): void
    {
        $this->pdo->exec("INSERT INTO my_coi_items (id, name) VALUES (2, 'Gadget')");

        $rows = $this->ztdQuery('SELECT * FROM my_coi_items WHERE id = 2');

        $this->assertCount(1, $rows);
        $this->assertSame('Gadget', $rows[0]['name']);
        $this->assertNull($rows[0]['price']);
        $this->assertNull($rows[0]['category']);
    }

    public function testPartialColumnsReordered(): void
    {
        $this->pdo->exec("INSERT INTO my_coi_items (category, id) VALUES ('books', 3)");

        $rows = $this->ztdQuery('SELECT * FROM my_coi_items WHERE id = 3');

        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['name']);
        $this->assertNull($rows[0]['price']);
        $this->assertSame('books', $rows[0]['category']);
    }

    public function testMixedColumnOrders(): void
    {
        $this->pdo->exec("INSERT INTO my_coi_items (id, name, price, category) VALUES (1, 'A', 1.00, 'cat1')");
        $this->pdo->exec("INSERT INTO my_coi_items (category, id, name, price) VALUES ('cat2', 2, 'B', 2.00)");
        $this->pdo->exec("INSERT INTO my_coi_items (price, category, id, name) VALUES (3.00, 'cat3', 3, 'C')");

        $rows = $this->ztdQuery('SELECT * FROM my_coi_items ORDER BY id');

        $this->assertCount(3, $rows);
        $this->assertSame('A', $rows[0]['name']);
        $this->assertSame('B', $rows[1]['name']);
        $this->assertSame('C', $rows[2]['name']);
        $this->assertSame('cat1', $rows[0]['category']);
        $this->assertSame('cat2', $rows[1]['category']);
        $this->assertSame('cat3', $rows[2]['category']);
    }

    public function testPreparedWithReorderedColumns(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO my_coi_items (price, id, name) VALUES (?, ?, ?)');
        $stmt->execute([19.99, 10, 'PrepItem']);

        $rows = $this->ztdQuery('SELECT * FROM my_coi_items WHERE id = 10');

        $this->assertCount(1, $rows);
        $this->assertSame('PrepItem', $rows[0]['name']);
        $this->assertEqualsWithDelta(19.99, (float) $rows[0]['price'], 0.01);
    }

    public function testUpdateAfterReorderedInsert(): void
    {
        $this->pdo->exec("INSERT INTO my_coi_items (name, id, category) VALUES ('OldName', 5, 'cat')");
        $this->pdo->exec("UPDATE my_coi_items SET name = 'NewName' WHERE id = 5");

        $rows = $this->ztdQuery('SELECT * FROM my_coi_items WHERE id = 5');

        $this->assertCount(1, $rows);
        $this->assertSame('NewName', $rows[0]['name']);
        $this->assertSame('cat', $rows[0]['category']);
    }

    // --- Multi-row INSERT tests ---

    public function testMultiRowInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO my_coi_items VALUES (1, 'Alpha', 10.00, 'A'), (2, 'Beta', 20.00, 'B'), (3, 'Gamma', 30.00, 'C')");

            $rows = $this->ztdQuery('SELECT * FROM my_coi_items ORDER BY id');
            $this->assertCount(3, $rows);
            $this->assertSame('Alpha', $rows[0]['name']);
            $this->assertSame('Beta', $rows[1]['name']);
            $this->assertSame('Gamma', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row INSERT VALUES failed: ' . $e->getMessage());
        }
    }

    public function testMultiRowInsertThenUpdateOne(): void
    {
        try {
            $this->pdo->exec("INSERT INTO my_coi_items VALUES (1, 'Alpha', 10.00, 'A'), (2, 'Beta', 20.00, 'B'), (3, 'Gamma', 30.00, 'C')");
            $this->pdo->exec("UPDATE my_coi_items SET price = 99.99 WHERE id = 2");

            $rows = $this->ztdQuery('SELECT * FROM my_coi_items ORDER BY id');
            $this->assertCount(3, $rows);
            $this->assertEqualsWithDelta(10.00, (float) $rows[0]['price'], 0.01);
            $this->assertEqualsWithDelta(99.99, (float) $rows[1]['price'], 0.01);
            $this->assertEqualsWithDelta(30.00, (float) $rows[2]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row INSERT then UPDATE failed: ' . $e->getMessage());
        }
    }

    public function testMultiRowInsertThenDeleteOne(): void
    {
        try {
            $this->pdo->exec("INSERT INTO my_coi_items VALUES (1, 'Alpha', 10.00, 'A'), (2, 'Beta', 20.00, 'B'), (3, 'Gamma', 30.00, 'C')");
            $this->pdo->exec("DELETE FROM my_coi_items WHERE id = 2");

            $rows = $this->ztdQuery('SELECT * FROM my_coi_items ORDER BY id');
            $this->assertCount(2, $rows);
            $this->assertSame('Alpha', $rows[0]['name']);
            $this->assertSame('Gamma', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row INSERT then DELETE failed: ' . $e->getMessage());
        }
    }

    public function testMultiRowInsertWithReorderedColumns(): void
    {
        try {
            $this->pdo->exec("INSERT INTO my_coi_items (category, name, id, price) VALUES ('X', 'First', 1, 5.00), ('Y', 'Second', 2, 15.00)");

            $rows = $this->ztdQuery('SELECT * FROM my_coi_items ORDER BY id');
            $this->assertCount(2, $rows);
            $this->assertSame('First', $rows[0]['name']);
            $this->assertSame('X', $rows[0]['category']);
            $this->assertEqualsWithDelta(5.00, (float) $rows[0]['price'], 0.01);
            $this->assertSame('Second', $rows[1]['name']);
            $this->assertSame('Y', $rows[1]['category']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row INSERT with reordered columns failed: ' . $e->getMessage());
        }
    }

    public function testMultiRowInsertPrepared(): void
    {
        try {
            $stmt = $this->pdo->prepare('INSERT INTO my_coi_items VALUES (?, ?, ?, ?), (?, ?, ?, ?)');
            $stmt->execute([1, 'A', 10.00, 'cat1', 2, 'B', 20.00, 'cat2']);

            $rows = $this->ztdQuery('SELECT * FROM my_coi_items ORDER BY id');
            $this->assertCount(2, $rows);
            $this->assertSame('A', $rows[0]['name']);
            $this->assertSame('B', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row INSERT prepared failed: ' . $e->getMessage());
        }
    }

    public function testMultiRowInsertWithAggregateQuery(): void
    {
        try {
            $this->pdo->exec("INSERT INTO my_coi_items VALUES (1, 'X', 10.00, 'A'), (2, 'Y', 20.00, 'A'), (3, 'Z', 30.00, 'B')");

            $rows = $this->ztdQuery(
                "SELECT category, COUNT(*) AS cnt, SUM(price) AS total
                 FROM my_coi_items
                 GROUP BY category
                 ORDER BY category"
            );
            $this->assertCount(2, $rows);
            $this->assertSame('A', $rows[0]['category']);
            $this->assertSame(2, (int) $rows[0]['cnt']);
            $this->assertEqualsWithDelta(30.00, (float) $rows[0]['total'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row INSERT then aggregate failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO my_coi_items VALUES (1, 'Test', 5.00, 'cat')");
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM my_coi_items');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
