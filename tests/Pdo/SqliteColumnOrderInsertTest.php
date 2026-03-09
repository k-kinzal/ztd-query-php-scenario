<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT with columns in non-DDL order through ZTD shadow store.
 *
 * Real applications often specify columns in INSERT in a different order
 * than the table definition. The shadow store must correctly map values
 * to columns regardless of the order specified in the INSERT.
 * @spec SPEC-4.1
 */
class SqliteColumnOrderInsertTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE coi_items (id INT PRIMARY KEY, name TEXT, price REAL, category TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['coi_items'];
    }

    /**
     * INSERT with columns in reverse order.
     */
    public function testReverseColumnOrder(): void
    {
        $this->pdo->exec("INSERT INTO coi_items (category, price, name, id) VALUES ('electronics', 9.99, 'Widget', 1)");

        $rows = $this->ztdQuery('SELECT * FROM coi_items WHERE id = 1');

        $this->assertCount(1, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertSame('9.99', (string) (float) $rows[0]['price']);
        $this->assertSame('electronics', $rows[0]['category']);
    }

    /**
     * INSERT with partial columns (omitting nullable ones).
     */
    public function testPartialColumns(): void
    {
        $this->pdo->exec("INSERT INTO coi_items (id, name) VALUES (2, 'Gadget')");

        $rows = $this->ztdQuery('SELECT * FROM coi_items WHERE id = 2');

        $this->assertCount(1, $rows);
        $this->assertSame('Gadget', $rows[0]['name']);
        $this->assertNull($rows[0]['price']);
        $this->assertNull($rows[0]['category']);
    }

    /**
     * INSERT with partial columns in non-standard order.
     */
    public function testPartialColumnsReordered(): void
    {
        $this->pdo->exec("INSERT INTO coi_items (category, id) VALUES ('books', 3)");

        $rows = $this->ztdQuery('SELECT * FROM coi_items WHERE id = 3');

        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['name']);
        $this->assertNull($rows[0]['price']);
        $this->assertSame('books', $rows[0]['category']);
    }

    /**
     * Multiple INSERTs with different column orderings, then SELECT all.
     */
    public function testMixedColumnOrders(): void
    {
        $this->pdo->exec("INSERT INTO coi_items (id, name, price, category) VALUES (1, 'A', 1.00, 'cat1')");
        $this->pdo->exec("INSERT INTO coi_items (category, id, name, price) VALUES ('cat2', 2, 'B', 2.00)");
        $this->pdo->exec("INSERT INTO coi_items (price, category, id, name) VALUES (3.00, 'cat3', 3, 'C')");

        $rows = $this->ztdQuery('SELECT * FROM coi_items ORDER BY id');

        $this->assertCount(3, $rows);
        $this->assertSame('A', $rows[0]['name']);
        $this->assertSame('B', $rows[1]['name']);
        $this->assertSame('C', $rows[2]['name']);
        $this->assertSame('cat1', $rows[0]['category']);
        $this->assertSame('cat2', $rows[1]['category']);
        $this->assertSame('cat3', $rows[2]['category']);
    }

    /**
     * Prepared statement with columns in different order.
     */
    public function testPreparedWithReorderedColumns(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO coi_items (price, id, name) VALUES (?, ?, ?)');
        $stmt->execute([19.99, 10, 'PrepItem']);

        $rows = $this->ztdQuery('SELECT * FROM coi_items WHERE id = 10');

        $this->assertCount(1, $rows);
        $this->assertSame('PrepItem', $rows[0]['name']);
        $this->assertSame('19.99', (string) (float) $rows[0]['price']);
    }

    /**
     * UPDATE after reordered INSERT.
     */
    public function testUpdateAfterReorderedInsert(): void
    {
        $this->pdo->exec("INSERT INTO coi_items (name, id, category) VALUES ('OldName', 5, 'cat')");
        $this->pdo->exec("UPDATE coi_items SET name = 'NewName' WHERE id = 5");

        $rows = $this->ztdQuery('SELECT * FROM coi_items WHERE id = 5');

        $this->assertCount(1, $rows);
        $this->assertSame('NewName', $rows[0]['name']);
        $this->assertSame('cat', $rows[0]['category']);
    }
}
