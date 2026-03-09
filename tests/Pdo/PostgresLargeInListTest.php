<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests large IN lists, many-row operations, and batch patterns through ZTD shadow store (PostgreSQL PDO).
 * Covers large numeric IN, UPDATE with IN, DELETE with IN, NOT IN, string IN,
 * prepared statements with multiple params, and physical isolation.
 * @spec SPEC-10.2.91
 */
class PostgresLargeInListTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_il_items (
                id INTEGER PRIMARY KEY,
                name VARCHAR(255),
                category VARCHAR(50),
                price NUMERIC(8,2),
                status VARCHAR(20)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_il_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $categories = ['Electronics', 'Books', 'Toys', 'Clothing', 'Food'];
        $statuses = ['active', 'active', 'active', 'discontinued'];

        for ($i = 1; $i <= 30; $i++) {
            $cat = $categories[($i - 1) % count($categories)];
            $status = $statuses[($i - 1) % count($statuses)];
            $price = 10.00 + ($i * 5.50);
            $this->pdo->exec(sprintf(
                "INSERT INTO pg_il_items VALUES (%d, 'Item %d', '%s', %.2f, '%s')",
                $i, $i, $cat, $price, $status
            ));
        }
    }

    /**
     * SELECT with a large IN list: verify correct count and content.
     */
    public function testSelectWithLargeInList(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name FROM pg_il_items WHERE id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20) ORDER BY id"
        );

        $this->assertCount(20, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertEquals(20, (int) $rows[19]['id']);
        $this->assertSame('Item 1', $rows[0]['name']);
        $this->assertSame('Item 20', $rows[19]['name']);
    }

    /**
     * UPDATE with IN list: set status to 'sale' for selected items.
     */
    public function testUpdateWithInList(): void
    {
        $affected = $this->pdo->exec(
            "UPDATE pg_il_items SET status = 'sale' WHERE id IN (1,3,5,7,9,11,13,15)"
        );
        $this->assertSame(8, $affected);

        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM pg_il_items WHERE status = 'sale'"
        );
        $this->assertEquals(8, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT status FROM pg_il_items WHERE id = 7");
        $this->assertSame('sale', $rows[0]['status']);
    }

    /**
     * DELETE with IN list: remove selected items and verify remaining count.
     */
    public function testDeleteWithInList(): void
    {
        $affected = $this->pdo->exec(
            "DELETE FROM pg_il_items WHERE id IN (2,4,6,8,10)"
        );
        $this->assertSame(5, $affected);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_il_items");
        $this->assertEquals(25, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT id FROM pg_il_items WHERE id IN (2,4,6,8,10)");
        $this->assertCount(0, $rows);
    }

    /**
     * NOT IN filter: exclude a set of IDs and verify exclusion works.
     */
    public function testNotInFilter(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id FROM pg_il_items WHERE id NOT IN (1,2,3,4,5) ORDER BY id"
        );

        $this->assertCount(25, $rows);
        $this->assertEquals(6, (int) $rows[0]['id']);
        $this->assertEquals(30, (int) $rows[24]['id']);
    }

    /**
     * IN list with string values: filter by category names.
     */
    public function testInListWithStrings(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, category FROM pg_il_items WHERE category IN ('Electronics', 'Books', 'Toys') ORDER BY id"
        );

        $this->assertCount(18, $rows);

        foreach ($rows as $row) {
            $this->assertContains($row['category'], ['Electronics', 'Books', 'Toys']);
        }
    }

    /**
     * Prepared statement with multiple parameters: category, price, and status.
     */
    public function testPreparedWithMultipleParams(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, name, category, price, status FROM pg_il_items WHERE category = ? AND price > ? AND status = ? ORDER BY id",
            ['Electronics', 50.00, 'active']
        );

        // Electronics items with price > 50 and status active: ids 11, 21, 26
        $this->assertCount(3, $rows);
        $this->assertEquals(11, (int) $rows[0]['id']);
        $this->assertEquals(21, (int) $rows[1]['id']);
        $this->assertEquals(26, (int) $rows[2]['id']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("UPDATE pg_il_items SET status = 'sale' WHERE id IN (1,2,3)");
        $this->pdo->exec("DELETE FROM pg_il_items WHERE id IN (28,29,30)");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_il_items WHERE status = 'sale'");
        $this->assertEquals(3, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_il_items");
        $this->assertEquals(27, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM pg_il_items')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
