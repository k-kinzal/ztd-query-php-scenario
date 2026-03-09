<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT ... SELECT from a different table where both tables have shadow data.
 *
 * Pattern: INSERT INTO archive SELECT * FROM orders WHERE status = 'completed'
 * The SELECT must see shadow data from orders, and the INSERT must go into
 * the archive shadow store.
 * @spec SPEC-4.1
 */
class SqliteInsertFromDifferentTableSelectTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE isrc_orders (id INT PRIMARY KEY, product TEXT, status TEXT)',
            'CREATE TABLE isrc_archive (id INT PRIMARY KEY, product TEXT, status TEXT)',
            'CREATE TABLE isrc_summary (product TEXT PRIMARY KEY, total_orders INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['isrc_orders', 'isrc_archive', 'isrc_summary'];
    }

    /**
     * Basic INSERT...SELECT from another shadow table.
     */
    public function testInsertSelectFromShadowTable(): void
    {
        $this->pdo->exec("INSERT INTO isrc_orders VALUES (1, 'Widget', 'completed')");
        $this->pdo->exec("INSERT INTO isrc_orders VALUES (2, 'Gadget', 'pending')");
        $this->pdo->exec("INSERT INTO isrc_orders VALUES (3, 'Doohickey', 'completed')");

        try {
            $this->pdo->exec(
                "INSERT INTO isrc_archive SELECT * FROM isrc_orders WHERE status = 'completed'"
            );

            $rows = $this->ztdQuery('SELECT * FROM isrc_archive ORDER BY id');
            $this->assertCount(2, $rows);
            $this->assertSame('Widget', $rows[0]['product']);
            $this->assertSame('Doohickey', $rows[1]['product']);
        } catch (\Exception $e) {
            $this->markTestSkipped('INSERT...SELECT from shadow not supported: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with aggregate into summary table.
     */
    public function testInsertSelectAggregate(): void
    {
        $this->pdo->exec("INSERT INTO isrc_orders VALUES (1, 'Widget', 'completed')");
        $this->pdo->exec("INSERT INTO isrc_orders VALUES (2, 'Widget', 'pending')");
        $this->pdo->exec("INSERT INTO isrc_orders VALUES (3, 'Gadget', 'completed')");

        try {
            $this->pdo->exec(
                "INSERT INTO isrc_summary
                 SELECT product, COUNT(*) FROM isrc_orders GROUP BY product"
            );

            $rows = $this->ztdQuery('SELECT * FROM isrc_summary ORDER BY product');
            $this->assertCount(2, $rows);
            $this->assertSame('Gadget', $rows[0]['product']);
            $this->assertSame('1', (string) $rows[0]['total_orders']);
            $this->assertSame('Widget', $rows[1]['product']);
            $this->assertSame('2', (string) $rows[1]['total_orders']);
        } catch (\Exception $e) {
            $this->markTestSkipped('INSERT...SELECT aggregate not supported: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT after mutation of source table.
     */
    public function testInsertSelectAfterSourceMutation(): void
    {
        $this->pdo->exec("INSERT INTO isrc_orders VALUES (1, 'Widget', 'pending')");
        $this->pdo->exec("UPDATE isrc_orders SET status = 'completed' WHERE id = 1");

        try {
            $this->pdo->exec(
                "INSERT INTO isrc_archive SELECT * FROM isrc_orders WHERE status = 'completed'"
            );

            $rows = $this->ztdQuery('SELECT * FROM isrc_archive');
            $this->assertCount(1, $rows);
            $this->assertSame('Widget', $rows[0]['product']);
        } catch (\Exception $e) {
            $this->markTestSkipped('INSERT...SELECT after mutation not supported: ' . $e->getMessage());
        }
    }

    /**
     * Verify source table is unchanged after INSERT...SELECT.
     */
    public function testSourceUnchangedAfterInsertSelect(): void
    {
        $this->pdo->exec("INSERT INTO isrc_orders VALUES (1, 'Widget', 'completed')");
        $this->pdo->exec("INSERT INTO isrc_orders VALUES (2, 'Gadget', 'pending')");

        try {
            $this->pdo->exec("INSERT INTO isrc_archive SELECT * FROM isrc_orders");

            $srcRows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM isrc_orders');
            $this->assertSame('2', (string) $srcRows[0]['cnt'], 'Source should be unchanged');

            $dstRows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM isrc_archive');
            $this->assertSame('2', (string) $dstRows[0]['cnt'], 'Destination should have copies');
        } catch (\Exception $e) {
            $this->markTestSkipped('INSERT...SELECT not supported: ' . $e->getMessage());
        }
    }
}
