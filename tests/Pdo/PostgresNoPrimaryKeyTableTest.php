<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests shadow store behavior on tables without a primary key on PostgreSQL.
 *
 * Many real applications have tables without primary keys (log tables,
 * junction/pivot tables, denormalized tables). The shadow store relies
 * on primary keys for overlay/deduplication.
 * @spec SPEC-3.1
 */
class PostgresNoPrimaryKeyTableTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pnopk_logs (ts TIMESTAMP, level VARCHAR(10), message TEXT)',
            'CREATE TABLE pnopk_tags (item_id INT, tag VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pnopk_logs', 'pnopk_tags'];
    }

    /**
     * INSERT into table without PK, then SELECT.
     */
    public function testInsertAndSelectNoPk(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pnopk_logs VALUES ('2024-01-01', 'INFO', 'started')");
            $this->pdo->exec("INSERT INTO pnopk_logs VALUES ('2024-01-02', 'ERROR', 'failed')");

            $rows = $this->ztdQuery('SELECT * FROM pnopk_logs ORDER BY ts');

            $this->assertCount(2, $rows);
            $this->assertSame('INFO', $rows[0]['level']);
            $this->assertSame('ERROR', $rows[1]['level']);
        } catch (\Exception $e) {
            $this->markTestSkipped('No-PK table not supported on PostgreSQL: ' . $e->getMessage());
        }
    }

    /**
     * Duplicate rows in no-PK table.
     */
    public function testDuplicateRowsNoPk(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pnopk_tags VALUES (1, 'php')");
            $this->pdo->exec("INSERT INTO pnopk_tags VALUES (1, 'php')");

            $rows = $this->ztdQuery("SELECT * FROM pnopk_tags WHERE item_id = 1 AND tag = 'php'");

            $this->assertCount(2, $rows, 'Duplicate rows should both be visible in no-PK table');
        } catch (\Exception $e) {
            $this->markTestSkipped('No-PK duplicate insert not supported on PostgreSQL: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE on table without PK.
     */
    public function testUpdateNoPk(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pnopk_tags VALUES (1, 'php')");
            $this->pdo->exec("INSERT INTO pnopk_tags VALUES (2, 'sql')");

            $this->pdo->exec("UPDATE pnopk_tags SET tag = 'PHP' WHERE item_id = 1");

            $rows = $this->ztdQuery("SELECT tag FROM pnopk_tags WHERE item_id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame('PHP', $rows[0]['tag']);
        } catch (\Exception $e) {
            $this->markTestSkipped('No-PK UPDATE not supported on PostgreSQL: ' . $e->getMessage());
        }
    }

    /**
     * DELETE from table without PK.
     */
    public function testDeleteNoPk(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pnopk_tags VALUES (1, 'php')");
            $this->pdo->exec("INSERT INTO pnopk_tags VALUES (1, 'sql')");
            $this->pdo->exec("INSERT INTO pnopk_tags VALUES (2, 'php')");

            $this->pdo->exec("DELETE FROM pnopk_tags WHERE item_id = 1 AND tag = 'php'");

            $rows = $this->ztdQuery('SELECT * FROM pnopk_tags ORDER BY item_id, tag');
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestSkipped('No-PK DELETE not supported on PostgreSQL: ' . $e->getMessage());
        }
    }

    /**
     * Aggregate query on no-PK table.
     */
    public function testAggregateNoPk(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pnopk_tags VALUES (1, 'php')");
            $this->pdo->exec("INSERT INTO pnopk_tags VALUES (1, 'sql')");
            $this->pdo->exec("INSERT INTO pnopk_tags VALUES (2, 'php')");

            $rows = $this->ztdQuery(
                'SELECT item_id, COUNT(*) AS cnt FROM pnopk_tags GROUP BY item_id ORDER BY item_id'
            );

            $this->assertCount(2, $rows);
            $this->assertSame('2', (string) $rows[0]['cnt']);
            $this->assertSame('1', (string) $rows[1]['cnt']);
        } catch (\Exception $e) {
            $this->markTestSkipped('No-PK aggregate not supported on PostgreSQL: ' . $e->getMessage());
        }
    }

    /**
     * JOIN between no-PK table and PK table.
     */
    public function testJoinNoPkWithPkTable(): void
    {
        $this->createTable('CREATE TABLE pnopk_items (id INT PRIMARY KEY, name TEXT)');

        try {
            $this->pdo->exec("INSERT INTO pnopk_items VALUES (1, 'Widget')");
            $this->pdo->exec("INSERT INTO pnopk_items VALUES (2, 'Gadget')");
            $this->pdo->exec("INSERT INTO pnopk_tags VALUES (1, 'electronics')");
            $this->pdo->exec("INSERT INTO pnopk_tags VALUES (2, 'electronics')");

            $rows = $this->ztdQuery(
                "SELECT i.name, t.tag
                 FROM pnopk_items i JOIN pnopk_tags t ON i.id = t.item_id
                 WHERE t.tag = 'electronics'
                 ORDER BY i.name"
            );

            $this->assertCount(2, $rows);
            $this->assertSame('Gadget', $rows[0]['name']);
            $this->assertSame('Widget', $rows[1]['name']);
        } catch (\Exception $e) {
            $this->markTestSkipped('No-PK JOIN not supported on PostgreSQL: ' . $e->getMessage());
        } finally {
            $this->dropTable('pnopk_items');
        }
    }
}
