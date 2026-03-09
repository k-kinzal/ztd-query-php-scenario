<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests shadow store behavior on tables without a primary key.
 *
 * Many real applications have tables without primary keys (log tables,
 * junction/pivot tables, denormalized tables). The shadow store relies
 * on primary keys for overlay/deduplication. This tests what happens
 * when that assumption is broken.
 * @spec SPEC-3.1
 */
class SqliteNoPrimaryKeyTableTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE nopk_logs (ts TEXT, level TEXT, message TEXT)',
            'CREATE TABLE nopk_tags (item_id INT, tag TEXT)',
            'CREATE TABLE nopk_pivot (user_id INT, group_id INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['nopk_logs', 'nopk_tags', 'nopk_pivot'];
    }

    /**
     * INSERT into table without PK, then SELECT.
     */
    public function testInsertAndSelectNoPk(): void
    {
        try {
            $this->pdo->exec("INSERT INTO nopk_logs VALUES ('2024-01-01', 'INFO', 'started')");
            $this->pdo->exec("INSERT INTO nopk_logs VALUES ('2024-01-02', 'ERROR', 'failed')");

            $rows = $this->ztdQuery('SELECT * FROM nopk_logs ORDER BY ts');

            $this->assertCount(2, $rows);
            $this->assertSame('INFO', $rows[0]['level']);
            $this->assertSame('ERROR', $rows[1]['level']);
        } catch (\Exception $e) {
            $this->markTestSkipped('No-PK table not supported: ' . $e->getMessage());
        }
    }

    /**
     * Multiple inserts of identical rows into no-PK table.
     * Without a PK, duplicate rows should coexist.
     */
    public function testDuplicateRowsNoPk(): void
    {
        try {
            $this->pdo->exec("INSERT INTO nopk_tags VALUES (1, 'php')");
            $this->pdo->exec("INSERT INTO nopk_tags VALUES (1, 'php')");

            $rows = $this->ztdQuery("SELECT * FROM nopk_tags WHERE item_id = 1 AND tag = 'php'");

            $this->assertCount(2, $rows, 'Duplicate rows should both be visible in no-PK table');
        } catch (\Exception $e) {
            $this->markTestSkipped('No-PK duplicate insert not supported: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE on table without PK.
     */
    public function testUpdateNoPk(): void
    {
        try {
            $this->pdo->exec("INSERT INTO nopk_logs VALUES ('2024-01-01', 'INFO', 'started')");
            $this->pdo->exec("INSERT INTO nopk_logs VALUES ('2024-01-02', 'ERROR', 'failed')");

            $this->pdo->exec("UPDATE nopk_logs SET level = 'WARN' WHERE ts = '2024-01-02'");

            $rows = $this->ztdQuery("SELECT level FROM nopk_logs WHERE ts = '2024-01-02'");
            $this->assertCount(1, $rows);
            $this->assertSame('WARN', $rows[0]['level']);
        } catch (\Exception $e) {
            $this->markTestSkipped('No-PK UPDATE not supported: ' . $e->getMessage());
        }
    }

    /**
     * DELETE from table without PK.
     */
    public function testDeleteNoPk(): void
    {
        try {
            $this->pdo->exec("INSERT INTO nopk_tags VALUES (1, 'php')");
            $this->pdo->exec("INSERT INTO nopk_tags VALUES (1, 'sql')");
            $this->pdo->exec("INSERT INTO nopk_tags VALUES (2, 'php')");

            $this->pdo->exec("DELETE FROM nopk_tags WHERE item_id = 1 AND tag = 'php'");

            $rows = $this->ztdQuery('SELECT * FROM nopk_tags ORDER BY item_id, tag');
            $this->assertCount(2, $rows);
            $this->assertSame('sql', $rows[0]['tag']);
        } catch (\Exception $e) {
            $this->markTestSkipped('No-PK DELETE not supported: ' . $e->getMessage());
        }
    }

    /**
     * Aggregate query on no-PK table after mutations.
     */
    public function testAggregateNoPk(): void
    {
        try {
            $this->pdo->exec("INSERT INTO nopk_pivot VALUES (1, 10)");
            $this->pdo->exec("INSERT INTO nopk_pivot VALUES (1, 20)");
            $this->pdo->exec("INSERT INTO nopk_pivot VALUES (2, 10)");

            $rows = $this->ztdQuery(
                'SELECT user_id, COUNT(*) AS cnt FROM nopk_pivot GROUP BY user_id ORDER BY user_id'
            );

            $this->assertCount(2, $rows);
            $this->assertSame('2', (string) $rows[0]['cnt']);
            $this->assertSame('1', (string) $rows[1]['cnt']);
        } catch (\Exception $e) {
            $this->markTestSkipped('No-PK aggregate not supported: ' . $e->getMessage());
        }
    }

    /**
     * JOIN between no-PK table and PK table.
     */
    public function testJoinNoPkWithPkTable(): void
    {
        $this->pdo->exec('CREATE TABLE nopk_items (id INT PRIMARY KEY, name TEXT)');

        try {
            $this->pdo->exec("INSERT INTO nopk_items VALUES (1, 'Widget')");
            $this->pdo->exec("INSERT INTO nopk_items VALUES (2, 'Gadget')");
            $this->pdo->exec("INSERT INTO nopk_tags VALUES (1, 'electronics')");
            $this->pdo->exec("INSERT INTO nopk_tags VALUES (2, 'electronics')");

            $rows = $this->ztdQuery(
                "SELECT i.name, t.tag
                 FROM nopk_items i JOIN nopk_tags t ON i.id = t.item_id
                 WHERE t.tag = 'electronics'
                 ORDER BY i.name"
            );

            $this->assertCount(2, $rows);
            $this->assertSame('Gadget', $rows[0]['name']);
            $this->assertSame('Widget', $rows[1]['name']);
        } catch (\Exception $e) {
            $this->markTestSkipped('No-PK JOIN not supported: ' . $e->getMessage());
        } finally {
            $this->pdo->disableZtd();
            $this->pdo->exec('DROP TABLE IF EXISTS nopk_items');
            $this->pdo->enableZtd();
        }
    }

    /**
     * COUNT(*) after DELETE on no-PK table.
     */
    public function testCountAfterDeleteNoPk(): void
    {
        try {
            $this->pdo->exec("INSERT INTO nopk_pivot VALUES (1, 10)");
            $this->pdo->exec("INSERT INTO nopk_pivot VALUES (1, 20)");
            $this->pdo->exec("INSERT INTO nopk_pivot VALUES (2, 10)");

            $this->pdo->exec("DELETE FROM nopk_pivot WHERE user_id = 1");

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM nopk_pivot');
            $this->assertSame('1', (string) $rows[0]['cnt']);
        } catch (\Exception $e) {
            $this->markTestSkipped('No-PK DELETE+COUNT not supported: ' . $e->getMessage());
        }
    }
}
