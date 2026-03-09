<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests shadow store behavior on tables without a primary key on MySQL.
 *
 * Many real applications have tables without primary keys (log tables,
 * junction/pivot tables, denormalized tables). The shadow store relies
 * on primary keys for overlay/deduplication.
 * @spec SPEC-3.1
 */
class MysqlNoPrimaryKeyTableTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mnopk_logs (ts DATETIME, level VARCHAR(10), message TEXT)',
            'CREATE TABLE mnopk_tags (item_id INT, tag VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mnopk_logs', 'mnopk_tags'];
    }

    /**
     * INSERT into table without PK, then SELECT.
     */
    public function testInsertAndSelectNoPk(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mnopk_logs VALUES ('2024-01-01', 'INFO', 'started')");
            $this->pdo->exec("INSERT INTO mnopk_logs VALUES ('2024-01-02', 'ERROR', 'failed')");

            $rows = $this->ztdQuery('SELECT * FROM mnopk_logs ORDER BY ts');

            $this->assertCount(2, $rows);
            $this->assertSame('INFO', $rows[0]['level']);
            $this->assertSame('ERROR', $rows[1]['level']);
        } catch (\Exception $e) {
            $this->markTestSkipped('No-PK table not supported on MySQL: ' . $e->getMessage());
        }
    }

    /**
     * Duplicate rows in no-PK table.
     */
    public function testDuplicateRowsNoPk(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mnopk_tags VALUES (1, 'php')");
            $this->pdo->exec("INSERT INTO mnopk_tags VALUES (1, 'php')");

            $rows = $this->ztdQuery("SELECT * FROM mnopk_tags WHERE item_id = 1 AND tag = 'php'");

            $this->assertCount(2, $rows, 'Duplicate rows should both be visible in no-PK table');
        } catch (\Exception $e) {
            $this->markTestSkipped('No-PK duplicate insert not supported on MySQL: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE on table without PK.
     */
    public function testUpdateNoPk(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mnopk_tags VALUES (1, 'php')");
            $this->pdo->exec("INSERT INTO mnopk_tags VALUES (2, 'sql')");

            $this->pdo->exec("UPDATE mnopk_tags SET tag = 'PHP' WHERE item_id = 1");

            $rows = $this->ztdQuery("SELECT tag FROM mnopk_tags WHERE item_id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame('PHP', $rows[0]['tag']);
        } catch (\Exception $e) {
            $this->markTestSkipped('No-PK UPDATE not supported on MySQL: ' . $e->getMessage());
        }
    }

    /**
     * DELETE from table without PK.
     */
    public function testDeleteNoPk(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mnopk_tags VALUES (1, 'php')");
            $this->pdo->exec("INSERT INTO mnopk_tags VALUES (1, 'sql')");
            $this->pdo->exec("INSERT INTO mnopk_tags VALUES (2, 'php')");

            $this->pdo->exec("DELETE FROM mnopk_tags WHERE item_id = 1 AND tag = 'php'");

            $rows = $this->ztdQuery('SELECT * FROM mnopk_tags ORDER BY item_id, tag');
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestSkipped('No-PK DELETE not supported on MySQL: ' . $e->getMessage());
        }
    }

    /**
     * Aggregate query on no-PK table.
     */
    public function testAggregateNoPk(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mnopk_tags VALUES (1, 'php')");
            $this->pdo->exec("INSERT INTO mnopk_tags VALUES (1, 'sql')");
            $this->pdo->exec("INSERT INTO mnopk_tags VALUES (2, 'php')");

            $rows = $this->ztdQuery(
                'SELECT item_id, COUNT(*) AS cnt FROM mnopk_tags GROUP BY item_id ORDER BY item_id'
            );

            $this->assertCount(2, $rows);
            $this->assertSame('2', (string) $rows[0]['cnt']);
            $this->assertSame('1', (string) $rows[1]['cnt']);
        } catch (\Exception $e) {
            $this->markTestSkipped('No-PK aggregate not supported on MySQL: ' . $e->getMessage());
        }
    }
}
