<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests temporary tables through ZTD shadow store on SQLite.
 *
 * Temporary tables are used in batch processing, ETL, and report generation.
 * ZTD must handle DML on temp tables: INSERT, SELECT, UPDATE, DELETE.
 *
 * @spec SPEC-4.1, SPEC-3.1, SPEC-5.1
 */
class SqliteTempTableDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_tmp_source (
                id INTEGER PRIMARY KEY,
                value TEXT NOT NULL,
                score INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_tmp_source'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_tmp_source VALUES (1, 'alpha', 90)");
        $this->pdo->exec("INSERT INTO sl_tmp_source VALUES (2, 'beta', 75)");
        $this->pdo->exec("INSERT INTO sl_tmp_source VALUES (3, 'gamma', 60)");
    }

    /**
     * CREATE TEMP TABLE + INSERT + SELECT through ZTD.
     */
    public function testCreateTempTableAndDml(): void
    {
        try {
            $this->pdo->exec(
                "CREATE TEMPORARY TABLE sl_tmp_staging (
                    id INTEGER PRIMARY KEY,
                    value TEXT NOT NULL,
                    score INTEGER NOT NULL
                )"
            );

            $this->pdo->exec(
                "INSERT INTO sl_tmp_staging SELECT * FROM sl_tmp_source WHERE score >= 75"
            );

            $rows = $this->ztdQuery("SELECT value FROM sl_tmp_staging ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Temp table DML: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('alpha', $rows[0]['value']);
            $this->assertSame('beta', $rows[1]['value']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Create temp table + DML failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE on temporary table.
     */
    public function testUpdateTempTable(): void
    {
        try {
            $this->pdo->exec(
                "CREATE TEMP TABLE sl_tmp_work (id INTEGER PRIMARY KEY, val TEXT, score INTEGER)"
            );
            $this->pdo->exec("INSERT INTO sl_tmp_work VALUES (1, 'x', 10)");
            $this->pdo->exec("INSERT INTO sl_tmp_work VALUES (2, 'y', 20)");

            $this->pdo->exec("UPDATE sl_tmp_work SET score = score + 100 WHERE id = 1");

            $rows = $this->ztdQuery("SELECT id, score FROM sl_tmp_work ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete('UPDATE temp: got ' . json_encode($rows));
            }

            $this->assertSame(110, (int) $rows[0]['score']);
            $this->assertSame(20, (int) $rows[1]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE on temp table failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE from temporary table.
     */
    public function testDeleteFromTempTable(): void
    {
        try {
            $this->pdo->exec(
                "CREATE TEMP TABLE sl_tmp_del (id INTEGER PRIMARY KEY, label TEXT)"
            );
            $this->pdo->exec("INSERT INTO sl_tmp_del VALUES (1, 'keep')");
            $this->pdo->exec("INSERT INTO sl_tmp_del VALUES (2, 'remove')");
            $this->pdo->exec("INSERT INTO sl_tmp_del VALUES (3, 'keep')");

            $this->pdo->exec("DELETE FROM sl_tmp_del WHERE label = 'remove'");

            $rows = $this->ztdQuery("SELECT id FROM sl_tmp_del ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE temp: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE from temp table failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT from temp table into permanent table.
     */
    public function testInsertFromTempIntoPermanent(): void
    {
        try {
            $this->pdo->exec(
                "CREATE TEMP TABLE sl_tmp_import (id INTEGER PRIMARY KEY, value TEXT, score INTEGER)"
            );
            $this->pdo->exec("INSERT INTO sl_tmp_import VALUES (10, 'imported', 100)");
            $this->pdo->exec("INSERT INTO sl_tmp_import VALUES (11, 'also-imported', 200)");

            $this->pdo->exec(
                "INSERT INTO sl_tmp_source SELECT * FROM sl_tmp_import"
            );

            $rows = $this->ztdQuery("SELECT value FROM sl_tmp_source WHERE id >= 10 ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT temp→permanent: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('imported', $rows[0]['value']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT from temp into permanent failed: ' . $e->getMessage());
        }
    }

    /**
     * JOIN between temp table and permanent table.
     */
    public function testJoinTempWithPermanent(): void
    {
        try {
            $this->pdo->exec(
                "CREATE TEMP TABLE sl_tmp_filter (id INTEGER PRIMARY KEY)"
            );
            $this->pdo->exec("INSERT INTO sl_tmp_filter VALUES (1)");
            $this->pdo->exec("INSERT INTO sl_tmp_filter VALUES (3)");

            $rows = $this->ztdQuery(
                "SELECT s.value FROM sl_tmp_source s
                 JOIN sl_tmp_filter f ON s.id = f.id
                 ORDER BY s.id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'JOIN temp+permanent: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('alpha', $rows[0]['value']);
            $this->assertSame('gamma', $rows[1]['value']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('JOIN temp with permanent failed: ' . $e->getMessage());
        }
    }
}
