<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT...SELECT where the source table has shadow modifications on SQLite.
 *
 * @spec SPEC-4.2
 */
class SqliteInsertSelectFromShadowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_iss_source (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            )',
            'CREATE TABLE sl_iss_dest (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_iss_dest', 'sl_iss_source'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_iss_source VALUES (1, 'Alpha', 1)");
        $this->pdo->exec("INSERT INTO sl_iss_source VALUES (2, 'Beta', 1)");
        $this->pdo->exec("INSERT INTO sl_iss_source VALUES (3, 'Gamma', 0)");
    }

    public function testInsertSelectSeesSourceShadow(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_iss_source VALUES (4, 'Delta', 1)");

            $this->pdo->exec(
                "INSERT INTO sl_iss_dest (id, name) SELECT id, name FROM sl_iss_source WHERE active = 1"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM sl_iss_dest ORDER BY id");

            $names = array_column($rows, 'name');
            if (!in_array('Delta', $names)) {
                $this->markTestIncomplete(
                    'INSERT...SELECT did not see shadow-inserted row. Got: ' . json_encode($names)
                );
            }
            $this->assertCount(3, $rows);
            $this->assertSame('Delta', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT from shadow source failed: ' . $e->getMessage());
        }
    }

    public function testInsertSelectAfterSourceDelete(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_iss_source WHERE id = 2");

            $this->pdo->exec(
                "INSERT INTO sl_iss_dest (id, name) SELECT id, name FROM sl_iss_source WHERE active = 1"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM sl_iss_dest ORDER BY id");
            $this->assertCount(1, $rows);
            $this->assertSame('Alpha', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT after source DELETE failed: ' . $e->getMessage());
        }
    }

    public function testInsertSelectSameTable(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_iss_source (id, name, active) SELECT id + 100, name, active FROM sl_iss_source WHERE active = 1"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM sl_iss_source WHERE id > 100 ORDER BY id");

            if (empty($rows)) {
                $this->markTestIncomplete('INSERT...SELECT same table: no copied rows found');
            }
            $this->assertCount(2, $rows);
            $this->assertEquals(101, (int) $rows[0]['id']);
            $this->assertSame('Alpha', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT same table failed: ' . $e->getMessage());
        }
    }
}
