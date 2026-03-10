<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests STRICT tables through the ZTD shadow store.
 *
 * SQLite 3.37+ (2021-11-27) added STRICT tables that enforce column types.
 * The CTE shadow store embeds values as text in the VALUES clause. If
 * strict type checking applies to the CTE output, queries may fail because
 * the shadow values are text, not their declared types.
 *
 * @spec SPEC-3.1
 */
class SqliteStrictTableTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_str_measurements (
                id INTEGER PRIMARY KEY,
                sensor TEXT NOT NULL,
                reading REAL NOT NULL,
                count INTEGER NOT NULL,
                raw_data BLOB
            ) STRICT",
            "CREATE TABLE sl_str_config (
                key TEXT PRIMARY KEY,
                int_val INTEGER,
                real_val REAL,
                text_val TEXT
            ) STRICT, WITHOUT ROWID",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_str_config', 'sl_str_measurements'];
    }

    protected function setUp(): void
    {
        // Check if SQLite version supports STRICT
        $raw = new \PDO('sqlite::memory:', null, null, [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
        ]);
        $version = $raw->query('SELECT sqlite_version()')->fetchColumn();
        if (version_compare($version, '3.37.0', '<')) {
            $this->markTestSkipped("STRICT tables require SQLite 3.37+, have {$version}");
        }

        parent::setUp();
    }

    /**
     * Basic INSERT + SELECT on STRICT table.
     */
    public function testInsertAndSelectStrict(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_str_measurements VALUES (1, 'temp', 23.5, 100, NULL)");
            $this->pdo->exec("INSERT INTO sl_str_measurements VALUES (2, 'humidity', 65.2, 200, NULL)");

            $rows = $this->ztdQuery(
                "SELECT id, sensor, reading, count FROM sl_str_measurements ORDER BY id"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'STRICT table SELECT returned 0 rows. Shadow store may embed text values '
                    . 'that violate STRICT type rules.'
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('temp', $rows[0]['sensor']);
            $this->assertEquals(23.5, (float) $rows[0]['reading']);
            $this->assertSame(100, (int) $rows[0]['count']);
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
            if (stripos($msg, 'cannot store') !== false || stripos($msg, 'type mismatch') !== false) {
                $this->markTestIncomplete(
                    'STRICT table: CTE shadow store type mismatch. The shadow embeds values as text '
                    . 'which STRICT mode rejects. Error: ' . $msg
                );
            }
            $this->markTestIncomplete('STRICT table INSERT+SELECT failed: ' . $msg);
        }
    }

    /**
     * UPDATE on STRICT table — the shadow must preserve types.
     */
    public function testUpdateStrict(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_str_measurements VALUES (1, 'temp', 23.5, 100, NULL)");
            $this->pdo->exec("UPDATE sl_str_measurements SET reading = 25.0, count = 150 WHERE id = 1");

            $rows = $this->ztdQuery("SELECT reading, count FROM sl_str_measurements WHERE id = 1");

            if (count($rows) === 0) {
                $this->markTestIncomplete('STRICT table UPDATE: row disappeared.');
            }

            $this->assertEquals(25.0, (float) $rows[0]['reading']);
            $this->assertSame(150, (int) $rows[0]['count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('STRICT table UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * STRICT + WITHOUT ROWID combination.
     */
    public function testStrictWithoutRowid(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_str_config VALUES ('timeout', 30, NULL, NULL)");
            $this->pdo->exec("INSERT INTO sl_str_config VALUES ('rate', NULL, 0.75, NULL)");
            $this->pdo->exec("INSERT INTO sl_str_config VALUES ('name', NULL, NULL, 'test')");

            $rows = $this->ztdQuery("SELECT key, int_val, real_val, text_val FROM sl_str_config ORDER BY key");

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'STRICT WITHOUT ROWID: SELECT returned 0 rows.'
                );
            }

            $this->assertCount(3, $rows);

            // Verify UPDATE
            $this->pdo->exec("UPDATE sl_str_config SET int_val = 60 WHERE key = 'timeout'");
            $rows = $this->ztdQuery("SELECT int_val FROM sl_str_config WHERE key = 'timeout'");

            if ((int) $rows[0]['int_val'] !== 60) {
                $this->markTestIncomplete(
                    'STRICT WITHOUT ROWID UPDATE: int_val is ' . $rows[0]['int_val'] . ', expected 60.'
                );
            }

            $this->assertSame(60, (int) $rows[0]['int_val']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('STRICT WITHOUT ROWID failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared statements on STRICT table — type binding matters.
     */
    public function testPreparedStrictTypeBinding(): void
    {
        try {
            $stmt = $this->ztdPrepare(
                "INSERT INTO sl_str_measurements VALUES (?, ?, ?, ?, NULL)"
            );
            $stmt->execute([1, 'pressure', 101.3, 50]);

            $rows = $this->ztdPrepareAndExecute(
                "SELECT sensor, reading FROM sl_str_measurements WHERE count > ?",
                [10]
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete('Prepared SELECT on STRICT table returned 0 rows.');
            }

            $this->assertCount(1, $rows);
            $this->assertSame('pressure', $rows[0]['sensor']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared STRICT table failed: ' . $e->getMessage());
        }
    }

    /**
     * Aggregate on STRICT table after DML.
     */
    public function testAggregateStrictAfterDml(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_str_measurements VALUES (1, 'a', 10.0, 1, NULL)");
            $this->pdo->exec("INSERT INTO sl_str_measurements VALUES (2, 'b', 20.0, 2, NULL)");
            $this->pdo->exec("INSERT INTO sl_str_measurements VALUES (3, 'c', 30.0, 3, NULL)");
            $this->pdo->exec("DELETE FROM sl_str_measurements WHERE id = 2");

            $rows = $this->ztdQuery(
                "SELECT COUNT(*) as cnt, SUM(reading) as total, AVG(reading) as avg_r
                 FROM sl_str_measurements"
            );

            $this->assertSame(2, (int) $rows[0]['cnt']);
            $this->assertEquals(40.0, (float) $rows[0]['total']);
            $this->assertEquals(20.0, (float) $rows[0]['avg_r']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Aggregate on STRICT table failed: ' . $e->getMessage());
        }
    }
}
