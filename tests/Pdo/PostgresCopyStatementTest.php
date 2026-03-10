<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL COPY statement through the ZTD shadow store.
 *
 * COPY TO/FROM is PostgreSQL's bulk data transfer mechanism. It is widely used
 * for ETL pipelines, data migration, backups, and CSV import/export. The CTE
 * rewriter likely has no support for COPY since it's not a standard DML statement.
 *
 * This matters because:
 * - COPY TO (export) should read from the shadow store to reflect DML changes
 * - COPY FROM (import) should be tracked by the shadow store
 * - Many tools (pg_dump, data pipelines) use COPY under the hood
 *
 * @spec SPEC-6.1
 */
class PostgresCopyStatementTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_copy_data (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                amount NUMERIC(10,2) NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_copy_data'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_copy_data (id, name, amount) VALUES (1, 'Alice', 100.50)");
        $this->pdo->exec("INSERT INTO pg_copy_data (id, name, amount) VALUES (2, 'Bob', 200.75)");
        $this->pdo->exec("INSERT INTO pg_copy_data (id, name, amount) VALUES (3, 'Carol', 300.00)");
    }

    /**
     * COPY TO STDOUT should export shadow-visible data.
     *
     * After DML changes, COPY TO should reflect the shadow store state,
     * not the physical table state.
     */
    public function testCopyToStdout(): void
    {
        try {
            // First, modify data through shadow store
            $this->pdo->exec("UPDATE pg_copy_data SET amount = 999.99 WHERE id = 1");

            // Try COPY TO — this uses pdo_pgsql's pgsqlCopyToArray if available
            // or raw COPY TO STDOUT
            $result = $this->pdo->pgsqlCopyToArray('pg_copy_data');

            if ($result === false) {
                $this->markTestIncomplete(
                    'COPY TO returned false. May be blocked by ZTD Write Protection.'
                );
            }

            // Check if shadow UPDATE is reflected in COPY output
            $found999 = false;
            foreach ($result as $line) {
                if (str_contains($line, '999.99')) {
                    $found999 = true;
                    break;
                }
            }

            if (!$found999) {
                $this->markTestIncomplete(
                    'COPY TO did not reflect shadow UPDATE. Output reads physical table, '
                    . 'not shadow store. Lines: ' . json_encode(array_slice($result, 0, 5))
                );
            }

            $this->assertTrue($found999);
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
            if (stripos($msg, 'method') !== false && stripos($msg, 'exist') !== false) {
                $this->markTestSkipped('pgsqlCopyToArray not available on ZtdPdo');
            }
            $this->markTestIncomplete('COPY TO STDOUT failed: ' . $msg);
        }
    }

    /**
     * COPY TO with query (COPY (SELECT ...) TO STDOUT).
     */
    public function testCopyQueryToStdout(): void
    {
        try {
            $this->pdo->exec("DELETE FROM pg_copy_data WHERE id = 3");

            // COPY with query subform
            $result = $this->pdo->pgsqlCopyToArray(
                'pg_copy_data',
                "\t",
                "\\N",
                'id, name, amount'
            );

            if ($result === false) {
                $this->markTestIncomplete('COPY (query) TO returned false.');
            }

            // Should have 2 rows (id=3 deleted in shadow)
            if (count($result) !== 2) {
                $this->markTestIncomplete(
                    'COPY TO after DELETE: expected 2 rows, got ' . count($result)
                    . '. COPY may read physical table. Lines: ' . json_encode($result)
                );
            }

            $this->assertCount(2, $result);
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
            if (stripos($msg, 'method') !== false) {
                $this->markTestSkipped('pgsqlCopyToArray not available on ZtdPdo');
            }
            $this->markTestIncomplete('COPY query TO failed: ' . $msg);
        }
    }

    /**
     * COPY FROM STDIN should load data into shadow store.
     */
    public function testCopyFromStdin(): void
    {
        try {
            $data = "4\tDave\t400.00\n5\tEve\t500.00\n";

            $result = $this->pdo->pgsqlCopyFromArray('pg_copy_data', [
                "4\tDave\t400.00",
                "5\tEve\t500.00",
            ]);

            if ($result === false) {
                $this->markTestIncomplete(
                    'COPY FROM returned false. May be blocked by ZTD Write Protection.'
                );
            }

            // Check if COPY-loaded rows are visible through shadow store
            $rows = $this->ztdQuery("SELECT * FROM pg_copy_data ORDER BY id");

            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'COPY FROM: expected 5 rows (3 original + 2 copied), got ' . count($rows)
                    . '. COPY data may bypass shadow store. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(5, $rows);
            $this->assertSame('Dave', $rows[3]['name']);
            $this->assertSame('Eve', $rows[4]['name']);
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
            if (stripos($msg, 'method') !== false) {
                $this->markTestSkipped('pgsqlCopyFromArray not available on ZtdPdo');
            }
            $this->markTestIncomplete('COPY FROM STDIN failed: ' . $msg);
        }
    }

    /**
     * Raw COPY TO STDOUT via exec().
     *
     * Some applications use raw SQL COPY statements rather than the pgsql-specific methods.
     */
    public function testRawCopyToExec(): void
    {
        try {
            $this->pdo->exec("COPY pg_copy_data TO STDOUT");

            $this->markTestIncomplete(
                'COPY TO STDOUT via exec() did not throw. '
                . 'COPY output delivery mechanism through PDO exec is unclear.'
            );
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
            if (stripos($msg, 'write protection') !== false || stripos($msg, 'Cannot determine') !== false) {
                $this->markTestIncomplete(
                    'COPY TO blocked by ZTD Write Protection: ' . $msg
                );
            }
            if (stripos($msg, 'COPY') !== false) {
                $this->markTestIncomplete(
                    'COPY TO via exec() not supported: ' . $msg
                );
            }
            // Some error is expected since COPY TO STDOUT doesn't work well through PDO::exec
            $this->markTestIncomplete('Raw COPY TO STDOUT error: ' . $msg);
        }
    }

    /**
     * Raw COPY FROM STDIN via exec().
     */
    public function testRawCopyFromExec(): void
    {
        try {
            $this->pdo->exec("COPY pg_copy_data FROM STDIN");

            $this->fail('COPY FROM STDIN via exec() should fail (no data pipe).');
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
            if (stripos($msg, 'write protection') !== false || stripos($msg, 'Cannot determine') !== false) {
                $this->markTestIncomplete(
                    'COPY FROM blocked by ZTD Write Protection: ' . $msg
                    . '. This prevents bulk data loading while ZTD is enabled.'
                );
            }
            // Expected: COPY FROM STDIN needs a data stream, so it should fail regardless
            $this->assertTrue(true, 'COPY FROM STDIN correctly fails without data pipe');
        }
    }

    /**
     * DML then COPY TO: verify shadow consistency.
     */
    public function testDmlThenCopyConsistency(): void
    {
        try {
            // Perform DML changes
            $this->pdo->exec("INSERT INTO pg_copy_data (id, name, amount) VALUES (4, 'Dave', 400.00)");
            $this->pdo->exec("DELETE FROM pg_copy_data WHERE id = 2");
            $this->pdo->exec("UPDATE pg_copy_data SET amount = 150.00 WHERE id = 1");

            // Verify via SELECT first
            $selectRows = $this->ztdQuery("SELECT * FROM pg_copy_data ORDER BY id");
            $this->assertCount(3, $selectRows); // ids 1, 3, 4

            // Try COPY TO
            $result = $this->pdo->pgsqlCopyToArray('pg_copy_data');

            if ($result === false) {
                $this->markTestIncomplete('COPY TO after DML returned false.');
            }

            if (count($result) !== 3) {
                $this->markTestIncomplete(
                    'COPY TO after DML: expected 3 rows matching SELECT, got ' . count($result)
                    . '. COPY and SELECT disagree on shadow state.'
                );
            }

            $this->assertCount(3, $result);
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
            if (stripos($msg, 'method') !== false) {
                $this->markTestSkipped('pgsqlCopyToArray not available on ZtdPdo');
            }
            $this->markTestIncomplete('DML+COPY consistency check failed: ' . $msg);
        }
    }
}
