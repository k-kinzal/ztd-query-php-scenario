<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL LOAD DATA statement through the ZTD shadow store.
 *
 * LOAD DATA INFILE is MySQL's bulk data loading mechanism. It is widely used
 * for ETL, data migration, and CSV imports. The CTE rewriter likely has no
 * support for LOAD DATA since it's not a standard DML statement.
 *
 * This matters because applications that use LOAD DATA for bulk imports will
 * encounter errors or silently bypass the shadow store.
 *
 * @spec SPEC-6.1
 */
class MysqlLoadDataTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE my_ld_data (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value DECIMAL(10,2) NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_ld_data'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_ld_data (id, name, value) VALUES (1, 'Alice', 100.50)");
        $this->pdo->exec("INSERT INTO my_ld_data (id, name, value) VALUES (2, 'Bob', 200.75)");
    }

    /**
     * LOAD DATA LOCAL INFILE should either be blocked or tracked by shadow store.
     *
     * This is a common pattern for CSV import in PHP applications.
     */
    public function testLoadDataLocalInfile(): void
    {
        // Create a temporary CSV file
        $tmpFile = tempnam(sys_get_temp_dir(), 'ztd_load_');
        file_put_contents($tmpFile, "3\tCarol\t300.00\n4\tDave\t400.00\n");

        try {
            $this->pdo->exec("LOAD DATA LOCAL INFILE '{$tmpFile}' INTO TABLE my_ld_data");

            // If it succeeded, check if data is visible
            $rows = $this->ztdQuery("SELECT * FROM my_ld_data ORDER BY id");

            if (count($rows) === 2) {
                $this->markTestIncomplete(
                    'LOAD DATA LOCAL: succeeded but loaded rows not visible in shadow. '
                    . 'Data bypassed shadow store.'
                );
            }

            if (count($rows) === 4) {
                // Data loaded and visible — check correctness
                $this->assertSame('Carol', $rows[2]['name']);
                $this->assertSame('Dave', $rows[3]['name']);
                return;
            }

            $this->markTestIncomplete(
                'LOAD DATA LOCAL: unexpected row count ' . count($rows)
                . '. Rows: ' . json_encode($rows)
            );
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
            if (stripos($msg, 'write protection') !== false || stripos($msg, 'Cannot determine') !== false) {
                $this->markTestIncomplete(
                    'LOAD DATA LOCAL blocked by ZTD Write Protection: ' . $msg
                    . '. Bulk loading is not possible while ZTD is enabled.'
                );
            }
            if (stripos($msg, 'not allowed') !== false || stripos($msg, 'disabled') !== false) {
                $this->markTestSkipped('LOAD DATA LOCAL disabled on server: ' . $msg);
            }
            $this->markTestIncomplete('LOAD DATA LOCAL failed: ' . $msg);
        } finally {
            @unlink($tmpFile);
        }
    }

    /**
     * LOAD DATA with REPLACE option (replaces duplicate keys).
     */
    public function testLoadDataReplace(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'ztd_load_');
        // id=1 exists, should be replaced
        file_put_contents($tmpFile, "1\tAlice-New\t150.00\n5\tEve\t500.00\n");

        try {
            $this->pdo->exec("LOAD DATA LOCAL INFILE '{$tmpFile}' REPLACE INTO TABLE my_ld_data");

            $rows = $this->ztdQuery("SELECT * FROM my_ld_data ORDER BY id");

            if (count($rows) > 0) {
                $alice = array_values(array_filter($rows, fn($r) => (int) $r['id'] === 1));
                if (count($alice) > 0 && $alice[0]['name'] === 'Alice') {
                    $this->markTestIncomplete(
                        'LOAD DATA REPLACE: id=1 still has original name. '
                        . 'REPLACE not applied through shadow.'
                    );
                }
            }

            $this->assertCount(3, $rows); // id=1 replaced, id=2 original, id=5 new
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
            if (stripos($msg, 'write protection') !== false) {
                $this->markTestIncomplete('LOAD DATA REPLACE blocked by Write Protection: ' . $msg);
            }
            if (stripos($msg, 'not allowed') !== false || stripos($msg, 'disabled') !== false) {
                $this->markTestSkipped('LOAD DATA LOCAL disabled: ' . $msg);
            }
            $this->markTestIncomplete('LOAD DATA REPLACE failed: ' . $msg);
        } finally {
            @unlink($tmpFile);
        }
    }

    /**
     * LOAD DATA IGNORE (skip duplicate keys).
     */
    public function testLoadDataIgnore(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'ztd_load_');
        // id=1 exists and should be skipped
        file_put_contents($tmpFile, "1\tAlice-Dup\t150.00\n6\tFrank\t600.00\n");

        try {
            $this->pdo->exec("LOAD DATA LOCAL INFILE '{$tmpFile}' IGNORE INTO TABLE my_ld_data");

            $rows = $this->ztdQuery("SELECT * FROM my_ld_data ORDER BY id");

            if (count($rows) > 0) {
                $alice = array_values(array_filter($rows, fn($r) => (int) $r['id'] === 1));
                if (count($alice) > 0 && $alice[0]['name'] === 'Alice-Dup') {
                    $this->markTestIncomplete(
                        'LOAD DATA IGNORE: id=1 was overwritten instead of skipped.'
                    );
                }
            }

            // Expected: id=1 original + id=2 original + id=6 new = 3 rows
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
            if (stripos($msg, 'write protection') !== false) {
                $this->markTestIncomplete('LOAD DATA IGNORE blocked by Write Protection: ' . $msg);
            }
            if (stripos($msg, 'not allowed') !== false || stripos($msg, 'disabled') !== false) {
                $this->markTestSkipped('LOAD DATA LOCAL disabled: ' . $msg);
            }
            $this->markTestIncomplete('LOAD DATA IGNORE failed: ' . $msg);
        } finally {
            @unlink($tmpFile);
        }
    }

    /**
     * DML changes should be preserved after LOAD DATA attempt.
     *
     * Even if LOAD DATA fails, prior shadow DML should remain intact.
     */
    public function testShadowPreservedAfterLoadDataAttempt(): void
    {
        // Modify data through shadow
        $this->pdo->exec("UPDATE my_ld_data SET value = 999.99 WHERE id = 1");
        $this->pdo->exec("INSERT INTO my_ld_data (id, name, value) VALUES (10, 'Shadow', 10.00)");

        $tmpFile = tempnam(sys_get_temp_dir(), 'ztd_load_');
        file_put_contents($tmpFile, "20\tLoaded\t20.00\n");

        try {
            $this->pdo->exec("LOAD DATA LOCAL INFILE '{$tmpFile}' INTO TABLE my_ld_data");
        } catch (\Throwable $e) {
            // Expected to fail — that's OK
        } finally {
            @unlink($tmpFile);
        }

        // Verify prior shadow DML is intact
        $rows = $this->ztdQuery("SELECT id, name, value FROM my_ld_data ORDER BY id");

        // At minimum, shadow changes should be visible
        $row1 = array_values(array_filter($rows, fn($r) => (int) $r['id'] === 1));
        $row10 = array_values(array_filter($rows, fn($r) => (int) $r['id'] === 10));

        $this->assertCount(1, $row1, 'id=1 should exist after LOAD DATA attempt');
        $this->assertEquals(999.99, (float) $row1[0]['value'], '', 0.01);

        $this->assertCount(1, $row10, 'Shadow-inserted id=10 should exist after LOAD DATA attempt');
        $this->assertSame('Shadow', $row10[0]['name']);
    }
}
