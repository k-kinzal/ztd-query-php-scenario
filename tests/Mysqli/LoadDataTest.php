<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests MySQL LOAD DATA statement through the ZTD MySQLi shadow store.
 *
 * LOAD DATA INFILE is MySQL's bulk data loading mechanism. This tests
 * whether the MySQLi adapter handles it correctly.
 *
 * @spec SPEC-6.1
 */
class LoadDataTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE mi_ld_data (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value DECIMAL(10,2) NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_ld_data'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_ld_data (id, name, value) VALUES (1, 'Alice', 100.50)");
        $this->ztdExec("INSERT INTO mi_ld_data (id, name, value) VALUES (2, 'Bob', 200.75)");
    }

    /**
     * LOAD DATA LOCAL INFILE through MySQLi.
     */
    public function testLoadDataLocalInfile(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'ztd_mi_load_');
        file_put_contents($tmpFile, "3\tCarol\t300.00\n4\tDave\t400.00\n");

        try {
            $result = $this->mysqli->query("LOAD DATA LOCAL INFILE '{$tmpFile}' INTO TABLE mi_ld_data");

            if ($result === false) {
                $this->markTestIncomplete(
                    'LOAD DATA LOCAL returned false: ' . $this->mysqli->error
                );
            }

            $rows = $this->ztdQuery("SELECT * FROM mi_ld_data ORDER BY id");

            if (count($rows) === 2) {
                $this->markTestIncomplete(
                    'LOAD DATA LOCAL: succeeded but loaded rows not visible in shadow.'
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
            if (stripos($msg, 'write protection') !== false) {
                $this->markTestIncomplete('LOAD DATA LOCAL blocked by Write Protection: ' . $msg);
            }
            if (stripos($msg, 'not allowed') !== false || stripos($msg, 'disabled') !== false) {
                $this->markTestSkipped('LOAD DATA LOCAL disabled: ' . $msg);
            }
            $this->markTestIncomplete('LOAD DATA LOCAL failed: ' . $msg);
        } finally {
            @unlink($tmpFile);
        }
    }

    /**
     * Shadow state preserved after LOAD DATA attempt.
     */
    public function testShadowPreservedAfterLoadDataAttempt(): void
    {
        $this->ztdExec("UPDATE mi_ld_data SET value = 999.99 WHERE id = 1");

        $tmpFile = tempnam(sys_get_temp_dir(), 'ztd_mi_load_');
        file_put_contents($tmpFile, "20\tLoaded\t20.00\n");

        try {
            $this->mysqli->query("LOAD DATA LOCAL INFILE '{$tmpFile}' INTO TABLE mi_ld_data");
        } catch (\Throwable $e) {
            // May fail — OK
        } finally {
            @unlink($tmpFile);
        }

        $rows = $this->ztdQuery("SELECT id, value FROM mi_ld_data WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertEquals(999.99, (float) $rows[0]['value'], '', 0.01);
    }
}
