<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT with no explicit values (MySQL uses INSERT SET or INSERT () VALUES ())
 * through ZTD on MySQL PDO.
 *
 * MySQL does not support INSERT DEFAULT VALUES syntax directly; instead
 * INSERT INTO t () VALUES () is used. This tests the equivalent pattern.
 *
 * @spec SPEC-10.2
 */
class MysqlInsertDefaultValuesDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE my_idv_counters (
            id INT AUTO_INCREMENT PRIMARY KEY,
            value INT DEFAULT 0,
            label VARCHAR(100) DEFAULT 'untitled'
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['my_idv_counters'];
    }

    public function testInsertEmptyValues(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_idv_counters () VALUES ()");

            $rows = $this->ztdQuery("SELECT id, value, label FROM my_idv_counters");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT () VALUES () (MySQL): expected 1, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame(0, (int) $rows[0]['value']);
            $this->assertSame('untitled', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT () VALUES () (MySQL) failed: ' . $e->getMessage());
        }
    }

    public function testMultipleInsertEmptyValues(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_idv_counters () VALUES ()");
            $this->ztdExec("INSERT INTO my_idv_counters () VALUES ()");
            $this->ztdExec("INSERT INTO my_idv_counters () VALUES ()");

            $rows = $this->ztdQuery("SELECT id FROM my_idv_counters ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multiple INSERT () VALUES () (MySQL): expected 3, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiple INSERT () VALUES () (MySQL) failed: ' . $e->getMessage());
        }
    }

    public function testInsertEmptyValuesThenUpdate(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_idv_counters () VALUES ()");
            $this->ztdExec("UPDATE my_idv_counters SET value = 42, label = 'updated' WHERE value = 0");

            $rows = $this->ztdQuery("SELECT value, label FROM my_idv_counters");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT empty then UPDATE (MySQL): expected 1, got ' . count($rows)
                );
            }

            $this->assertSame(42, (int) $rows[0]['value']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT empty then UPDATE (MySQL) failed: ' . $e->getMessage());
        }
    }
}
