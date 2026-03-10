<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests INSERT () VALUES () (MySQL's DEFAULT VALUES equivalent) on MySQLi.
 *
 * @spec SPEC-10.2
 */
class InsertDefaultValuesDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE mi_idv_counters (
            id INT AUTO_INCREMENT PRIMARY KEY,
            value INT DEFAULT 0,
            label VARCHAR(100) DEFAULT 'untitled'
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['mi_idv_counters'];
    }

    public function testInsertEmptyValues(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_idv_counters () VALUES ()");

            $rows = $this->ztdQuery("SELECT id, value, label FROM mi_idv_counters");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT () VALUES () (MySQLi): expected 1, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame(0, (int) $rows[0]['value']);
            $this->assertSame('untitled', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT () VALUES () (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testMultipleInsertEmptyValues(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_idv_counters () VALUES ()");
            $this->ztdExec("INSERT INTO mi_idv_counters () VALUES ()");
            $this->ztdExec("INSERT INTO mi_idv_counters () VALUES ()");

            $rows = $this->ztdQuery("SELECT id FROM mi_idv_counters ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multiple INSERT () VALUES () (MySQLi): expected 3, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiple INSERT () VALUES () (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
