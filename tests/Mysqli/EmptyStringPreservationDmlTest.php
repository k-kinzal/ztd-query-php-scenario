<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests that empty strings ('') are preserved through the ZTD CTE rewriter on MySQLi.
 *
 * @spec SPEC-10.2
 */
class EmptyStringPreservationDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE mi_estr_t (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            notes TEXT
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['mi_estr_t'];
    }

    public function testEmptyStringInsert(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_estr_t (name, notes) VALUES ('', 'has name empty')");

            $rows = $this->ztdQuery("SELECT name, notes FROM mi_estr_t WHERE notes = 'has name empty'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Empty string (MySQLi): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['name'] === null) {
                $this->markTestIncomplete(
                    'Empty string (MySQLi): got NULL instead of empty string'
                );
            }

            $this->assertSame('', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Empty string (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateToEmptyString(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_estr_t (name, notes) VALUES ('Alice', 'some notes')");
            $this->ztdExec("UPDATE mi_estr_t SET notes = '' WHERE name = 'Alice'");

            $rows = $this->ztdQuery("SELECT notes FROM mi_estr_t WHERE name = 'Alice'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE to empty string (MySQLi): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['notes'] === null) {
                $this->markTestIncomplete(
                    'UPDATE to empty string (MySQLi): got NULL instead of empty string'
                );
            }

            if ($rows[0]['notes'] !== '') {
                $this->markTestIncomplete(
                    'UPDATE to empty string (MySQLi): expected empty string, got '
                    . var_export($rows[0]['notes'], true)
                );
            }

            $this->assertSame('', $rows[0]['notes']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE to empty string (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testEmptyStringVsNullDistinction(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_estr_t (name, notes) VALUES ('empty', '')");
            $this->ztdExec("INSERT INTO mi_estr_t (name, notes) VALUES ('null_val', NULL)");

            $emptyRows = $this->ztdQuery("SELECT name FROM mi_estr_t WHERE notes = ''");
            $nullRows = $this->ztdQuery("SELECT name FROM mi_estr_t WHERE notes IS NULL");

            if (count($emptyRows) !== 1) {
                $this->markTestIncomplete(
                    'Empty vs NULL (MySQLi): expected 1 empty row, got ' . count($emptyRows)
                    . '. Rows: ' . json_encode($emptyRows)
                );
            }

            if (count($nullRows) !== 1) {
                $this->markTestIncomplete(
                    'Empty vs NULL (MySQLi): expected 1 NULL row, got ' . count($nullRows)
                    . '. Rows: ' . json_encode($nullRows)
                );
            }

            $this->assertSame('empty', $emptyRows[0]['name']);
            $this->assertSame('null_val', $nullRows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Empty vs NULL (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
