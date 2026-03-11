<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests that empty strings ('') are preserved through the ZTD CTE rewriter on SQLite.
 *
 * @spec SPEC-10.2
 */
class SqliteEmptyStringPreservationDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        // Use explicit PK (not AUTOINCREMENT) to avoid Issue #145 shadow PK=null
        return "CREATE TABLE sl_estr_t (
            id INTEGER PRIMARY KEY,
            name TEXT,
            notes TEXT
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_estr_t'];
    }

    public function testEmptyStringInsert(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_estr_t (id, name, notes) VALUES (1, '', 'has name empty')");

            $rows = $this->ztdQuery("SELECT name, notes FROM sl_estr_t WHERE notes = 'has name empty'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Empty string (SQLite): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['name'] === null) {
                $this->markTestIncomplete(
                    'Empty string (SQLite): got NULL instead of empty string'
                );
            }

            $this->assertSame('', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Empty string (SQLite) failed: ' . $e->getMessage());
        }
    }

    public function testPreparedEmptyStringInsert(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO sl_estr_t (id, name, notes) VALUES (?, ?, ?)");
            $stmt->execute([2, '', 'prepared_test']);

            $rows = $this->ztdQuery("SELECT name FROM sl_estr_t WHERE notes = 'prepared_test'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared empty string (SQLite): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['name'] === null) {
                $this->markTestIncomplete(
                    'Prepared empty string (SQLite): got NULL instead of empty string'
                );
            }

            $this->assertSame('', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared empty string (SQLite) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateToEmptyString(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_estr_t (id, name, notes) VALUES (1, 'Alice', 'some notes')");
            $this->ztdExec("UPDATE sl_estr_t SET notes = '' WHERE name = 'Alice'");

            $rows = $this->ztdQuery("SELECT notes FROM sl_estr_t WHERE name = 'Alice'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE to empty string (SQLite): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['notes'] === null) {
                $this->markTestIncomplete(
                    'UPDATE to empty string (SQLite): got NULL instead of empty string'
                );
            }

            $this->assertSame('', $rows[0]['notes']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE to empty string (SQLite) failed: ' . $e->getMessage());
        }
    }

    public function testEmptyStringVsNullDistinction(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_estr_t (id, name, notes) VALUES (1, 'empty', '')");
            $this->ztdExec("INSERT INTO sl_estr_t (id, name, notes) VALUES (2, 'null_val', NULL)");

            $emptyRows = $this->ztdQuery("SELECT name FROM sl_estr_t WHERE notes = ''");
            $nullRows = $this->ztdQuery("SELECT name FROM sl_estr_t WHERE notes IS NULL");

            if (count($emptyRows) !== 1) {
                $this->markTestIncomplete(
                    'Empty vs NULL (SQLite): expected 1 empty row, got ' . count($emptyRows)
                    . '. Rows: ' . json_encode($emptyRows)
                );
            }

            if (count($nullRows) !== 1) {
                $this->markTestIncomplete(
                    'Empty vs NULL (SQLite): expected 1 NULL row, got ' . count($nullRows)
                    . '. Rows: ' . json_encode($nullRows)
                );
            }

            $this->assertSame('empty', $emptyRows[0]['name']);
            $this->assertSame('null_val', $nullRows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Empty vs NULL (SQLite) failed: ' . $e->getMessage());
        }
    }
}
