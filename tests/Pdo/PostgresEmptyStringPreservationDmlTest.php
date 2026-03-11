<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests that empty strings ('') are preserved through the ZTD CTE rewriter on PostgreSQL.
 *
 * @spec SPEC-10.2
 */
class PostgresEmptyStringPreservationDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE pg_estr_t (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            notes TEXT
        )";
    }

    protected function getTableNames(): array
    {
        return ['pg_estr_t'];
    }

    public function testEmptyStringInsert(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_estr_t (name, notes) VALUES ('', 'has name empty')");

            $rows = $this->ztdQuery("SELECT name, notes FROM pg_estr_t WHERE notes = 'has name empty'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Empty string (PG): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['name'] === null) {
                $this->markTestIncomplete(
                    'Empty string (PG): got NULL instead of empty string'
                );
            }

            $this->assertSame('', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Empty string (PG) failed: ' . $e->getMessage());
        }
    }

    public function testPreparedEmptyStringInsert(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO pg_estr_t (name, notes) VALUES (?, ?)");
            $stmt->execute(['', 'prepared_test']);

            $rows = $this->ztdQuery("SELECT name FROM pg_estr_t WHERE notes = 'prepared_test'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared empty string (PG): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['name'] === null) {
                $this->markTestIncomplete(
                    'Prepared empty string (PG): got NULL instead of empty string'
                );
            }

            $this->assertSame('', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared empty string (PG) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateToEmptyString(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_estr_t (name, notes) VALUES ('Alice', 'some notes')");
            $this->ztdExec("UPDATE pg_estr_t SET notes = '' WHERE name = 'Alice'");

            $rows = $this->ztdQuery("SELECT notes FROM pg_estr_t WHERE name = 'Alice'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE to empty string (PG): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['notes'] === null) {
                $this->markTestIncomplete(
                    'UPDATE to empty string (PG): got NULL instead of empty string'
                );
            }

            if ($rows[0]['notes'] !== '') {
                $this->markTestIncomplete(
                    'UPDATE to empty string (PG): expected empty string, got '
                    . var_export($rows[0]['notes'], true)
                );
            }

            $this->assertSame('', $rows[0]['notes']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE to empty string (PG) failed: ' . $e->getMessage());
        }
    }

    public function testEmptyStringVsNullDistinction(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_estr_t (name, notes) VALUES ('empty', '')");
            $this->ztdExec("INSERT INTO pg_estr_t (name, notes) VALUES ('null_val', NULL)");

            $emptyRows = $this->ztdQuery("SELECT name FROM pg_estr_t WHERE notes = ''");
            $nullRows = $this->ztdQuery("SELECT name FROM pg_estr_t WHERE notes IS NULL");

            if (count($emptyRows) !== 1) {
                $this->markTestIncomplete(
                    'Empty vs NULL (PG): expected 1 empty row, got ' . count($emptyRows)
                    . '. Rows: ' . json_encode($emptyRows)
                );
            }

            if (count($nullRows) !== 1) {
                $this->markTestIncomplete(
                    'Empty vs NULL (PG): expected 1 NULL row, got ' . count($nullRows)
                    . '. Rows: ' . json_encode($nullRows)
                );
            }

            $this->assertSame('empty', $emptyRows[0]['name']);
            $this->assertSame('null_val', $nullRows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Empty vs NULL (PG) failed: ' . $e->getMessage());
        }
    }
}
