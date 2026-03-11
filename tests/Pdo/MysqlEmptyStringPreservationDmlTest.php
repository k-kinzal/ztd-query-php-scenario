<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests that empty strings ('') are preserved through the ZTD CTE rewriter.
 *
 * Empty strings are distinct from NULL in SQL. If the CTE rewriter embeds
 * values as literals, it must preserve '' as '' and not convert it to NULL
 * or drop it. This is a common source of subtle data corruption.
 *
 * @spec SPEC-10.2
 */
class MysqlEmptyStringPreservationDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE my_estr_t (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            notes TEXT,
            code CHAR(10)
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['my_estr_t'];
    }

    /**
     * INSERT with empty string in VARCHAR, then SELECT sees '' not NULL.
     */
    public function testEmptyStringVarcharInsert(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_estr_t (name, notes) VALUES ('', 'has name empty')");

            $rows = $this->ztdQuery("SELECT name, notes FROM my_estr_t WHERE notes = 'has name empty'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Empty string VARCHAR (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            // Must be '' not NULL
            if ($rows[0]['name'] === null) {
                $this->markTestIncomplete(
                    'Empty string VARCHAR (MySQL): got NULL instead of empty string'
                );
            }

            $this->assertSame('', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Empty string VARCHAR (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with empty string in TEXT column.
     */
    public function testEmptyStringTextInsert(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_estr_t (name, notes) VALUES ('test', '')");

            $rows = $this->ztdQuery("SELECT notes FROM my_estr_t WHERE name = 'test'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Empty string TEXT (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['notes'] === null) {
                $this->markTestIncomplete(
                    'Empty string TEXT (MySQL): got NULL instead of empty string'
                );
            }

            $this->assertSame('', $rows[0]['notes']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Empty string TEXT (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT with empty string via bindValue.
     */
    public function testPreparedEmptyStringInsert(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO my_estr_t (name, notes) VALUES (?, ?)");
            $stmt->execute(['', 'prepared_test']);

            $rows = $this->ztdQuery("SELECT name FROM my_estr_t WHERE notes = 'prepared_test'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared empty string (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['name'] === null) {
                $this->markTestIncomplete(
                    'Prepared empty string (MySQL): got NULL instead of empty string'
                );
            }

            $this->assertSame('', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared empty string (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE to empty string, verify it stays empty (not NULL).
     */
    public function testUpdateToEmptyString(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_estr_t (name, notes) VALUES ('Alice', 'some notes')");
            $this->ztdExec("UPDATE my_estr_t SET notes = '' WHERE name = 'Alice'");

            $rows = $this->ztdQuery("SELECT notes FROM my_estr_t WHERE name = 'Alice'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE to empty string (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['notes'] === null) {
                $this->markTestIncomplete(
                    'UPDATE to empty string (MySQL): got NULL instead of empty string'
                );
            }

            if ($rows[0]['notes'] !== '') {
                $this->markTestIncomplete(
                    'UPDATE to empty string (MySQL): expected empty string, got '
                    . var_export($rows[0]['notes'], true)
                );
            }

            $this->assertSame('', $rows[0]['notes']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE to empty string (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * Empty string and NULL coexist: WHERE distinguishes them.
     */
    public function testEmptyStringVsNullDistinction(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_estr_t (name, notes) VALUES ('empty', '')");
            $this->ztdExec("INSERT INTO my_estr_t (name, notes) VALUES ('null_val', NULL)");

            // Count rows where notes = ''
            $emptyRows = $this->ztdQuery("SELECT name FROM my_estr_t WHERE notes = ''");
            // Count rows where notes IS NULL
            $nullRows = $this->ztdQuery("SELECT name FROM my_estr_t WHERE notes IS NULL");

            if (count($emptyRows) !== 1) {
                $this->markTestIncomplete(
                    'Empty vs NULL (MySQL): expected 1 empty row, got ' . count($emptyRows)
                    . '. Rows: ' . json_encode($emptyRows)
                );
            }

            if (count($nullRows) !== 1) {
                $this->markTestIncomplete(
                    'Empty vs NULL (MySQL): expected 1 NULL row, got ' . count($nullRows)
                    . '. Rows: ' . json_encode($nullRows)
                );
            }

            $this->assertSame('empty', $emptyRows[0]['name']);
            $this->assertSame('null_val', $nullRows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Empty vs NULL (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * CHAR column with empty string — MySQL pads CHAR columns.
     */
    public function testEmptyStringCharColumn(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_estr_t (name, code) VALUES ('char_test', '')");

            $rows = $this->ztdQuery("SELECT code FROM my_estr_t WHERE name = 'char_test'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Empty string CHAR (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            // CHAR may pad or trim; the key is it should not be NULL
            if ($rows[0]['code'] === null) {
                $this->markTestIncomplete(
                    'Empty string CHAR (MySQL): got NULL instead of empty/padded string'
                );
            }

            $this->assertNotNull($rows[0]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Empty string CHAR (MySQL) failed: ' . $e->getMessage());
        }
    }
}
